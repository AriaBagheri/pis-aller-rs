use std::sync::LazyLock;
use std::time::Duration;
use colored::Colorize;
use tokio::task::JoinHandle;
use crossbeam_queue::SegQueue;
use persistent_mongo::PersistentMongo;
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use uuid::Uuid;
use crate::item::PisAller;
use crate::record::PisAllerRecord;

pub struct PersistentPisAller {
    application: &'static str,
    mongo: &'static PersistentMongo,

    log_queue: SegQueue<PisAllerRecord>,
    log_thread: Mutex<Option<JoinHandle<()>>>,

    file_queue: SegQueue<PisAllerRecord>,
    file_thread: Mutex<Option<JoinHandle<()>>>,

    mongo_queue: SegQueue<PisAllerRecord>,
    mongo_thread: Mutex<Option<JoinHandle<()>>>,

    shutdown_signal: LazyLock<Sender<()>>,
}

/// Takes care of the different pipelines.
/// MongoDB ?-> File ?-> Logs
impl PersistentPisAller {
    pub const fn const_new(application: &'static str, mongo: &'static PersistentMongo) -> Self {
        Self {
            application,
            mongo,

            log_queue: SegQueue::new(),
            log_thread: Mutex::const_new(None),

            file_queue: SegQueue::new(),
            file_thread: Mutex::const_new(None),

            mongo_queue: SegQueue::new(),
            mongo_thread: Mutex::const_new(None),

            shutdown_signal: LazyLock::new(|| Sender::new(1)),
        }
    }
    pub async fn initiate(&'static self) -> Result<(), &'static str> {
        println!(
            "{}",
            "PIS-ALLER - INITIATE - Initializing pis aller!".blue()
        );

        if let Err(e) = tokio::fs::create_dir_all(format!("pis-aller/{}", self.application)).await {
            println!(
                "{}",
                format!(
                    "PIS-ALLER - INITIATE - Failed to initiate pis aller directories | \
                    error = {e:?}"
                ).red()
            );
            return Err("Failed to create pis aller directories");
        }

        *self.mongo_thread.lock().await = Some(self.mongo_thread());
        *self.file_thread.lock().await = Some(self.file_thread());
        *self.log_thread.lock().await = Some(self.log_thread());
        Ok(())
    }
    pub fn process(&self, tag: &'static str, value: impl Into<PisAller>, error: impl ToString) {
        self.mongo_queue.push(PisAllerRecord {
            tag,
            error: error.to_string(),
            data: value.into(),
            retries: 0,
        });
    }

    pub fn mongo_thread(&'static self) -> JoinHandle<()> {
        let mut shutdown = self.shutdown_signal.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        break;
                    },
                    client = self.mongo.client() => {
                        let mut record = if let Some(item) = self.mongo_queue.pop() {
                            item
                        } else {
                            interval.tick().await;
                            continue;
                        };

                        let db = client.database(&self.application);
                        let collection = db.collection::<PisAllerRecord>(&format!("pis-aller-{}", record.tag));
                        match collection.insert_one(&record).await {
                            Ok(_) => {},
                            Err(e) => {
                                record.retries += 1;
                                record.error = format!("{}\n{:?}", record.error, e);
                                if record.retries == 5 {
                                    record.retries = 0;
                                    self.file_queue.push(record);
                                } else {
                                    self.mongo_queue.push(record);
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn file_thread(&'static self) -> JoinHandle<()> {
        let mut shutdown = self.shutdown_signal.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        break;
                    },
                    _ = interval.tick() => {
                        let mut record = if let Some(item) = self.file_queue.pop() {
                            item
                        } else {
                            interval.tick().await;
                            continue;
                        };
                        let folder = format!("pis-aller/{}/{}", self.application, record.tag);
                        let uuid = Uuid::new_v4().to_string();
                        let file_path = format!("{}/{}.{}", folder, uuid, record.data.format());
                        if let Err(e) = tokio::fs::create_dir_all(folder).await {
                            record.retries += 1;
                            record.error = format!("{}\n{:?}", record.error, e);
                            if record.retries == 5 {
                                record.retries = 0;
                                self.log_queue.push(record);
                            } else {
                                self.file_queue.push(record);
                            }
                        } else {
                            if let Err(e) = tokio::fs::write(
                                file_path,
                                match &record.data {
                                    PisAller::Bin(b) => {b.as_slice()}
                                    PisAller::Json(s) => {s.as_bytes()}
                                    PisAller::String(s) => {s.as_bytes()}
                                    PisAller::RustString(s) => {s.as_bytes()}
                                }
                            ).await {
                                record.retries += 1;
                                record.error = format!("{}\n{:?}", record.error, e);
                                if record.retries == 5 {
                                    record.retries = 0;
                                    self.log_queue.push(record);
                                } else {
                                    self.file_queue.push(record);
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn log_thread(&'static self) -> JoinHandle<()> {
        let mut shutdown = self.shutdown_signal.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        break;
                    },
                    _ = interval.tick() => {
                        let record = if let Some(item) = self.file_queue.pop() {
                            item
                        } else {
                            interval.tick().await;
                            continue;
                        };
                        println!(
                            "{}",
                            format!(
                                "PIS-ALLER - LOG_THREAD - Failed to store pis aller record | \
                                record = {:?}", record
                            ).red().bold()
                        );
                    }
                }
            }
        })
    }

    pub async fn shutdown(&self) {
        print!("\n");

        let _ = self.shutdown_signal.send(());

        if let Some(mongo_thread) = self.mongo_thread.lock().await.as_mut() {
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut *mongo_thread)
                .await
                .map(|_| {
                    println!(
                        "{}",
                        "PIS-ALLER - SHUTDOWN - Mongo thread terminated gracefully!".cyan()
                    );
                })
                .map_err(|_| {
                    mongo_thread.abort();
                    println!(
                        "{} {}",
                        "PIS-ALLER - SHUTDOWN - Mongo thread terminated".cyan(),
                        "forcefully".bold().red()
                    );
                });
        }
        if let Some(file_thread) = self.file_thread.lock().await.as_mut() {
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut *file_thread)
                .await
                .map(|_| {
                    println!(
                        "{}",
                        "PIS-ALLER - SHUTDOWN - File thread terminated gracefully!".cyan()
                    );
                })
                .map_err(|_| {
                    file_thread.abort();
                    println!(
                        "{} {}",
                        "PIS-ALLER - SHUTDOWN - File thread terminated".cyan(),
                        "forcefully".bold().red()
                    );
                });
        }
        if let Some(log_thread) = self.log_thread.lock().await.as_mut() {
            let _ = tokio::time::timeout(Duration::from_secs(5), &mut *log_thread)
                .await
                .map(|_| {
                    println!(
                        "{}",
                        "PIS-ALLER - SHUTDOWN - Log thread terminated gracefully!".cyan()
                    );
                })
                .map_err(|_| {
                    log_thread.abort();
                    println!(
                        "{} {}",
                        "PIS-ALLER - SHUTDOWN - Log thread terminated".cyan(),
                        "forcefully".bold().red()
                    );
                });
        }
    }
}