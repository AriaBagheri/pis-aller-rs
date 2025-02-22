use serde::Serialize;
use crate::item::PisAller;


#[derive(Debug, Serialize, Clone)]
pub struct PisAllerRecord {
    pub tag: &'static str,
    pub error: String,
    #[serde(flatten)]
    pub data: PisAller,
    pub retries: usize,
}