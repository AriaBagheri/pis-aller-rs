use serde::Serialize;

/// Pis Aller. If all else fails, we are at the mercy of our logs.
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", content = "value")]
pub enum PisAller {
    Bin(Box<Vec<u8>>),
    Json(String),
    String(String),
    RustString(String),
}

impl PisAller {
    pub fn format(&self) -> &'static str {
        match self {
            PisAller::Bin(_) => "bin",
            PisAller::Json(_) => "json",
            PisAller::String(_) => "txt",
            PisAller::RustString(_) => "rson",
        }
    }
}

impl From<&[u8]> for PisAller {
    fn from(value: &[u8]) -> Self {
        Self::Bin(Box::new(value.to_vec()))
    }
}

impl From<String> for PisAller {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for PisAller {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}
