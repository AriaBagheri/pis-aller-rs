use serde::Serialize;
use crate::item::PisAller;
use standard_error::traits::*;

trait PisAllerTrait: Serialize + StandardErrorDescriptionTrait + StandardErrorCausesTrait + StandardErrorSolutionsTrait {}

#[derive(Debug, Serialize, Clone)]
pub struct PisAllerRecord {
    pub tag: &'static str,
    pub error: dyn PisAllerTrait,
    #[serde(flatten)]
    pub data: PisAller,
    pub retries: usize,
}