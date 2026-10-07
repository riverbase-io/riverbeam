use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Modality {
    Text,
    Image,
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentPartType {
    Text,
    ImageUrl,
    ImageBase64,
    Audio,
    File,
    Video,
    MediaRef,
    Reasoning,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ContentPart {
    #[serde(rename = "type")]
    pub part_type: ContentPartType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default = "empty_object", skip_serializing_if = "is_default_metadata")]
    pub metadata: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_id: Option<Uuid>,
}

pub(crate) fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn is_default_metadata(v: &Value) -> bool {
    v.is_null() || (v.is_object() && v.as_object().is_some_and(|m| m.is_empty()))
}

impl ContentPart {
    pub fn text(content: impl Into<String>) -> Self {
        Self {
            part_type: ContentPartType::Text,
            text: Some(content.into()),
            url: None,
            data: None,
            mime_type: None,
            metadata: Value::Object(Default::default()),
            media_id: None,
        }
    }

    pub fn detected_modality(&self) -> Modality {
        match self.part_type {
            ContentPartType::Text | ContentPartType::Reasoning => Modality::Text,
            ContentPartType::ImageUrl | ContentPartType::ImageBase64 => Modality::Image,
            ContentPartType::Audio => Modality::Audio,
            ContentPartType::Video => Modality::Video,
            ContentPartType::File | ContentPartType::MediaRef => {
                if let Some(mime) = &self.mime_type {
                    if mime.starts_with("video/") {
                        return Modality::Video;
                    }
                    if mime.starts_with("audio/") {
                        return Modality::Audio;
                    }
                    if mime.starts_with("image/") {
                        return Modality::Image;
                    }
                }
                Modality::Text
            }
        }
    }
}
