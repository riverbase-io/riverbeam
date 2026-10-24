//! Beam content parts as rig user content (Python `content_to_lc_content`).
//!
//! Runtimes send every image; the gateway drops or refuses them for a model
//! whose catalog entry does not list the image modality.

use beam_types::{ContentPart, ContentPartType};
use rig_core::completion::Message;
use rig_core::message::{ImageMediaType, MimeType, UserContent};

/// The parts' text joined by `sep`, then their images (`image_base64`,
/// `image_url`). Audio, video and file parts are not sent.
pub(crate) fn user_content(parts: &[ContentPart], sep: &str) -> Vec<UserContent> {
    let text = parts
        .iter()
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join(sep);
    let mut content = Vec::new();
    if !text.is_empty() {
        content.push(UserContent::text(text));
    }
    content.extend(parts.iter().filter_map(image));
    content
}

/// The current turn's user message; empty text when no part is sent.
pub(crate) fn user_message(parts: &[ContentPart], sep: &str) -> Message {
    let content = user_content(parts, sep);
    if content.is_empty() {
        return Message::user(String::new());
    }
    Message::User { content }
}

fn image(part: &ContentPart) -> Option<UserContent> {
    let media = part.mime_type.as_deref().and_then(media_type);
    match part.part_type {
        ContentPartType::ImageBase64 => {
            let data = part.data.as_deref().filter(|d| !d.is_empty())?;
            // Python sends `image/png` when the part names no type. A data
            // URL needs a type rig knows, so an unknown one is sent as PNG.
            Some(UserContent::image_base64(
                data,
                Some(media.unwrap_or(ImageMediaType::PNG)),
                None,
            ))
        }
        ContentPartType::ImageUrl => {
            let url = part.url.as_deref().filter(|u| !u.is_empty())?;
            Some(UserContent::image_url(url, media, None))
        }
        _ => None,
    }
}

fn media_type(mime: &str) -> Option<ImageMediaType> {
    let mime = mime.trim().to_ascii_lowercase();
    let mime = if mime == "image/jpg" {
        "image/jpeg"
    } else {
        &mime
    };
    ImageMediaType::from_mime_type(mime)
}
