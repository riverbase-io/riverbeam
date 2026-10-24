//! Document parsers (Python `knowledge/parsers.py`): text, Markdown with
//! heading breadcrumbs, PDF, and [`AutoParser`], which dispatches on the MIME
//! type or file name.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use riverbase_core::RiverbaseResult;
use serde_json::{Map, Value};

use crate::types::ParsedBlock;

/// What an ingest reads (Python's accepted source shapes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Text(String),
    Bytes(Vec<u8>),
    Path(PathBuf),
    /// A file-service media entry (read through a [`MediaReader`]).
    Media {
        media_id: String,
        filename: Option<String>,
    },
}

/// Options for parsing a document.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParseOptions {
    pub mime_type: Option<String>,
    pub metadata: Map<String, Value>,
}

/// Reads a `media_id` document's bytes from the host's file service.
/// riverbeam ships no implementation: a host that stores media provides one.
#[async_trait]
pub trait MediaReader: Send + Sync {
    /// The entry's bytes, or `None` when there is no such entry.
    ///
    /// # Errors
    /// Read errors.
    async fn read(&self, media_id: &str) -> RiverbaseResult<Option<Vec<u8>>>;
}

/// Turns a source into text blocks.
#[async_trait]
pub trait DocumentParser: Send + Sync {
    /// # Errors
    /// Unreadable sources.
    async fn parse(
        &self,
        source: &Source,
        opts: &ParseOptions,
    ) -> RiverbaseResult<Vec<ParsedBlock>>;
}

async fn media_bytes(
    media: Option<&Arc<dyn MediaReader>>,
    media_id: &str,
) -> RiverbaseResult<Vec<u8>> {
    let Some(reader) = media else {
        tracing::warn!("media_id provided but no media reader is configured");
        return Ok(Vec::new());
    };
    if let Some(bytes) = reader.read(media_id).await? {
        Ok(bytes)
    } else {
        tracing::warn!(media_id, "media reader returned no entry");
        Ok(Vec::new())
    }
}

/// A source's text, read from media when needed.
async fn coerce_text(
    source: &Source,
    media: Option<&Arc<dyn MediaReader>>,
) -> RiverbaseResult<String> {
    Ok(match source {
        Source::Text(text) => text.clone(),
        Source::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Source::Path(path) => {
            std::fs::read_to_string(path).map_err(|e| crate::BEM_236.with_data(e.to_string()))?
        }
        Source::Media { media_id, .. } => {
            String::from_utf8_lossy(&media_bytes(media, media_id).await?).into_owned()
        }
    })
}

/// A source's bytes, read from media when needed.
async fn coerce_bytes(
    source: &Source,
    media: Option<&Arc<dyn MediaReader>>,
) -> RiverbaseResult<Vec<u8>> {
    Ok(match source {
        Source::Text(text) => text.as_bytes().to_vec(),
        Source::Bytes(bytes) => bytes.clone(),
        Source::Path(path) => {
            std::fs::read(path).map_err(|e| crate::BEM_236.with_data(e.to_string()))?
        }
        Source::Media { media_id, .. } => media_bytes(media, media_id).await?,
    })
}

/// Python `str.isspace`.
fn py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `str.splitlines()` (without line ends).
fn py_splitlines(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        let breaks = matches!(
            c,
            '\n' | '\r'
                | '\u{0b}'
                | '\u{0c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if !breaks {
            continue;
        }
        out.push(&text[start..i]);
        let mut next = i + c.len_utf8();
        if c == '\r' {
            if let Some((j, '\n')) = chars.peek().copied() {
                chars.next();
                next = j + 1;
            }
        }
        start = next;
    }
    if start < text.len() {
        out.push(&text[start..]);
    }
    out
}

/// The whole source as one block.
#[derive(Default, Clone)]
pub struct TextParser {
    pub media: Option<Arc<dyn MediaReader>>,
}

#[async_trait]
impl DocumentParser for TextParser {
    async fn parse(
        &self,
        source: &Source,
        opts: &ParseOptions,
    ) -> RiverbaseResult<Vec<ParsedBlock>> {
        let text = coerce_text(source, self.media.as_ref()).await?;
        if text.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![ParsedBlock {
            text,
            metadata: opts.metadata.clone(),
        }])
    }
}

/// One block per section, with the heading path as
/// `metadata.headings`.
#[derive(Default, Clone)]
pub struct MarkdownParser {
    pub media: Option<Arc<dyn MediaReader>>,
}

/// Python's Markdown sectioning over already-loaded text.
#[must_use]
pub fn markdown_blocks(text: &str, base: &Map<String, Value>) -> Vec<ParsedBlock> {
    let mut blocks = Vec::new();
    let mut headings: Vec<String> = Vec::new();
    let mut buf: Vec<&str> = Vec::new();
    let flush = |buf: &mut Vec<&str>, headings: &[String], blocks: &mut Vec<ParsedBlock>| {
        if buf.is_empty() {
            return;
        }
        let joined = buf.join("\n");
        let joined = joined.trim_matches(py_space);
        if !joined.is_empty() {
            let mut metadata = base.clone();
            if !headings.is_empty() {
                metadata.insert("headings".into(), Value::from(headings.to_vec()));
            }
            blocks.push(ParsedBlock {
                text: joined.to_owned(),
                metadata,
            });
        }
        buf.clear();
    };
    for line in py_splitlines(text) {
        let stripped = line.trim_start_matches(py_space);
        if stripped.starts_with('#') {
            flush(&mut buf, &headings, &mut blocks);
            let level = stripped.chars().take_while(|c| *c == '#').count();
            let title = stripped[level..].trim_matches(py_space).to_owned();
            // (current[: level - 1] + [title])[: max(level, 1)]
            headings.truncate(level.saturating_sub(1));
            headings.push(title);
            headings.truncate(level.max(1));
            continue;
        }
        buf.push(line);
    }
    flush(&mut buf, &headings, &mut blocks);
    blocks
}

#[async_trait]
impl DocumentParser for MarkdownParser {
    async fn parse(
        &self,
        source: &Source,
        opts: &ParseOptions,
    ) -> RiverbaseResult<Vec<ParsedBlock>> {
        let text = coerce_text(source, self.media.as_ref()).await?;
        if text.is_empty() {
            return Ok(Vec::new());
        }
        Ok(markdown_blocks(&text, &opts.metadata))
    }
}

/// Python `PdfParser` (`pypdf`): one block per page with text, `metadata.page`
/// 1-based. Text comes from `pdf-extract`, so spacing can differ from
/// `pypdf`; compare after whitespace normalization.
#[derive(Default, Clone)]
pub struct PdfParser {
    pub media: Option<Arc<dyn MediaReader>>,
}

/// The largest PDF the parser reads (64 MiB); bigger files are refused
/// before parsing.
pub const MAX_PDF_BYTES: usize = 64 * 1024 * 1024;

/// Page texts of a PDF; a malformed file is an error, never a panic.
///
/// # Errors
/// `BEM-237` when the PDF cannot be read.
pub fn pdf_pages(bytes: &[u8]) -> RiverbaseResult<Vec<String>> {
    if bytes.len() > MAX_PDF_BYTES {
        return Err(
            crate::BEM_237.with_data(format!("the PDF is larger than {MAX_PDF_BYTES} bytes"))
        );
    }
    let pages = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pdf_extract::extract_text_from_mem_by_pages(bytes)
    }))
    .map_err(|_| crate::BEM_237.with_data("the PDF parser panicked"))?;
    pages.map_err(|e| crate::BEM_237.with_data(e.to_string()))
}

#[async_trait]
impl DocumentParser for PdfParser {
    async fn parse(
        &self,
        source: &Source,
        opts: &ParseOptions,
    ) -> RiverbaseResult<Vec<ParsedBlock>> {
        let bytes = coerce_bytes(source, self.media.as_ref()).await?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let pages = tokio::task::spawn_blocking(move || pdf_pages(&bytes))
            .await
            .map_err(|e| crate::BEM_237.with_data(e.to_string()))??;
        Ok(pages
            .iter()
            .enumerate()
            .filter_map(|(index, page)| {
                let text = page.trim_matches(py_space);
                if text.is_empty() {
                    return None;
                }
                let mut metadata = opts.metadata.clone();
                metadata.insert("page".into(), Value::from(index + 1));
                Some(ParsedBlock {
                    text: text.to_owned(),
                    metadata,
                })
            })
            .collect())
    }
}

/// MIME type first, then the file extension.
#[must_use]
pub fn detect_kind(source: &Source, opts: &ParseOptions) -> &'static str {
    let mime = opts.mime_type.as_deref().unwrap_or_default().to_lowercase();
    if !mime.is_empty() {
        if mime.contains("pdf") {
            return "pdf";
        }
        if mime.contains("markdown") {
            return "markdown";
        }
        if mime.starts_with("text/") {
            return "text";
        }
    }
    let name = match source {
        Source::Path(path) => path
            .extension()
            .map(|e| format!(".{}", e.to_string_lossy()))
            .unwrap_or_default(),
        Source::Media { filename, .. } => filename.clone().unwrap_or_default(),
        Source::Text(_) | Source::Bytes(_) => String::new(),
    }
    .to_lowercase();
    // Already lowercased; Python compares `filename.lower()`.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    if name.ends_with(".pdf") {
        "pdf"
    } else if name.ends_with(".md") || name.ends_with(".markdown") {
        "markdown"
    } else {
        "text"
    }
}

/// Picks a parser by MIME type, then file extension.
#[derive(Default, Clone)]
pub struct AutoParser {
    pub media: Option<Arc<dyn MediaReader>>,
}

impl AutoParser {
    #[must_use]
    pub fn with_media(media: Arc<dyn MediaReader>) -> Self {
        Self { media: Some(media) }
    }
}

#[async_trait]
impl DocumentParser for AutoParser {
    async fn parse(
        &self,
        source: &Source,
        opts: &ParseOptions,
    ) -> RiverbaseResult<Vec<ParsedBlock>> {
        let media = self.media.clone();
        match detect_kind(source, opts) {
            "pdf" => PdfParser { media }.parse(source, opts).await,
            "markdown" => MarkdownParser { media }.parse(source, opts).await,
            _ => TextParser { media }.parse(source, opts).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_sections_carry_heading_paths() {
        let text = "intro\n# A\none\n## B\ntwo\n### C\nthree\n## D\nfour\n#E\n\n";
        let blocks = markdown_blocks(text, &Map::new());
        let rows: Vec<(String, Value)> = blocks
            .iter()
            .map(|b| {
                (
                    b.text.clone(),
                    b.metadata.get("headings").cloned().unwrap_or(Value::Null),
                )
            })
            .collect();
        assert_eq!(
            rows,
            [
                ("intro".to_owned(), Value::Null),
                ("one".to_owned(), serde_json::json!(["A"])),
                ("two".to_owned(), serde_json::json!(["A", "B"])),
                ("three".to_owned(), serde_json::json!(["A", "B", "C"])),
                ("four".to_owned(), serde_json::json!(["A", "D"])),
            ]
        );
    }

    #[test]
    fn splitlines_matches_python() {
        assert_eq!(
            py_splitlines("a\r\nb\rc\n\nd\u{2028}e\n"),
            ["a", "b", "c", "", "d", "e"]
        );
    }

    #[test]
    fn kinds_follow_mime_then_name() {
        let opts = |m: &str| ParseOptions {
            mime_type: Some(m.into()),
            ..ParseOptions::default()
        };
        let text = Source::Text(String::new());
        assert_eq!(detect_kind(&text, &opts("application/pdf")), "pdf");
        assert_eq!(detect_kind(&text, &opts("text/markdown")), "markdown");
        let media = Source::Media {
            media_id: "m".into(),
            filename: Some("Guide.MD".into()),
        };
        assert_eq!(detect_kind(&media, &ParseOptions::default()), "markdown");
        assert_eq!(detect_kind(&text, &ParseOptions::default()), "text");
    }

    /// A one-page PDF whose content stream shows "Hello PDF world".
    fn tiny_pdf() -> Vec<u8> {
        let stream = "BT /F1 12 Tf 72 712 Td (Hello PDF world) Tj ET";
        let objects = [
            "<< /Type /Catalog /Pages 2 0 R >>".to_owned(),
            "<< /Type /Pages /Kids [3 0 R] /Count 1 >>".to_owned(),
            "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>".to_owned(),
            format!("<< /Length {} >>\nstream\n{stream}\nendstream", stream.len()),
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".to_owned(),
        ];
        let mut pdf = b"%PDF-1.4\n".to_vec();
        let mut offsets = Vec::new();
        for (i, body) in objects.iter().enumerate() {
            offsets.push(pdf.len());
            pdf.extend_from_slice(format!("{} 0 obj\n{body}\nendobj\n", i + 1).as_bytes());
        }
        let xref = pdf.len();
        pdf.extend_from_slice(
            format!("xref\n0 {}\n0000000000 65535 f \n", objects.len() + 1).as_bytes(),
        );
        for offset in offsets {
            pdf.extend_from_slice(format!("{offset:010} 00000 n \n").as_bytes());
        }
        pdf.extend_from_slice(
            format!(
                "trailer\n<< /Size {} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n",
                objects.len() + 1
            )
            .as_bytes(),
        );
        pdf
    }

    #[tokio::test]
    async fn pdf_pages_become_blocks() {
        let blocks = AutoParser::default()
            .parse(
                &Source::Bytes(tiny_pdf()),
                &ParseOptions {
                    mime_type: Some("application/pdf".into()),
                    ..ParseOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(blocks.len(), 1);
        let normalized: Vec<&str> = blocks[0].text.split_whitespace().collect();
        assert_eq!(normalized, ["Hello", "PDF", "world"]);
        assert_eq!(blocks[0].metadata["page"], 1);
    }

    #[test]
    fn a_broken_pdf_is_an_error() {
        assert!(pdf_pages(b"%PDF-1.4 not really").is_err());
    }
}
