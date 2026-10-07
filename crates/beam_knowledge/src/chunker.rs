use serde_json::json;

use crate::types::Chunk;

/// Splits text into chunks for embedding.
pub trait Chunker: Send + Sync {
    fn chunk(&self, text: &str) -> Vec<Chunk>;
}

/// Paragraph-aware chunker with a soft character budget (ports the default
/// recursive chunker behavior: split on blank lines, pack up to `max_chars`).
pub struct SimpleChunker {
    max_chars: usize,
}

impl SimpleChunker {
    #[must_use]
    pub fn new(max_chars: usize) -> Self {
        Self {
            max_chars: max_chars.max(1),
        }
    }
}

impl Default for SimpleChunker {
    fn default() -> Self {
        Self::new(800)
    }
}

impl Chunker for SimpleChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        let mut chunks = Vec::new();
        let mut current = String::new();

        let flush = |buf: &mut String, idx: &mut usize, out: &mut Vec<Chunk>| {
            let trimmed = buf.trim();
            if !trimmed.is_empty() {
                out.push(Chunk {
                    text: trimmed.to_string(),
                    chunk_index: *idx,
                    token_count: Some(trimmed.split_whitespace().count()),
                    metadata: json!({}),
                });
                *idx += 1;
            }
            buf.clear();
        };

        let mut idx = 0;
        for para in text.split("\n\n") {
            let para = para.trim();
            if para.is_empty() {
                continue;
            }
            if !current.is_empty() && current.len() + para.len() + 2 > self.max_chars {
                flush(&mut current, &mut idx, &mut chunks);
            }
            if para.len() > self.max_chars {
                // Hard-split oversized paragraphs on the char budget.
                flush(&mut current, &mut idx, &mut chunks);
                let mut start = 0;
                let bytes: Vec<char> = para.chars().collect();
                while start < bytes.len() {
                    let end = (start + self.max_chars).min(bytes.len());
                    let slice: String = bytes[start..end].iter().collect();
                    let mut buf = slice;
                    flush(&mut buf, &mut idx, &mut chunks);
                    start = end;
                }
            } else {
                if !current.is_empty() {
                    current.push_str("\n\n");
                }
                current.push_str(para);
            }
        }
        flush(&mut current, &mut idx, &mut chunks);
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_paragraphs_within_budget() {
        let chunker = SimpleChunker::new(40);
        let text = "first paragraph here.\n\nsecond paragraph also here.\n\nthird.";
        let chunks = chunker.chunk(text);
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0].chunk_index, 0);
        assert!(chunks.iter().all(|c| !c.text.is_empty()));
    }
}
