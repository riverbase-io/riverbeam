//! Chunkers (Python `knowledge/chunkers.py`). Lengths are in code points, as
//! Python's `len` counts them.

use crate::types::{Chunk, ParsedBlock};

pub const DEFAULT_CHUNK_SIZE: usize = 800;
pub const DEFAULT_CHUNK_OVERLAP: usize = 120;

/// Splits parsed blocks into chunks.
pub trait Chunker: Send + Sync {
    fn chunk(&self, blocks: &[ParsedBlock]) -> Vec<Chunk>;
}

/// Python `str.isspace` (what `\s` and `strip()` match).
fn py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

fn py_strip(s: &str) -> &str {
    s.trim_matches(py_space)
}

/// `max(1, len(text) // 4)`.
#[must_use]
pub fn approx_tokens(text: &str) -> usize {
    (text.chars().count() / 4).max(1)
}

/// `re.split(r"\n\s*\n", text)`: a break is a whitespace run from a newline
/// to the last newline of that run.
fn split_paragraphs(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\n' {
            let mut j = i + 1;
            let mut last_newline = None;
            while j < chars.len() && py_space(chars[j]) {
                if chars[j] == '\n' {
                    last_newline = Some(j);
                }
                j += 1;
            }
            if let Some(end) = last_newline {
                out.push(chars[start..i].iter().collect());
                start = end + 1;
                i = end + 1;
                continue;
            }
        }
        i += 1;
    }
    out.push(chars[start..].iter().collect());
    out
}

/// `re.split(r"(?<=[.!?])\s+", text)`.
fn split_sentences(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut start = 0;
    let mut i = 0;
    while i < chars.len() {
        if py_space(chars[i]) && i > 0 && matches!(chars[i - 1], '.' | '!' | '?') {
            let mut j = i;
            while j < chars.len() && py_space(chars[j]) {
                j += 1;
            }
            out.push(chars[start..i].iter().collect());
            start = j;
            i = j;
            continue;
        }
        i += 1;
    }
    out.push(chars[start..].iter().collect());
    out
}

/// Paragraph, then sentence, then a hard cut, with
/// `chunk_overlap` tokens (4 characters each) shared between hard-cut windows.
#[derive(Debug, Clone, Copy)]
pub struct RecursiveChunker {
    size: usize,
    overlap: usize,
}

impl RecursiveChunker {
    /// # Panics
    /// When `chunk_overlap >= chunk_size` (Python raises `ValueError`).
    #[must_use]
    pub fn new(chunk_size: usize, chunk_overlap: usize) -> Self {
        assert!(
            chunk_overlap < chunk_size,
            "chunk_overlap must be < chunk_size"
        );
        Self {
            size: chunk_size,
            overlap: chunk_overlap,
        }
    }

    fn split(text: &str, size: usize) -> Vec<String> {
        if text.is_empty() {
            return Vec::new();
        }
        if approx_tokens(text) <= size {
            return vec![text.to_owned()];
        }
        let mut out = Vec::new();
        for para in split_paragraphs(text) {
            if approx_tokens(&para) <= size {
                out.push(para);
                continue;
            }
            let mut buf = String::new();
            for sent in split_sentences(&para) {
                let joined = format!("{buf} {sent}");
                if approx_tokens(&joined) <= size {
                    py_strip(&joined).clone_into(&mut buf);
                } else {
                    if !buf.is_empty() {
                        out.push(std::mem::take(&mut buf));
                    }
                    buf = sent;
                }
            }
            if !buf.is_empty() {
                out.push(buf);
            }
        }
        out
    }

    fn pack(text: &str, size: usize, overlap: usize) -> Vec<String> {
        let (char_size, char_overlap) = (size * 4, overlap * 4);
        let chars: Vec<char> = text.chars().collect();
        if chars.len() <= char_size {
            return vec![text.to_owned()];
        }
        let mut out = Vec::new();
        let mut start = 0;
        while start < chars.len() {
            let end = (start + char_size).min(chars.len());
            out.push(chars[start..end].iter().collect());
            if end >= chars.len() {
                break;
            }
            start = end - char_overlap;
        }
        out
    }
}

impl Default for RecursiveChunker {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_OVERLAP)
    }
}

impl Chunker for RecursiveChunker {
    fn chunk(&self, blocks: &[ParsedBlock]) -> Vec<Chunk> {
        let mut out = Vec::new();
        for block in blocks {
            for piece in Self::split(&block.text, self.size) {
                for text in Self::pack(&piece, self.size, self.overlap) {
                    let text = py_strip(&text);
                    if text.is_empty() {
                        continue;
                    }
                    out.push(Chunk {
                        text: text.to_owned(),
                        chunk_index: out.len(),
                        token_count: Some(approx_tokens(text)),
                        metadata: block.metadata.clone(),
                    });
                }
            }
        }
        out
    }
}

/// Fixed windows of `cl100k_base` tokens.
#[derive(Debug, Clone, Copy)]
pub struct TokenChunker {
    size: usize,
    overlap: usize,
}

impl TokenChunker {
    /// # Panics
    /// When `chunk_overlap >= chunk_size`.
    #[must_use]
    pub fn new(chunk_size: usize, chunk_overlap: usize) -> Self {
        assert!(
            chunk_overlap < chunk_size,
            "chunk_overlap must be < chunk_size"
        );
        Self {
            size: chunk_size,
            overlap: chunk_overlap,
        }
    }
}

impl Default for TokenChunker {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_OVERLAP)
    }
}

impl Chunker for TokenChunker {
    fn chunk(&self, blocks: &[ParsedBlock]) -> Vec<Chunk> {
        let bpe = tiktoken_rs::cl100k_base_singleton();
        let mut out = Vec::new();
        for block in blocks {
            let tokens = bpe.encode_ordinary(&block.text);
            let mut start = 0;
            while start < tokens.len() {
                let end = (start + self.size).min(tokens.len());
                let window = &tokens[start..end];
                // Python decodes with `errors="replace"`: a window may cut a
                // multi-byte character.
                let bytes = bpe.decode_bytes(window).unwrap_or_default();
                let text = String::from_utf8_lossy(&bytes);
                let text = py_strip(&text);
                if !text.is_empty() {
                    out.push(Chunk {
                        text: text.to_owned(),
                        chunk_index: out.len(),
                        token_count: Some(window.len()),
                        metadata: block.metadata.clone(),
                    });
                }
                if end >= tokens.len() {
                    break;
                }
                start = end - self.overlap;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitters_match_python_regexes() {
        // re.split(r"\n\s*\n", "a\n \n\nb\nc\n\n  d")
        assert_eq!(
            split_paragraphs("a\n \n\nb\nc\n\n  d"),
            ["a", "b\nc", "  d"]
        );
        // re.split(r"(?<=[.!?])\s+", "One. Two!  Three? four.five")
        assert_eq!(
            split_sentences("One. Two!  Three? four.five"),
            ["One.", "Two!", "Three?", "four.five"]
        );
    }

    #[test]
    fn short_text_is_one_chunk() {
        let chunks = RecursiveChunker::default().chunk(&[ParsedBlock::new("  hello world  ")]);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "hello world");
        assert_eq!(chunks[0].token_count, Some(2));
    }
}
