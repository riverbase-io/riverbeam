use crate::types::Hit;

/// Injects retrieved knowledge into the model context.
///
/// Ports `RAGContextStrategy`: formats top hits into a single system preamble
/// the executor prepends after memory trimming when RAG is enabled.
pub struct RagContextStrategy {
    header: String,
}

impl RagContextStrategy {
    #[must_use]
    pub fn new(header: impl Into<String>) -> Self {
        Self {
            header: header.into(),
        }
    }

    /// Render hits into a context block, or `None` when there is nothing to add.
    #[must_use]
    pub fn render(&self, hits: &[Hit]) -> Option<String> {
        if hits.is_empty() {
            return None;
        }
        let mut out = String::new();
        out.push_str(&self.header);
        out.push('\n');
        for (i, hit) in hits.iter().enumerate() {
            out.push_str(&format!("[{}] {}\n", i + 1, hit.text.trim()));
        }
        Some(out)
    }
}

impl Default for RagContextStrategy {
    fn default() -> Self {
        Self::new("Relevant knowledge:")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hit(text: &str) -> Hit {
        Hit {
            id: "1".into(),
            text: text.into(),
            score: 0.9,
            metadata: json!({}),
            document_id: None,
            chunk_index: None,
            source: None,
            scope: None,
        }
    }

    #[test]
    fn render_empty_is_none() {
        assert!(RagContextStrategy::default().render(&[]).is_none());
    }

    #[test]
    fn render_formats_numbered_snippets() {
        let block = RagContextStrategy::default()
            .render(&[hit("first"), hit("second")])
            .unwrap();
        assert!(block.contains("[1] first"));
        assert!(block.contains("[2] second"));
    }
}
