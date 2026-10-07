use crate::types::Chunk;

/// Ingest QA statistics computed over a document's chunks.
///
/// The Python pipeline uses Polars for corpus QA (chunk stats, dedup). Enable
/// the `polars-ingest` feature to compute these via a Polars `DataFrame`;
/// otherwise an equivalent pure-Rust pass is used. Both produce identical
/// values and land in the document's SQL `metadata_json`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct IngestStats {
    pub chunk_count: usize,
    pub total_tokens: usize,
    pub mean_tokens: f64,
    pub min_tokens: usize,
    pub max_tokens: usize,
    pub duplicate_chunks: usize,
    pub engine: String,
}

#[cfg(not(feature = "polars-ingest"))]
#[must_use]
pub fn ingest_stats(chunks: &[Chunk]) -> IngestStats {
    use std::collections::HashSet;

    let tokens: Vec<usize> = chunks
        .iter()
        .map(|c| c.token_count.unwrap_or_else(|| c.text.split_whitespace().count()))
        .collect();
    let total: usize = tokens.iter().sum();
    let count = chunks.len();
    let mean = if count == 0 { 0.0 } else { total as f64 / count as f64 };
    let mut seen = HashSet::new();
    let duplicates = chunks
        .iter()
        .filter(|c| !seen.insert(c.text.trim().to_string()))
        .count();
    IngestStats {
        chunk_count: count,
        total_tokens: total,
        mean_tokens: mean,
        min_tokens: tokens.iter().copied().min().unwrap_or(0),
        max_tokens: tokens.iter().copied().max().unwrap_or(0),
        duplicate_chunks: duplicates,
        engine: "rust".into(),
    }
}

#[cfg(feature = "polars-ingest")]
#[must_use]
pub fn ingest_stats(chunks: &[Chunk]) -> IngestStats {
    use polars::prelude::*;

    if chunks.is_empty() {
        return IngestStats {
            chunk_count: 0,
            total_tokens: 0,
            mean_tokens: 0.0,
            min_tokens: 0,
            max_tokens: 0,
            duplicate_chunks: 0,
            engine: "polars".into(),
        };
    }

    let texts: Vec<&str> = chunks.iter().map(|c| c.text.trim()).collect();
    let tokens: Vec<i64> = chunks
        .iter()
        .map(|c| {
            i64::try_from(c.token_count.unwrap_or_else(|| c.text.split_whitespace().count()))
                .unwrap_or(i64::MAX)
        })
        .collect();
    let df = df!("text" => texts.clone(), "tokens" => tokens.clone())
        .expect("ingest stats dataframe");

    let total: i64 = tokens.iter().sum();
    let count = chunks.len();
    let unique = df
        .column("text")
        .ok()
        .and_then(|c| c.as_series().map(|s| s.unique().map(|u| u.len())))
        .and_then(Result::ok)
        .unwrap_or(count);

    IngestStats {
        chunk_count: count,
        total_tokens: usize::try_from(total).unwrap_or(0),
        mean_tokens: total as f64 / count as f64,
        min_tokens: usize::try_from(tokens.iter().copied().min().unwrap_or(0)).unwrap_or(0),
        max_tokens: usize::try_from(tokens.iter().copied().max().unwrap_or(0)).unwrap_or(0),
        duplicate_chunks: count.saturating_sub(unique),
        engine: "polars".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chunk(text: &str, tokens: usize) -> Chunk {
        Chunk {
            text: text.into(),
            chunk_index: 0,
            token_count: Some(tokens),
            metadata: json!({}),
        }
    }

    #[test]
    fn computes_stats_and_duplicates() {
        let chunks = vec![chunk("alpha", 3), chunk("beta beta", 5), chunk("alpha", 3)];
        let stats = ingest_stats(&chunks);
        assert_eq!(stats.chunk_count, 3);
        assert_eq!(stats.total_tokens, 11);
        assert_eq!(stats.duplicate_chunks, 1);
        assert_eq!(stats.max_tokens, 5);
    }
}
