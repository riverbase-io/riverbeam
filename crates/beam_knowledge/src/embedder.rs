use async_trait::async_trait;

use riverbase_core::RiverbaseResult;

/// Turns text into embedding vectors.
#[async_trait]
pub trait Embedder: Send + Sync {
    fn name(&self) -> String;
    fn dimensions(&self) -> usize;
    async fn embed(&self, texts: &[String]) -> RiverbaseResult<Vec<Vec<f32>>>;

    async fn embed_query(&self, text: &str) -> RiverbaseResult<Vec<f32>> {
        let mut out = self.embed(&[text.to_string()]).await?;
        out.pop()
            .ok_or_else(|| crate::BEM_220.raise())
    }
}

/// Deterministic hashing embedder — bag-of-words token hashing into a fixed
/// dimension, L2-normalized. Hermetic stand-in for a real provider embedder:
/// semantically-overlapping text yields higher cosine similarity.
pub struct HashEmbedder {
    dim: usize,
}

impl HashEmbedder {
    #[must_use]
    pub fn new(dim: usize) -> Self {
        Self { dim: dim.max(1) }
    }
}

impl Default for HashEmbedder {
    fn default() -> Self {
        Self::new(64)
    }
}

fn token_hash(token: &str) -> u64 {
    // FNV-1a 64-bit.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in token.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[async_trait]
impl Embedder for HashEmbedder {
    fn name(&self) -> String {
        format!("hash:{}", self.dim)
    }

    fn dimensions(&self) -> usize {
        self.dim
    }

    async fn embed(&self, texts: &[String]) -> RiverbaseResult<Vec<Vec<f32>>> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            let mut vec = vec![0.0_f32; self.dim];
            for token in text.split(|c: char| !c.is_alphanumeric()) {
                if token.is_empty() {
                    continue;
                }
                let token = token.to_lowercase();
                #[allow(clippy::cast_possible_truncation)]
                let idx = (token_hash(&token) as usize) % self.dim;
                vec[idx] += 1.0;
            }
            let norm = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
            if norm > 0.0 {
                for v in &mut vec {
                    *v /= norm;
                }
            }
            out.push(vec);
        }
        Ok(out)
    }
}

/// Deterministic fixed-width embedder for tests that must match a SQL
/// `VECTOR(dim)` column (typically 1536). Not [`HashEmbedder`]: never pair a
/// hash embedder with pgvector.
pub struct MockEmbedder {
    dim: usize,
    model: String,
}

impl MockEmbedder {
    /// `dimensions` must be ≥ 4 (same floor as the Python mock).
    #[must_use]
    pub fn new(dimensions: usize) -> Self {
        Self {
            dim: dimensions.max(4),
            model: "mock-1".into(),
        }
    }

    #[must_use]
    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }
}

impl Default for MockEmbedder {
    fn default() -> Self {
        Self::new(1536)
    }
}

#[async_trait]
impl Embedder for MockEmbedder {
    fn name(&self) -> String {
        format!("mock:{}", self.model)
    }

    fn dimensions(&self) -> usize {
        self.dim
    }

    async fn embed(&self, texts: &[String]) -> RiverbaseResult<Vec<Vec<f32>>> {
        HashEmbedder::new(self.dim).embed(texts).await
    }
}

/// HTTP `POST {base}/embeddings` (OpenAI-compatible, including NVIDIA).
pub struct OpenAiCompatEmbedder {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
    dim: usize,
    vendor: &'static str,
}

impl OpenAiCompatEmbedder {
    #[must_use]
    #[allow(clippy::similar_names)]
    pub fn openai(base_url: impl Into<String>, api_key: impl Into<String>, model: impl Into<String>, dim: usize) -> Self {
        Self::new("openai", base_url, api_key, model, dim)
    }

    #[must_use]
    #[allow(clippy::similar_names)]
    pub fn nvidia(base_url: impl Into<String>, api_key: impl Into<String>, model: impl Into<String>, dim: usize) -> Self {
        Self::new("nvidia", base_url, api_key, model, dim)
    }

    fn new(
        vendor: &'static str,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        dim: usize,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
            dim: dim.max(1),
            vendor,
        }
    }

    fn embeddings_url(&self) -> String {
        if self.base_url.ends_with("/embeddings") {
            self.base_url.clone()
        } else {
            format!("{}/embeddings", self.base_url)
        }
    }
}

#[async_trait]
impl Embedder for OpenAiCompatEmbedder {
    fn name(&self) -> String {
        format!("{}:{}", self.vendor, self.model)
    }

    fn dimensions(&self) -> usize {
        self.dim
    }

    async fn embed(&self, texts: &[String]) -> RiverbaseResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let body = serde_json::json!({
            "model": self.model,
            "input": texts,
            "dimensions": self.dim,
        });
        let resp = self
            .client
            .post(self.embeddings_url())
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| crate::BEM_221.with_data(e.to_string()))?;
        let status = resp.status();
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| crate::BEM_222.with_data(e.to_string()))?;
        if !status.is_success() {
            return Err(crate::BEM_223.with_data(format!("embeddings http {status}: {value}")));
        }
        parse_embeddings_response(&value, texts.len(), self.dim)
    }
}

#[allow(clippy::cast_possible_truncation)]
fn parse_embeddings_response(
    value: &serde_json::Value,
    expected: usize,
    dim: usize,
) -> RiverbaseResult<Vec<Vec<f32>>> {
    let data = value
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| crate::BEM_224.raise())?;
    let mut indexed: Vec<(usize, Vec<f32>)> = Vec::with_capacity(data.len());
    for item in data {
        let index = usize::try_from(
            item.get("index")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        )
        .unwrap_or(0);
        let embedding = item
            .get("embedding")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| crate::BEM_225.raise())?;
        let vec: Vec<f32> = embedding
            .iter()
            .filter_map(serde_json::Value::as_f64)
            .map(|v| v as f32)
            .collect();
        if vec.len() != dim {
            return Err(crate::BEM_226.with_data(format!("embedding width {} != expected {dim}", vec.len())));
        }
        indexed.push((index, vec));
    }
    indexed.sort_by_key(|(i, _)| *i);
    let out: Vec<Vec<f32>> = indexed.into_iter().map(|(_, v)| v).collect();
    if out.len() != expected {
        return Err(crate::BEM_227.with_data(format!("got {} embeddings, expected {expected}", out.len())));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn similar_text_embeds_closer() {
        let embedder = HashEmbedder::new(128);
        let vecs = embedder
            .embed(&[
                "the quick brown fox".into(),
                "the quick brown fox jumps".into(),
                "completely unrelated content here".into(),
            ])
            .await
            .unwrap();
        let cos = |a: &[f32], b: &[f32]| -> f32 { a.iter().zip(b).map(|(x, y)| x * y).sum() };
        let close = cos(&vecs[0], &vecs[1]);
        let far = cos(&vecs[0], &vecs[2]);
        assert!(close > far, "similar text should score higher: {close} > {far}");
    }

    #[tokio::test]
    async fn mock_embedder_is_1536_by_default() {
        let embedder = MockEmbedder::default();
        assert_eq!(embedder.dimensions(), 1536);
        assert!(embedder.name().starts_with("mock:"));
        let vecs = embedder.embed(&["hello world".into()]).await.unwrap();
        assert_eq!(vecs[0].len(), 1536);
    }

    #[tokio::test]
    async fn openai_compat_embedder_parses_mock_http() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let dim = 8;
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let embedding: Vec<f32> = (0..dim).map(|i| f32::from(u8::try_from(i).unwrap_or(0)) * 0.1).collect();
            let body = serde_json::json!({
                "data": [{ "index": 0, "embedding": embedding }]
            });
            let body = body.to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(resp.as_bytes()).await;
        });

        let embedder = OpenAiCompatEmbedder::openai(
            format!("http://{addr}/v1"),
            "test-key",
            "text-embedding-3-small",
            dim,
        );
        let out = embedder.embed(&["hello".into()]).await.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), dim);
        assert!((out[0][1] - 0.1).abs() < f32::EPSILON);
    }
}
