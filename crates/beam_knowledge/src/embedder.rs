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
        out.pop().ok_or_else(|| crate::BEM_220.raise())
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

/// Deterministic bag-of-tokens hashing (BLAKE2b-64),
/// L2-normalized. Vectors equal Python's for the same text, dimension and
/// model, so fixtures are shared across the stacks.
pub struct MockEmbedder {
    dim: usize,
    model: String,
}

impl MockEmbedder {
    /// `dimensions` below 4 is raised to 4 (Python refuses it).
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

    /// Python `_vectorize`, computed in `f64` as Python does.
    #[must_use]
    pub fn vectorize(&self, text: &str) -> Vec<f32> {
        let mut vec = vec![0.0_f64; self.dim];
        for token in text
            .to_lowercase()
            .split(py_space)
            .filter(|t| !t.is_empty())
        {
            let digest = crate::blake2b::blake2b(token.as_bytes(), 8);
            let idx = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) as usize
                % self.dim;
            vec[idx] += if digest[4] & 1 == 1 { 1.0 } else { -1.0 };
        }
        let norm = vec.iter().map(|v| v * v).sum::<f64>().sqrt();
        let norm = if norm == 0.0 { 1.0 } else { norm };
        #[allow(clippy::cast_possible_truncation)]
        vec.iter().map(|v| (v / norm) as f32).collect()
    }
}

/// Python `str.split()` separators.
fn py_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

impl Default for MockEmbedder {
    /// Python's default is 32 dimensions; the SQL columns are 1536 wide.
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
        Ok(texts.iter().map(|t| self.vectorize(t)).collect())
    }
}

/// `KNOWLEDGE_EMBED_BATCH_SIZE` / `KNOWLEDGE_EMBED_MAX_RETRIES` (Python 64 / 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbedBatching {
    pub batch_size: usize,
    pub max_retries: u32,
    /// The first retry waits this long, doubling after (Python: 1s).
    pub backoff: std::time::Duration,
}

impl Default for EmbedBatching {
    fn default() -> Self {
        Self {
            batch_size: 64,
            max_retries: 4,
            backoff: std::time::Duration::from_secs(1),
        }
    }
}

impl EmbedBatching {
    /// From `KNOWLEDGE_EMBED_BATCH_SIZE` and `KNOWLEDGE_EMBED_MAX_RETRIES`.
    #[must_use]
    pub fn from_env() -> Self {
        let read = beam_env::number::<u64>;
        let default = Self::default();
        Self {
            batch_size: read("KNOWLEDGE_EMBED_BATCH_SIZE")
                .and_then(|v| usize::try_from(v).ok())
                .unwrap_or(default.batch_size),
            max_retries: read("KNOWLEDGE_EMBED_MAX_RETRIES")
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(default.max_retries),
            ..default
        }
    }
}

/// Embed `texts` in batches; each batch retries with
/// exponential backoff (`backoff * 2^attempt`) up to `max_retries` times.
///
/// # Errors
/// The last error of a batch that never succeeded.
pub async fn batched_embed<F, Fut>(
    texts: &[String],
    batching: EmbedBatching,
    label: &str,
    embed: F,
) -> RiverbaseResult<Vec<Vec<f32>>>
where
    F: Fn(Vec<String>) -> Fut,
    Fut: std::future::Future<Output = RiverbaseResult<Vec<Vec<f32>>>>,
{
    let mut out = Vec::with_capacity(texts.len());
    for batch in texts.chunks(batching.batch_size.max(1)) {
        let mut attempt = 0_u32;
        loop {
            match embed(batch.to_vec()).await {
                Ok(vectors) => {
                    out.extend(vectors);
                    break;
                }
                // A bad response shape or width will not change on retry.
                Err(e) if attempt < batching.max_retries && retryable(&e) => {
                    let wait = batching
                        .backoff
                        .saturating_mul(2_u32.saturating_pow(attempt));
                    tracing::warn!(
                        embedder = label,
                        attempt = attempt + 1,
                        error = %e,
                        detail = %e.errdata,
                        "embed attempt failed; retrying"
                    );
                    tokio::time::sleep(wait).await;
                    attempt += 1;
                }
                Err(e) => {
                    tracing::error!(
                        embedder = label,
                        attempts = attempt + 1,
                        batch = batch.len(),
                        "embedding failed"
                    );
                    return Err(e);
                }
            }
        }
    }
    Ok(out)
}

/// Whether an embed error may succeed on retry (transport and status
/// errors), as opposed to a malformed or mis-sized answer.
/// Transport errors, unreadable answers, rate limits and server errors may
/// pass on a retry; a refused request (any other 4xx) will not.
fn retryable(e: &riverbase_core::RiverbaseError) -> bool {
    match e.errcode.as_str() {
        "BEM-221" | "BEM-222" => true,
        "BEM-223" => e
            .errdata
            .get("status")
            .and_then(serde_json::Value::as_u64)
            .is_none_or(|status| status == 429 || status >= 500),
        _ => false,
    }
}

/// The JSON answer of an embedding call. An error status is `BEM-223` with
/// the status and the provider's answer (its JSON, else its text), which
/// says what was wrong with the request.
async fn json_answer(resp: reqwest::Response) -> RiverbaseResult<serde_json::Value> {
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| crate::BEM_222.with_data(e.to_string()))?;
    if !status.is_success() {
        let body = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
        return Err(
            crate::BEM_223.with_data(serde_json::json!({"status": status.as_u16(), "body": body}))
        );
    }
    serde_json::from_str(&text).map_err(|e| crate::BEM_222.with_data(e.to_string()))
}

/// An HTTP client with a request timeout (embedding calls must not hang a worker).
fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_mins(2))
        .build()
        .unwrap_or_default()
}

/// The D8 sidecar for Python's local `HuggingFaceEmbedder`: a Hugging Face
/// text-embeddings-inference server (`POST {url}/embed` with
/// `{"inputs": [...], "normalize": true}`, answering `[[...], ...]`). The name
/// is Python's (`huggingface:<model>`), so stored vectors keep their embedder.
pub struct HttpEmbedder {
    client: reqwest::Client,
    url: String,
    model: String,
    dim: usize,
    batching: EmbedBatching,
}

impl HttpEmbedder {
    #[must_use]
    pub fn new(url: impl Into<String>, model: impl Into<String>, dim: usize) -> Self {
        Self {
            client: http_client(),
            url: url.into().trim_end_matches('/').to_owned(),
            model: model.into(),
            dim: dim.max(1),
            batching: EmbedBatching::from_env(),
        }
    }

    #[must_use]
    pub fn with_batching(mut self, batching: EmbedBatching) -> Self {
        self.batching = batching;
        self
    }

    async fn embed_batch(&self, texts: Vec<String>) -> RiverbaseResult<Vec<Vec<f32>>> {
        let resp = self
            .client
            .post(format!("{}/embed", self.url))
            .json(&serde_json::json!({"inputs": texts, "normalize": true}))
            .send()
            .await
            .map_err(|e| crate::BEM_221.with_data(e.to_string()))?;
        let value = json_answer(resp).await?;
        let rows = value.as_array().ok_or_else(|| crate::BEM_224.raise())?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let row = row.as_array().ok_or_else(|| crate::BEM_225.raise())?;
            #[allow(clippy::cast_possible_truncation)]
            let vector: Vec<f32> = row
                .iter()
                .filter_map(serde_json::Value::as_f64)
                .map(|v| v as f32)
                .collect();
            if vector.len() != self.dim {
                return Err(crate::BEM_226.with_data(format!(
                    "embedding width {} != expected {}",
                    vector.len(),
                    self.dim
                )));
            }
            out.push(vector);
        }
        if out.len() != texts.len() {
            return Err(crate::BEM_227.with_data(format!(
                "got {} embeddings, expected {}",
                out.len(),
                texts.len()
            )));
        }
        Ok(out)
    }
}

#[async_trait]
impl Embedder for HttpEmbedder {
    fn name(&self) -> String {
        format!("huggingface:{}", self.model)
    }

    fn dimensions(&self) -> usize {
        self.dim
    }

    async fn embed(&self, texts: &[String]) -> RiverbaseResult<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let name = self.name();
        batched_embed(texts, self.batching, &name, |batch| self.embed_batch(batch)).await
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
    batching: EmbedBatching,
}

impl OpenAiCompatEmbedder {
    #[must_use]
    #[allow(clippy::similar_names)]
    pub fn openai(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        dim: usize,
    ) -> Self {
        Self::new("openai", base_url, api_key, model, dim)
    }

    #[must_use]
    #[allow(clippy::similar_names)]
    pub fn nvidia(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
        dim: usize,
    ) -> Self {
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
            client: http_client(),
            base_url: base_url.into().trim_end_matches('/').to_string(),
            api_key: api_key.into(),
            model: model.into(),
            dim: dim.max(1),
            vendor,
            batching: EmbedBatching::from_env(),
        }
    }

    #[must_use]
    pub fn with_batching(mut self, batching: EmbedBatching) -> Self {
        self.batching = batching;
        self
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
        // Python's OpenAI and NVIDIA embedders batch and retry (`batched_embed`).
        let name = self.name();
        batched_embed(texts, self.batching, &name, |batch| {
            self.embed_batch(batch, InputType::Passage)
        })
        .await
    }

    async fn embed_query(&self, text: &str) -> RiverbaseResult<Vec<f32>> {
        let name = self.name();
        let mut out = batched_embed(&[text.to_owned()], self.batching, &name, |batch| {
            self.embed_batch(batch, InputType::Query)
        })
        .await?;
        out.pop().ok_or_else(|| crate::BEM_220.raise())
    }
}

/// What a text is to an asymmetric retrieval model (NVIDIA's `input_type`).
#[derive(Debug, Clone, Copy)]
enum InputType {
    /// Stored text: document chunks.
    Passage,
    /// Search text.
    Query,
}

impl InputType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Passage => "passage",
            Self::Query => "query",
        }
    }
}

impl OpenAiCompatEmbedder {
    /// NVIDIA's retrieval models are asymmetric: a text is a `passage` or a
    /// `query`. Their width is the model's own, so no `dimensions` (most sizes
    /// are refused); an over-long text is cut at the end. `OpenAI` takes
    /// `dimensions` on `text-embedding-3-*`.
    fn request_body(&self, texts: &[String], input_type: InputType) -> serde_json::Value {
        if self.vendor == "nvidia" {
            serde_json::json!({
                "model": self.model,
                "input": texts,
                "input_type": input_type.as_str(),
                "truncate": "END",
            })
        } else {
            serde_json::json!({
                "model": self.model,
                "input": texts,
                "dimensions": self.dim,
            })
        }
    }

    async fn embed_batch(
        &self,
        texts: Vec<String>,
        input_type: InputType,
    ) -> RiverbaseResult<Vec<Vec<f32>>> {
        let texts = &texts;
        let body = self.request_body(texts, input_type);
        let resp = self
            .client
            .post(self.embeddings_url())
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| crate::BEM_221.with_data(e.to_string()))?;
        let value = json_answer(resp).await?;
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
            return Err(crate::BEM_226
                .with_data(format!("embedding width {} != expected {dim}", vec.len())));
        }
        indexed.push((index, vec));
    }
    indexed.sort_by_key(|(i, _)| *i);
    let out: Vec<Vec<f32>> = indexed.into_iter().map(|(_, v)| v).collect();
    if out.len() != expected {
        return Err(
            crate::BEM_227.with_data(format!("got {} embeddings, expected {expected}", out.len()))
        );
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
        assert!(
            close > far,
            "similar text should score higher: {close} > {far}"
        );
    }

    /// Python `MockEmbedder(dimensions=n)._vectorize(text)`.
    #[test]
    fn mock_embedder_matches_python_vectors() {
        let small = MockEmbedder::new(8);
        let expected: Vec<f32> = [
            -0.894_427_190_999_915_9_f64,
            0.447_213_595_499_957_9,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
        ]
        .iter()
        .map(|v| {
            #[allow(clippy::cast_possible_truncation)]
            let v = *v as f32;
            v
        })
        .collect();
        assert_eq!(
            small.vectorize("Alpha document about INVOICES alpha"),
            expected
        );
        assert_eq!(small.vectorize(""), vec![0.0; 8]);
        let wide = MockEmbedder::new(1536).vectorize("hello world");
        let nonzero: Vec<(usize, f32)> = wide
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, v)| *v != 0.0)
            .collect();
        assert_eq!(nonzero, [(424, 0.707_106_77), (1407, 0.707_106_77)]);
    }

    #[tokio::test]
    async fn batched_embed_retries_then_succeeds() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        let batching = EmbedBatching {
            batch_size: 2,
            max_retries: 2,
            backoff: std::time::Duration::from_millis(1),
        };
        let texts: Vec<String> = ["a", "b", "c"].iter().map(|s| (*s).to_owned()).collect();
        let out = batched_embed(&texts, batching, "test", |batch| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    Err(crate::BEM_221.raise())
                } else {
                    Ok(batch.iter().map(|_| vec![1.0]).collect())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "one retry, then two batches"
        );
        let failing = batched_embed(
            &texts,
            EmbedBatching {
                max_retries: 1,
                ..batching
            },
            "test",
            |_| async { Err::<Vec<Vec<f32>>, _>(crate::BEM_221.raise()) },
        )
        .await;
        assert!(failing.is_err());
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
            let embedding: Vec<f32> = (0..dim)
                .map(|i| f32::from(u8::try_from(i).unwrap_or(0)) * 0.1)
                .collect();
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

    /// One whole HTTP request's JSON body from `stream`.
    async fn request_body(stream: &mut tokio::net::TcpStream) -> serde_json::Value {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            if n == 0 {
                return serde_json::Value::Null;
            }
            buf.extend_from_slice(&chunk[..n]);
            let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                continue;
            };
            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
            let len = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if buf.len() >= end + 4 + len {
                return serde_json::from_slice(&buf[end + 4..end + 4 + len]).unwrap();
            }
        }
    }

    /// Answer one connection per `(status, body)`, in order, then stop
    /// listening. Yields the request bodies it saw.
    async fn serve(
        responses: Vec<(u16, String)>,
    ) -> (String, tokio::task::JoinHandle<Vec<serde_json::Value>>) {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let seen = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                bodies.push(request_body(&mut stream).await);
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(resp.as_bytes()).await.unwrap();
            }
            bodies
        });
        (base, seen)
    }

    #[tokio::test]
    async fn nvidia_embedder_marks_passages_and_queries_and_sends_no_dimensions() {
        let ok = serde_json::json!({"data": [{"index": 0, "embedding": [0.1, 0.2, 0.3, 0.4]}]})
            .to_string();
        let (base, seen) = serve(vec![(200, ok.clone()), (200, ok)]).await;
        let model = "nvidia/llama-3.2-nv-embedqa-1b-v2";
        let embedder = OpenAiCompatEmbedder::nvidia(base, "test-key", model, 4);
        embedder.embed(&["a document".into()]).await.unwrap();
        embedder.embed_query("a question").await.unwrap();
        let bodies = seen.await.unwrap();
        // NVIDIA's retrieval models are asymmetric, and their width is the
        // model's own: `dimensions` is refused for most sizes.
        assert_eq!(bodies[0]["model"], model);
        assert_eq!(bodies[0]["input_type"], "passage", "{}", bodies[0]);
        assert_eq!(bodies[1]["input_type"], "query", "{}", bodies[1]);
        assert!(bodies[0].get("dimensions").is_none(), "{}", bodies[0]);
    }

    #[tokio::test]
    async fn a_client_error_is_not_retried_and_keeps_the_providers_answer() {
        let refusal = serde_json::json!({"detail": "input_type is required"}).to_string();
        let (base, seen) = serve(vec![(400, refusal)]).await;
        let embedder =
            OpenAiCompatEmbedder::nvidia(base, "test-key", "m", 4).with_batching(EmbedBatching {
                batch_size: 8,
                max_retries: 3,
                backoff: std::time::Duration::from_millis(1),
            });
        let err = embedder.embed(&["a document".into()]).await.unwrap_err();
        // A retry would have found no server (BEM-221) instead.
        assert_eq!(err.errcode.as_str(), "BEM-223", "{err:?}");
        assert_eq!(err.errdata["status"], 400, "{}", err.errdata);
        assert_eq!(err.errdata["body"]["detail"], "input_type is required");
        assert_eq!(seen.await.unwrap().len(), 1);
    }
}
