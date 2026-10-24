//! Long-term memory (Python `knowledge/longterm.py`): remember, edit, expire,
//! recall and forget over the `memory` namespace, and the extraction pipeline.

use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, SecondsFormat, Utc};
use riverbase_core::RiverbaseResult;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::embedder::Embedder;
use crate::store::VectorStore;
use crate::types::{Hit, MemoryType, Namespace, RetrievalFilter, VectorRecord};

/// Python `datetime.isoformat()` for an aware UTC time.
#[must_use]
pub fn iso(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Micros, false)
}

/// Python `datetime.fromisoformat`, naive times read as UTC.
#[must_use]
pub fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|t| t.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f")
                .ok()
                .map(|n| n.and_utc())
        })
        .or_else(|| {
            NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|n| n.and_utc())
        })
}

fn opt_string(v: Option<String>) -> Value {
    v.map_or(Value::Null, Value::String)
}

/// What to remember for a user.
#[derive(Debug, Clone)]
pub struct RememberRequest {
    pub user_id: String,
    pub content: String,
    pub memory_type: String,
    pub agent_id: Option<String>,
    pub importance: i64,
    pub expires_at: Option<DateTime<Utc>>,
    pub metadata: Map<String, Value>,
    pub memory_id: Option<String>,
    pub confidence: Option<f64>,
    pub source_execution_id: Option<String>,
    pub source_message_id: Option<String>,
    pub consent: bool,
}

impl RememberRequest {
    #[must_use]
    pub fn new(user_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            content: content.into(),
            memory_type: MemoryType::Semantic.as_str().into(),
            agent_id: None,
            importance: 1,
            expires_at: None,
            metadata: Map::new(),
            memory_id: None,
            confidence: None,
            source_execution_id: None,
            source_message_id: None,
            consent: true,
        }
    }
}

/// What to recall for a user.
#[derive(Debug, Clone)]
pub struct RecallRequest {
    pub query: String,
    pub user_id: String,
    pub agent_id: Option<String>,
    pub types: Option<Vec<String>>,
    pub k: usize,
    pub require_consent: bool,
    pub min_score: f64,
    pub min_confidence: Option<f64>,
}

impl RecallRequest {
    #[must_use]
    pub fn new(query: impl Into<String>, user_id: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            user_id: user_id.into(),
            agent_id: None,
            types: None,
            k: 5,
            require_consent: true,
            min_score: 0.0,
            min_confidence: None,
        }
    }
}

/// Long-term memories over the knowledge store.
#[derive(Clone)]
pub struct LongTermMemoryStore {
    embedder: Arc<dyn Embedder>,
    store: Arc<dyn VectorStore>,
}

impl LongTermMemoryStore {
    #[must_use]
    pub fn new(embedder: Arc<dyn Embedder>, store: Arc<dyn VectorStore>) -> Self {
        Self { embedder, store }
    }

    /// Persist a memory and return its id.
    ///
    /// # Errors
    /// `BEM-238` for blank content; embedder and store errors.
    pub async fn remember(&self, request: RememberRequest) -> RiverbaseResult<String> {
        if request.content.trim().is_empty() {
            return Err(crate::BEM_238.raise());
        }
        let embedding = self.embedder.embed_query(&request.content).await?;
        let id = request
            .memory_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let mut metadata = request.metadata;
        for (key, value) in [
            ("type", Value::String(request.memory_type)),
            ("importance", Value::from(request.importance)),
            ("expires_at", opt_string(request.expires_at.map(iso))),
            ("remembered_at", Value::String(iso(Utc::now()))),
            (
                "confidence",
                request
                    .confidence
                    .and_then(serde_json::Number::from_f64)
                    .map_or(Value::Null, Value::Number),
            ),
            ("consent", Value::Bool(request.consent)),
            ("revision", Value::from(1)),
            (
                "source_execution_id",
                opt_string(request.source_execution_id),
            ),
            ("source_message_id", opt_string(request.source_message_id)),
        ] {
            metadata.insert(key.into(), value);
        }
        let mut record =
            VectorRecord::new(id.clone(), Namespace::Memory, request.content, embedding);
        record.user_id = Some(request.user_id);
        record.agent_id = request.agent_id;
        record.embedder_name = Some(self.embedder.name());
        record.metadata = metadata;
        self.store.upsert(vec![record]).await?;
        Ok(id)
    }

    /// # Errors
    /// Store errors (`BEM-235` when the store cannot fetch by id).
    pub async fn get(&self, memory_id: &str) -> RiverbaseResult<Option<VectorRecord>> {
        self.store.get(memory_id, Namespace::Memory).await
    }

    /// New content (re-embedded) and/or expiry; the version goes up by one.
    ///
    /// # Errors
    /// `BEM-239` for an unknown memory; embedder and store errors.
    pub async fn edit(
        &self,
        memory_id: &str,
        content: Option<&str>,
        expires_at: Option<DateTime<Utc>>,
    ) -> RiverbaseResult<String> {
        let mut record = self
            .get(memory_id)
            .await?
            .ok_or_else(|| crate::BEM_239.with_data(format!("unknown memory {memory_id}")))?;
        if let Some(content) = content.filter(|c| !c.trim().is_empty()) {
            record.text = content.to_owned();
            record.embedding = self.embedder.embed_query(content).await?;
        }
        if let Some(expires_at) = expires_at {
            record
                .metadata
                .insert("expires_at".into(), Value::String(iso(expires_at)));
        }
        let current = ["version", "revision"]
            .iter()
            .find_map(|k| {
                record
                    .metadata
                    .get(*k)
                    .and_then(Value::as_i64)
                    .filter(|v| *v != 0)
            })
            .unwrap_or(1);
        record
            .metadata
            .insert("version".into(), Value::from(current + 1));
        record
            .metadata
            .insert("revision".into(), Value::from(current + 1));
        self.store.upsert(vec![record]).await?;
        Ok(memory_id.to_owned())
    }

    /// # Errors
    /// As [`Self::edit`].
    pub async fn expire(
        &self,
        memory_id: &str,
        expires_at: DateTime<Utc>,
    ) -> RiverbaseResult<String> {
        self.edit(memory_id, None, Some(expires_at)).await
    }

    /// The user's memories nearest to the query: expired, unconsented and
    /// low-score or low-confidence ones are dropped.
    ///
    /// # Errors
    /// Embedder and store errors.
    pub async fn recall(&self, request: &RecallRequest) -> RiverbaseResult<Vec<Hit>> {
        // No user, no memories (a blank id must not mean "every user").
        if request.query.trim().is_empty() || request.user_id.trim().is_empty() {
            return Ok(Vec::new());
        }
        let filters = RetrievalFilter {
            user_id: Some(request.user_id.clone()),
            agent_id: request.agent_id.clone(),
            memory_types: request.types.clone(),
            ..RetrievalFilter::default()
        };
        let vector = self.embedder.embed_query(&request.query).await?;
        let hits = self
            .store
            .query(&vector, request.k, Some(&filters), Namespace::Memory)
            .await?;
        let now = Utc::now();
        Ok(hits
            .into_iter()
            .filter(|hit| !is_expired(&hit.metadata, now))
            .filter(|hit| {
                !(request.require_consent
                    && hit.metadata.get("consent") == Some(&Value::Bool(false)))
            })
            .filter(|hit| request.min_score == 0.0 || hit.score >= request.min_score)
            .filter(|hit| {
                request.min_confidence.is_none_or(|min| {
                    hit.metadata
                        .get("confidence")
                        .and_then(Value::as_f64)
                        .is_some_and(|c| c >= min)
                })
            })
            .collect())
    }

    /// # Errors
    /// Store errors.
    pub async fn forget(&self, memory_id: &str) -> RiverbaseResult<usize> {
        self.store
            .delete(Some(&[memory_id.to_owned()]), None, Namespace::Memory)
            .await
    }

    /// Only with consent, a user, enough importance
    /// and some content.
    ///
    /// # Errors
    /// As [`Self::remember`].
    pub async fn maybe_remember(
        &self,
        request: RememberRequest,
        min_importance: i64,
    ) -> RiverbaseResult<Option<String>> {
        if !request.consent || request.user_id.is_empty() || request.importance < min_importance {
            return Ok(None);
        }
        if request.content.trim().is_empty() {
            return Ok(None);
        }
        self.remember(RememberRequest {
            consent: true,
            ..request
        })
        .await
        .map(Some)
    }
}

/// Whether a memory's `expires_at` has passed.
fn is_expired(metadata: &Map<String, Value>, now: DateTime<Utc>) -> bool {
    match metadata
        .get("expires_at")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        Some(raw) => parse_time(raw).is_some_and(|t| t <= now),
        None => false,
    }
}

/// Sentences become memories when confident enough and
/// not already remembered.
pub struct LtmPipeline {
    store: LongTermMemoryStore,
    pub min_confidence: f64,
}

/// Sentences split on `.`.
fn extract(text: &str) -> Vec<String> {
    let body = text.trim();
    if body.is_empty() {
        return Vec::new();
    }
    let parts: Vec<String> = body
        .replace('\n', " ")
        .split('.')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect();
    if parts.is_empty() {
        vec![body.to_owned()]
    } else {
        parts
    }
}

/// A memory's type from its text.
#[must_use]
pub fn classify(text: &str) -> MemoryType {
    let lowered = text.to_lowercase();
    if ["prefer", "like", "always", "never"]
        .iter()
        .any(|t| lowered.contains(t))
    {
        MemoryType::Preference
    } else if ["yesterday", "today", "last", "we met"]
        .iter()
        .any(|t| lowered.contains(t))
    {
        MemoryType::Episodic
    } else {
        MemoryType::Semantic
    }
}

/// How sure the extraction is that a sentence is worth remembering.
fn confidence(text: &str) -> f64 {
    if text.chars().count() >= 20 {
        0.85
    } else {
        0.3
    }
}

impl LtmPipeline {
    #[must_use]
    pub fn new(store: LongTermMemoryStore) -> Self {
        Self {
            store,
            min_confidence: 0.5,
        }
    }

    /// The ids of the memories remembered from `text`.
    ///
    /// # Errors
    /// Embedder and store errors.
    pub async fn run(
        &self,
        text: &str,
        user_id: &str,
        agent_id: Option<&str>,
        source_execution_id: Option<String>,
    ) -> RiverbaseResult<Vec<String>> {
        let mut remembered = Vec::new();
        for candidate in extract(text) {
            let conf = confidence(&candidate);
            if conf < self.min_confidence {
                continue;
            }
            let existing = self
                .store
                .recall(&RecallRequest {
                    k: 3,
                    ..RecallRequest::new(candidate.clone(), user_id)
                })
                .await?;
            let needle = candidate.trim().to_lowercase();
            if existing
                .iter()
                .any(|h| h.text.trim().to_lowercase() == needle)
            {
                continue;
            }
            let mut request = RememberRequest::new(user_id, candidate.clone());
            request.memory_type = classify(&candidate).as_str().into();
            request.agent_id = agent_id.map(str::to_owned);
            request.confidence = Some(conf);
            request.source_execution_id.clone_from(&source_execution_id);
            remembered.push(self.store.remember(request).await?);
        }
        Ok(remembered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedder::MockEmbedder;
    use crate::store::InMemoryVectorStore;

    fn store() -> LongTermMemoryStore {
        LongTermMemoryStore::new(
            Arc::new(MockEmbedder::new(32)),
            Arc::new(InMemoryVectorStore::new()),
        )
    }

    #[tokio::test]
    async fn edit_bumps_the_version_and_expire_hides_the_memory() {
        let ltm = store();
        let id = ltm
            .remember(RememberRequest::new("u1", "user prefers metric units"))
            .await
            .unwrap();
        ltm.edit(&id, Some("user prefers imperial units"), None)
            .await
            .unwrap();
        let record = ltm.get(&id).await.unwrap().unwrap();
        assert_eq!(record.text, "user prefers imperial units");
        assert_eq!(record.metadata["version"], 2);
        assert_eq!(record.metadata["revision"], 2);
        assert_eq!(
            ltm.recall(&RecallRequest::new("imperial units", "u1"))
                .await
                .unwrap()
                .len(),
            1
        );

        ltm.expire(&id, Utc::now() - chrono::Duration::seconds(1))
            .await
            .unwrap();
        assert!(ltm
            .recall(&RecallRequest::new("imperial units", "u1"))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(ltm.get(&id).await.unwrap().unwrap().metadata["version"], 3);
        // Another user never sees it.
        assert!(ltm
            .recall(&RecallRequest::new("imperial units", "u2"))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(ltm.forget(&id).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn pipeline_remembers_confident_new_sentences_once() {
        let ltm = store();
        let pipeline = LtmPipeline::new(ltm.clone());
        let text =
            "I always prefer window seats on flights. Ok. We met at the conference last spring.";
        let first = pipeline.run(text, "u1", None, None).await.unwrap();
        assert_eq!(first.len(), 2, "the short sentence is not confident enough");
        assert_eq!(
            ltm.get(&first[0]).await.unwrap().unwrap().metadata["type"],
            "preference"
        );
        assert!(
            pipeline
                .run(text, "u1", None, None)
                .await
                .unwrap()
                .is_empty(),
            "duplicates are skipped"
        );
    }
}
