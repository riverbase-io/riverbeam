//! Durable incremental session summaries (Python `transcript/summary.py`).
//! Canonical transcripts stay in the backend; a summary covers the older part.

use std::collections::HashMap;
use std::sync::Mutex;

use beam_types::{pyjson, AgentMessage, AgentMessageRole};
use chrono::{DateTime, Utc};
use riverbase_core::RiverbaseResult;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const SUMMARY_SCHEMA_VERSION: &str = "1";
const SERIALIZER_REVISION: &str = "beam-summary-v1";
/// Characters kept from each summarized message.
const CLIP: usize = 240;
/// Entries kept per summary list.
const KEEP: usize = 32;
/// Entries rendered per summary list.
const RENDER: usize = 8;

/// Counts a summary's tokens; the context pipeline passes the model's counter.
pub type CountTokens = dyn Fn(&str) -> usize + Send + Sync;

/// A summary of a session's older messages and what it covers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub coverage_start: usize,
    #[serde(default)]
    pub coverage_end: usize,
    #[serde(default)]
    pub coverage_through_sequence: usize,
    #[serde(default)]
    pub source_hash: String,
    #[serde(default)]
    pub facts: Vec<String>,
    #[serde(default)]
    pub commitments: Vec<String>,
    #[serde(default)]
    pub tool_outcomes: Vec<String>,
    #[serde(default)]
    pub citations: Vec<String>,
    #[serde(default)]
    pub preferences: Vec<String>,
    #[serde(default)]
    pub uncertainty: Vec<String>,
    #[serde(default)]
    pub token_count: usize,
    #[serde(default)]
    pub summarizer_model: Option<String>,
    #[serde(default)]
    pub prompt_revision: Option<String>,
    #[serde(default)]
    pub model_revision: Option<String>,
    #[serde(default)]
    pub serializer_revision: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub invalidated: bool,
    #[serde(default = "schema_version")]
    pub schema_version: String,
}

fn schema_version() -> String {
    SUMMARY_SCHEMA_VERSION.to_owned()
}

/// Summaries by session id, in process memory.
#[derive(Debug, Default)]
pub struct SummaryBook {
    rows: Mutex<HashMap<String, SessionSummary>>,
}

impl SummaryBook {
    #[must_use]
    pub fn get(&self, session_id: &str) -> Option<SessionSummary> {
        self.rows.lock().ok()?.get(session_id).cloned()
    }

    pub fn put(&self, summary: SessionSummary) {
        if let Ok(mut rows) = self.rows.lock() {
            rows.insert(summary.session_id.clone(), summary);
        }
    }

    /// Store `summary` unless a concurrent writer moved the coverage: the
    /// current end must equal `expected_end` (when given) and must not be
    /// past the new summary's end.
    pub fn cas_put(&self, summary: SessionSummary, expected_end: Option<usize>) -> bool {
        let Ok(mut rows) = self.rows.lock() else {
            return false;
        };
        if let Some(current) = rows.get(&summary.session_id) {
            if expected_end.is_some_and(|end| current.coverage_end != end) {
                return false;
            }
            if summary.coverage_end < current.coverage_end {
                return false;
            }
        }
        rows.insert(summary.session_id.clone(), summary);
        true
    }

    /// Without `from_sequence` the summary is dropped; otherwise one covering
    /// `from_sequence` is marked invalidated and its coverage cut before it.
    pub fn invalidate(&self, session_id: &str, from_sequence: Option<usize>) {
        let Ok(mut rows) = self.rows.lock() else {
            return;
        };
        let Some(from) = from_sequence else {
            rows.remove(session_id);
            return;
        };
        if let Some(current) = rows.get_mut(session_id) {
            if current.coverage_end >= from {
                current.invalidated = true;
                current.coverage_end = current.coverage_end.min(from.saturating_sub(1));
            }
        }
    }

    pub fn delete(&self, session_id: &str) {
        if let Ok(mut rows) = self.rows.lock() {
            rows.remove(session_id);
        }
    }
}

/// Where session summaries live (plan 023 B3): [`SummaryBook`] in process,
/// [`PgSummaryStore`] in `agent_session_summary`, durable and shared by
/// every worker. `coverage_end` is the last transcript sequence a summary
/// covers.
#[async_trait::async_trait]
pub trait SummaryStore: Send + Sync {
    /// # Errors
    /// Store errors.
    async fn get(&self, session_id: &str) -> RiverbaseResult<Option<SessionSummary>>;

    /// Store `summary` unless a concurrent writer moved the coverage (see
    /// [`SummaryBook::cas_put`]). Returns whether it was stored.
    ///
    /// # Errors
    /// Store errors.
    async fn cas_put(
        &self,
        summary: SessionSummary,
        expected_end: Option<usize>,
    ) -> RiverbaseResult<bool>;

    /// See [`SummaryBook::invalidate`].
    ///
    /// # Errors
    /// Store errors.
    async fn invalidate(
        &self,
        session_id: &str,
        from_sequence: Option<usize>,
    ) -> RiverbaseResult<()>;
}

#[async_trait::async_trait]
impl SummaryStore for SummaryBook {
    async fn get(&self, session_id: &str) -> RiverbaseResult<Option<SessionSummary>> {
        Ok(Self::get(self, session_id))
    }

    async fn cas_put(
        &self,
        summary: SessionSummary,
        expected_end: Option<usize>,
    ) -> RiverbaseResult<bool> {
        Ok(Self::cas_put(self, summary, expected_end))
    }

    async fn invalidate(
        &self,
        session_id: &str,
        from_sequence: Option<usize>,
    ) -> RiverbaseResult<()> {
        Self::invalidate(self, session_id, from_sequence);
        Ok(())
    }
}

/// [`SummaryStore`] over `agent_session_summary` (migration 0010): the
/// summary is the row's payload; the row lock makes the compare-and-set
/// safe across workers.
pub struct PgSummaryStore(pub beam_store::session_summary::PgSessionSummaries);

fn session_uuid(session_id: &str) -> RiverbaseResult<uuid::Uuid> {
    uuid::Uuid::parse_str(session_id.trim())
        .map_err(|_| crate::BEM_139.with_data(format!("session id {session_id} is not a UUID")))
}

fn sequence(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

#[async_trait::async_trait]
impl SummaryStore for PgSummaryStore {
    async fn get(&self, session_id: &str) -> RiverbaseResult<Option<SessionSummary>> {
        let Some(row) = self.0.get(session_uuid(session_id)?).await? else {
            return Ok(None);
        };
        let mut summary: SessionSummary = match serde_json::from_value(row.payload) {
            Ok(summary) => summary,
            Err(e) => {
                tracing::warn!(session_id, error = %e, "unreadable session summary");
                return Ok(None);
            }
        };
        // The columns are the truth: an edit cuts them without the payload.
        summary.coverage_end = usize::try_from(row.coverage_end).unwrap_or(0);
        summary.invalidated = row.invalidated;
        Ok(Some(summary))
    }

    async fn cas_put(
        &self,
        summary: SessionSummary,
        expected_end: Option<usize>,
    ) -> RiverbaseResult<bool> {
        let session = session_uuid(&summary.session_id)?;
        let payload =
            serde_json::to_value(&summary).map_err(|e| crate::BEM_139.with_data(e.to_string()))?;
        self.0
            .cas_put(
                session,
                sequence(summary.coverage_end),
                &payload,
                expected_end.map(sequence),
            )
            .await
    }

    async fn invalidate(
        &self,
        session_id: &str,
        from_sequence: Option<usize>,
    ) -> RiverbaseResult<()> {
        self.0
            .invalidate(session_uuid(session_id)?, from_sequence.map(sequence))
            .await
    }
}

fn role_label(role: AgentMessageRole) -> &'static str {
    match role {
        AgentMessageRole::System => "system",
        AgentMessageRole::Human => "human",
        AgentMessageRole::Ai => "ai",
        AgentMessageRole::Tool => "tool",
    }
}

/// Sha256 over each message's id, role, text and
/// `repr(tool_calls)`.
#[must_use]
pub fn messages_hash(messages: &[AgentMessage]) -> String {
    let mut digest = Sha256::new();
    for m in messages {
        digest.update(m.id.to_string().as_bytes());
        digest.update(role_label(m.role).as_bytes());
        digest.update(m.text_content().as_bytes());
        digest.update(pyjson::py_repr(&Value::Array(m.tool_calls.clone())).as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn clip(text: &str) -> String {
    text.chars().take(CLIP).collect()
}

fn tail(items: &[String], n: usize) -> &[String] {
    &items[items.len().saturating_sub(n)..]
}

/// Python `build_structured_summary` without an LLM summarizer: facts from
/// human turns, commitments from AI turns, outcomes from tool turns, appended
/// to the prior summary's lists.
#[must_use]
pub fn build_structured_summary(
    messages: &[AgentMessage],
    session_id: &str,
    coverage_start: usize,
    coverage_end: usize,
    prior: Option<&SessionSummary>,
    count: &CountTokens,
) -> SessionSummary {
    let mut facts = prior.map(|p| p.facts.clone()).unwrap_or_default();
    let mut commitments = prior.map(|p| p.commitments.clone()).unwrap_or_default();
    let mut tool_outcomes = prior.map(|p| p.tool_outcomes.clone()).unwrap_or_default();
    for m in messages {
        let text = m.text_content();
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        match m.role {
            AgentMessageRole::Human => facts.push(clip(text)),
            AgentMessageRole::Ai => commitments.push(clip(text)),
            AgentMessageRole::Tool => tool_outcomes.push(clip(text)),
            AgentMessageRole::System => {}
        }
    }
    let rendered = render_structured(&facts, &commitments, &tool_outcomes, prior);
    let delta_hash = messages_hash(messages);
    let source_hash = match prior {
        None => delta_hash,
        Some(p) => format!(
            "{:x}",
            Sha256::digest(format!("{}{delta_hash}", p.source_hash).as_bytes())
        ),
    };
    SessionSummary {
        session_id: session_id.to_owned(),
        tenant_id: prior.and_then(|p| p.tenant_id.clone()),
        token_count: count(&rendered),
        text: rendered,
        coverage_start: prior.map_or(coverage_start, |p| p.coverage_start),
        coverage_end,
        coverage_through_sequence: coverage_end,
        source_hash,
        facts: tail(&facts, KEEP).to_vec(),
        commitments: tail(&commitments, KEEP).to_vec(),
        tool_outcomes: tail(&tool_outcomes, KEEP).to_vec(),
        citations: prior.map(|p| p.citations.clone()).unwrap_or_default(),
        preferences: prior.map(|p| p.preferences.clone()).unwrap_or_default(),
        uncertainty: prior.map(|p| p.uncertainty.clone()).unwrap_or_default(),
        summarizer_model: None,
        prompt_revision: None,
        model_revision: None,
        serializer_revision: Some(SERIALIZER_REVISION.to_owned()),
        created_at: Utc::now(),
        invalidated: false,
        schema_version: schema_version(),
    }
}

fn render_structured(
    facts: &[String],
    commitments: &[String],
    tool_outcomes: &[String],
    prior: Option<&SessionSummary>,
) -> String {
    let mut lines = vec!["Conversation summary (untrusted):".to_owned()];
    if let Some(p) = prior.filter(|p| !p.text.is_empty()) {
        lines.push(p.text.clone());
    }
    if !facts.is_empty() {
        lines.push(format!("Facts: {}", tail(facts, RENDER).join(" | ")));
    }
    if !commitments.is_empty() {
        lines.push(format!(
            "Commitments: {}",
            tail(commitments, RENDER).join(" | ")
        ));
    }
    if !tool_outcomes.is_empty() {
        lines.push(format!(
            "Tool outcomes: {}",
            tail(tool_outcomes, RENDER).join(" | ")
        ));
    }
    if lines.len() == 1 {
        lines.push("Summary of earlier messages omitted for context limit".to_owned());
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(role: AgentMessageRole, text: &str) -> AgentMessage {
        AgentMessage::text(role, text)
    }

    fn chars(s: &str) -> usize {
        s.chars().count()
    }

    #[test]
    fn renders_like_python() {
        let messages = [
            msg(AgentMessageRole::Human, " my name is Ann "),
            msg(AgentMessageRole::Ai, "noted"),
            msg(AgentMessageRole::Tool, "42"),
            msg(AgentMessageRole::System, "ignored"),
        ];
        let s = build_structured_summary(&messages, "s1", 0, 3, None, &chars);
        assert_eq!(
            s.text,
            "Conversation summary (untrusted):\nFacts: my name is Ann\nCommitments: noted\nTool outcomes: 42"
        );
        assert_eq!(s.coverage_end, 3);
        assert_eq!(s.token_count, chars(&s.text));
        assert_eq!(s.source_hash, messages_hash(&messages));
    }

    #[test]
    fn incremental_summary_keeps_prior_text_and_start() {
        let first = build_structured_summary(
            &[msg(AgentMessageRole::Human, "a")],
            "s",
            0,
            0,
            None,
            &chars,
        );
        let next = build_structured_summary(
            &[msg(AgentMessageRole::Human, "b")],
            "s",
            5,
            1,
            Some(&first),
            &chars,
        );
        assert_eq!(next.coverage_start, 0);
        assert_eq!(next.facts, vec!["a", "b"]);
        assert!(next.text.starts_with("Conversation summary (untrusted):\nConversation summary (untrusted):\nFacts: a\nFacts: a | b"));
        assert_ne!(next.source_hash, first.source_hash);
    }

    #[test]
    fn empty_messages_render_placeholder() {
        let s = build_structured_summary(&[], "s", 0, 0, None, &chars);
        assert_eq!(
            s.text,
            "Conversation summary (untrusted):\nSummary of earlier messages omitted for context limit"
        );
    }

    #[test]
    fn cas_and_invalidate() {
        let book = SummaryBook::default();
        let s = build_structured_summary(&[], "s", 0, 5, None, &chars);
        assert!(book.cas_put(s.clone(), None));
        assert!(!book.cas_put(
            SessionSummary {
                coverage_end: 4,
                ..s.clone()
            },
            None
        ));
        assert!(!book.cas_put(
            SessionSummary {
                coverage_end: 9,
                ..s.clone()
            },
            Some(3)
        ));
        assert!(book.cas_put(
            SessionSummary {
                coverage_end: 9,
                ..s
            },
            Some(5)
        ));
        book.invalidate("s", Some(4));
        let row = book.get("s").unwrap();
        assert!(row.invalidated);
        assert_eq!(row.coverage_end, 3);
        book.invalidate("s", None);
        assert!(book.get("s").is_none());
    }
}
