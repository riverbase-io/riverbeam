//! Session transcripts (Python `transcript/`): where conversation messages are
//! kept, with a manager that adds durable incremental summaries. Budgeting is
//! the context composer's job, not the transcript's.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use beam_store::{BeamStore, MessagePage, MessagePhase, MessageRole, MessageRow, MessageType};
use beam_types::{
    AgentMessage, AgentMessagePhase, AgentMessageRole, AgentMessageType, ContentPart,
};
use riverbase_core::RiverbaseResult;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::summary::{
    build_structured_summary, CountTokens, SessionSummary, SummaryBook, SummaryStore,
};

/// Python's default page (`HistoryPaging.page`).
pub const DEFAULT_PAGE: usize = 200;
/// `metadata` key on the summary message [`TranscriptManager::load_history`] merges in.
pub const SUMMARY_MARKER: &str = "transcript_summary";
/// Messages kept verbatim after a summary (`TranscriptManager(keep_recent=10)`).
pub const KEEP_RECENT: usize = 10;

/// Optional stable-sequence cursors for [`TranscriptStore::load`].
pub type LoadRange = MessagePage;

/// Where conversation messages are persisted.
#[async_trait]
pub trait TranscriptStore: Send + Sync {
    /// The session's messages in sequence order; with a limit, the latest ones.
    ///
    /// # Errors
    /// Store errors.
    async fn load(&self, session_id: Uuid, range: LoadRange) -> RiverbaseResult<Vec<AgentMessage>>;

    /// Append messages to a session.
    ///
    /// # Errors
    /// Store errors.
    async fn save(
        &self,
        agent_id: Uuid,
        session_id: Uuid,
        messages: &[AgentMessage],
        user_id: Option<&str>,
    ) -> RiverbaseResult<()>;

    /// # Errors
    /// Store errors.
    async fn delete(&self, session_id: Uuid) -> RiverbaseResult<()>;

    /// # Errors
    /// Store errors.
    async fn list_sessions(&self, user_id: &str) -> RiverbaseResult<Vec<String>>;

    /// # Errors
    /// Store errors.
    async fn sequence_length(&self, session_id: Uuid) -> RiverbaseResult<usize>;
}

/// The sequence a stored message carries in `metadata.sequence`.
#[must_use]
pub fn sequence_of(message: &AgentMessage) -> i64 {
    message
        .metadata
        .get("sequence")
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

fn with_sequence(mut message: AgentMessage, sequence: usize) -> AgentMessage {
    if !message.metadata.is_object() {
        message.metadata = Value::Object(Map::new());
    }
    if let Some(map) = message.metadata.as_object_mut() {
        if map.get("sequence").is_none_or(Value::is_null) {
            map.insert("sequence".into(), Value::from(sequence));
        }
    }
    message
}

/// Python `InMemoryTranscriptStore`, the default backend.
#[derive(Default)]
pub struct InMemoryTranscriptStore {
    max_stored_messages_per_session: usize,
    sessions: Mutex<HashMap<Uuid, Vec<AgentMessage>>>,
    user_sessions: Mutex<HashMap<String, HashSet<Uuid>>>,
}

impl InMemoryTranscriptStore {
    /// `max_stored_messages_per_session` 0 keeps every message
    /// (`MEMORY_MAX_STORED_MESSAGES_PER_SESSION`).
    #[must_use]
    pub fn new(max_stored_messages_per_session: usize) -> Self {
        Self {
            max_stored_messages_per_session,
            ..Self::default()
        }
    }
}

fn poisoned<T>(_: T) -> riverbase_core::RiverbaseError {
    crate::BEM_143.raise()
}

#[async_trait]
impl TranscriptStore for InMemoryTranscriptStore {
    async fn load(&self, session_id: Uuid, range: LoadRange) -> RiverbaseResult<Vec<AgentMessage>> {
        let rows = self
            .sessions
            .lock()
            .map_err(poisoned)?
            .get(&session_id)
            .cloned()
            .unwrap_or_default();
        let rows = rows
            .into_iter()
            .enumerate()
            .map(|(i, m)| with_sequence(m, i))
            .collect();
        Ok(range.apply(rows, sequence_of))
    }

    async fn save(
        &self,
        _agent_id: Uuid,
        session_id: Uuid,
        messages: &[AgentMessage],
        user_id: Option<&str>,
    ) -> RiverbaseResult<()> {
        {
            let mut sessions = self.sessions.lock().map_err(poisoned)?;
            let rows = sessions.entry(session_id).or_default();
            // After the last stored sequence, not the row count: the cap
            // below drops old rows, and summaries cover sequences.
            let start = rows
                .last()
                .map_or(0, |m| usize::try_from(sequence_of(m)).unwrap_or(0) + 1);
            rows.extend(
                messages
                    .iter()
                    .enumerate()
                    .map(|(i, m)| with_sequence(m.clone(), start + i)),
            );
            let cap = self.max_stored_messages_per_session;
            if cap > 0 && rows.len() > cap {
                let excess = rows.len() - cap;
                rows.drain(..excess);
            }
        }
        if let Some(user) = user_id.filter(|u| !u.is_empty()) {
            self.user_sessions
                .lock()
                .map_err(poisoned)?
                .entry(user.to_owned())
                .or_default()
                .insert(session_id);
        }
        Ok(())
    }

    async fn delete(&self, session_id: Uuid) -> RiverbaseResult<()> {
        self.sessions.lock().map_err(poisoned)?.remove(&session_id);
        for ids in self.user_sessions.lock().map_err(poisoned)?.values_mut() {
            ids.remove(&session_id);
        }
        Ok(())
    }

    async fn list_sessions(&self, user_id: &str) -> RiverbaseResult<Vec<String>> {
        Ok(self
            .user_sessions
            .lock()
            .map_err(poisoned)?
            .get(user_id)
            .map(|ids| ids.iter().map(Uuid::to_string).collect())
            .unwrap_or_default())
    }

    async fn sequence_length(&self, session_id: Uuid) -> RiverbaseResult<usize> {
        Ok(self
            .sessions
            .lock()
            .map_err(poisoned)?
            .get(&session_id)
            .map_or(0, Vec::len))
    }
}

/// Messages are `agent_message` rows.
pub struct SqlTranscriptStore {
    store: Arc<dyn BeamStore>,
}

impl SqlTranscriptStore {
    #[must_use]
    pub fn new(store: Arc<dyn BeamStore>) -> Self {
        Self { store }
    }
}

fn row_role(role: AgentMessageRole) -> MessageRole {
    match role {
        AgentMessageRole::Human => MessageRole::Human,
        AgentMessageRole::Ai => MessageRole::Ai,
        AgentMessageRole::System => MessageRole::System,
        AgentMessageRole::Tool => MessageRole::Tool,
    }
}

/// The stored type of a message.
fn row_type(message: &AgentMessage) -> MessageType {
    match message.msg_type {
        AgentMessageType::ToolResult => MessageType::ToolResult,
        AgentMessageType::Action => MessageType::Action,
        AgentMessageType::ActionResult => MessageType::ActionResult,
        AgentMessageType::Step => MessageType::Step,
        AgentMessageType::Message
            if message.tool_call_id.is_some() && message.role == AgentMessageRole::Tool =>
        {
            MessageType::ToolResult
        }
        AgentMessageType::Message => MessageType::Message,
    }
}

/// An `agent_message` row for `message`, in Python's column shape.
#[must_use]
pub fn message_to_row(
    message: &AgentMessage,
    agent_id: Uuid,
    session_id: Uuid,
    sequence: i32,
    org_id: Option<String>,
) -> MessageRow {
    let mut row = MessageRow::new(
        session_id,
        agent_id,
        sequence,
        row_role(message.role),
        row_type(message),
        serde_json::to_value(&message.content).unwrap_or(Value::Array(Vec::new())),
    );
    row.id = message.id;
    row.execution_id = message.execution_id;
    row.interaction_id = message.interaction_id;
    row.run_id = message.run_id;
    row.step_id = message.step_id;
    row.request_id = message.request_id;
    row.phase = match message.phase {
        AgentMessagePhase::Final => MessagePhase::Final,
        AgentMessagePhase::Thinking => MessagePhase::Thinking,
    };
    row.name.clone_from(&message.name);
    row.tool_call_id.clone_from(&message.tool_call_id);
    row.tool_name.clone_from(&message.tool_name);
    row.tool_args = message.tool_args.clone().filter(|v| !is_empty_json(v));
    row.tool_result = message.tool_result.clone().filter(|v| !is_empty_json(v));
    row.tool_calls = Value::Array(message.tool_calls.clone());
    row.metadata = if message.metadata.is_object() {
        message.metadata.clone()
    } else {
        Value::Object(Map::new())
    };
    row.timestamp = message.timestamp;
    row.org_id = org_id;
    row
}

fn is_empty_json(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Object(o) => o.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

/// A message from its `agent_message` row.
#[must_use]
pub fn row_to_message(row: &MessageRow) -> AgentMessage {
    let mut metadata = row.metadata.as_object().cloned().unwrap_or_default();
    metadata.insert("sequence".into(), Value::from(row.sequence));
    let tool_calls = match &row.tool_calls {
        Value::Array(calls) if !calls.is_empty() => calls.clone(),
        _ => row
            .metadata
            .get("tool_calls")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    };
    AgentMessage {
        id: row.id,
        role: match row.role {
            MessageRole::Human => AgentMessageRole::Human,
            MessageRole::Ai => AgentMessageRole::Ai,
            MessageRole::System => AgentMessageRole::System,
            MessageRole::Tool => AgentMessageRole::Tool,
        },
        msg_type: match row.msg_type {
            MessageType::Message => AgentMessageType::Message,
            MessageType::ToolResult => AgentMessageType::ToolResult,
            MessageType::Action => AgentMessageType::Action,
            MessageType::ActionResult => AgentMessageType::ActionResult,
            MessageType::Step => AgentMessageType::Step,
        },
        content: serde_json::from_value::<Vec<ContentPart>>(row.content.clone())
            .unwrap_or_default(),
        name: row.name.clone(),
        timestamp: row.timestamp,
        run_id: row.run_id,
        request_id: row.request_id,
        interaction_id: row.interaction_id,
        execution_id: row.execution_id,
        step_id: row.step_id,
        tool_call_id: row.tool_call_id.clone(),
        tool_name: row.tool_name.clone(),
        tool_args: row.tool_args.clone(),
        tool_result: row.tool_result.clone(),
        tool_calls,
        is_delta: false,
        phase: match row.phase {
            MessagePhase::Final => AgentMessagePhase::Final,
            MessagePhase::Thinking => AgentMessagePhase::Thinking,
        },
        metadata: Value::Object(metadata),
    }
}

#[async_trait]
impl TranscriptStore for SqlTranscriptStore {
    async fn load(&self, session_id: Uuid, range: LoadRange) -> RiverbaseResult<Vec<AgentMessage>> {
        let rows = self.store.list_messages_page(session_id, range).await?;
        Ok(rows.iter().map(row_to_message).collect())
    }

    async fn save(
        &self,
        agent_id: Uuid,
        session_id: Uuid,
        messages: &[AgentMessage],
        _user_id: Option<&str>,
    ) -> RiverbaseResult<()> {
        // The session row already exists (the invoke pipeline ensures it).
        let org = self
            .store
            .get_session(session_id)
            .await?
            .and_then(|s| s.org_id);
        // Sequences are assigned by the store, in one transaction (Python
        // saves a turn in one).
        let rows = messages
            .iter()
            .map(|m| message_to_row(m, agent_id, session_id, 0, org.clone()))
            .collect();
        self.store.append_messages(session_id, rows).await
    }

    async fn delete(&self, session_id: Uuid) -> RiverbaseResult<()> {
        self.store.delete_messages(session_id).await.map(|_| ())
    }

    async fn list_sessions(&self, user_id: &str) -> RiverbaseResult<Vec<String>> {
        if user_id.trim().is_empty() {
            return Ok(Vec::new());
        }
        let ids = self.store.list_user_sessions(user_id).await?;
        Ok(ids.iter().map(Uuid::to_string).collect())
    }

    async fn sequence_length(&self, session_id: Uuid) -> RiverbaseResult<usize> {
        self.store.count_messages(session_id).await
    }
}

/// Sessions [`CachedTranscriptStore`] keeps by default.
pub const DEFAULT_CACHED_SESSIONS: usize = 1024;

/// One cached page: valid while the canonical sequence length is `version`.
struct CachedPage {
    version: usize,
    /// The page's limit (`None`: the whole session).
    limit: Option<usize>,
    rows: Vec<AgentMessage>,
}

#[derive(Default)]
struct PageCache {
    pages: HashMap<Uuid, CachedPage>,
    /// Least recently used first.
    order: std::collections::VecDeque<Uuid>,
}

impl PageCache {
    fn touch(&mut self, session_id: Uuid) {
        self.order.retain(|s| *s != session_id);
        self.order.push_back(session_id);
    }

    fn remove(&mut self, session_id: Uuid) {
        self.pages.remove(&session_id);
        self.order.retain(|s| *s != session_id);
    }
}

/// Python `CachedTranscriptStore` with Redis replaced by an in-process cache
/// (D4): the canonical store is the source of truth, and a cached page is
/// served only while the session's sequence length is unchanged and the page
/// covers the requested limit. The cache holds at most `capacity` sessions,
/// evicting the least recently used.
pub struct CachedTranscriptStore {
    canonical: Arc<dyn TranscriptStore>,
    capacity: usize,
    cache: Mutex<PageCache>,
}

impl CachedTranscriptStore {
    #[must_use]
    pub fn new(canonical: Arc<dyn TranscriptStore>) -> Self {
        Self::with_capacity(canonical, DEFAULT_CACHED_SESSIONS)
    }

    #[must_use]
    pub fn with_capacity(canonical: Arc<dyn TranscriptStore>, capacity: usize) -> Self {
        Self {
            canonical,
            capacity: capacity.max(1),
            cache: Mutex::new(PageCache::default()),
        }
    }

    /// The cached rows for a plain load, when still valid for `limit`.
    fn cached(
        &self,
        session_id: Uuid,
        version: usize,
        limit: Option<usize>,
    ) -> RiverbaseResult<Option<Vec<AgentMessage>>> {
        let mut cache = self.cache.lock().map_err(poisoned)?;
        let Some(page) = cache.pages.get(&session_id) else {
            return Ok(None);
        };
        let covers = match (page.limit, limit) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(cached), Some(wanted)) => cached >= wanted || page.rows.len() < cached,
        };
        if page.version != version || !covers || page.rows.is_empty() {
            return Ok(None);
        }
        let rows = match limit {
            Some(l) if page.rows.len() > l => page.rows[page.rows.len() - l..].to_vec(),
            _ => page.rows.clone(),
        };
        cache.touch(session_id);
        Ok(Some(rows))
    }

    /// Replace the session's page in one step (no interleaving with another load).
    fn store(&self, session_id: Uuid, page: CachedPage) -> RiverbaseResult<()> {
        let mut cache = self.cache.lock().map_err(poisoned)?;
        cache.pages.insert(session_id, page);
        cache.touch(session_id);
        while cache.order.len() > self.capacity {
            if let Some(oldest) = cache.order.pop_front() {
                cache.pages.remove(&oldest);
            }
        }
        Ok(())
    }

    fn forget(&self, session_id: Uuid) -> RiverbaseResult<()> {
        self.cache.lock().map_err(poisoned)?.remove(session_id);
        Ok(())
    }
}

#[async_trait]
impl TranscriptStore for CachedTranscriptStore {
    async fn load(&self, session_id: Uuid, range: LoadRange) -> RiverbaseResult<Vec<AgentMessage>> {
        let version = self.canonical.sequence_length(session_id).await?;
        let plain = range.before.is_none() && range.after.is_none() && range.through.is_none();
        let limit = range.limit.filter(|l| *l > 0);
        if plain {
            if let Some(rows) = self.cached(session_id, version, limit)? {
                return Ok(rows);
            }
        }
        let rows = self.canonical.load(session_id, range).await?;
        if plain && !rows.is_empty() {
            self.store(
                session_id,
                CachedPage {
                    version,
                    limit,
                    rows: rows.clone(),
                },
            )?;
        }
        Ok(rows)
    }

    async fn save(
        &self,
        agent_id: Uuid,
        session_id: Uuid,
        messages: &[AgentMessage],
        user_id: Option<&str>,
    ) -> RiverbaseResult<()> {
        self.canonical
            .save(agent_id, session_id, messages, user_id)
            .await?;
        self.forget(session_id)
    }

    async fn delete(&self, session_id: Uuid) -> RiverbaseResult<()> {
        self.canonical.delete(session_id).await?;
        self.forget(session_id)
    }

    async fn list_sessions(&self, user_id: &str) -> RiverbaseResult<Vec<String>> {
        self.canonical.list_sessions(user_id).await
    }

    async fn sequence_length(&self, session_id: Uuid) -> RiverbaseResult<usize> {
        self.canonical.sequence_length(session_id).await
    }
}

/// `MEMORY_BACKEND` keys (Python's, without `redis`, D4).
pub const MEMORY_BACKENDS: &[&str] = &["inmemory", "in_memory", "memory", "sql", "sql_cached"];

/// The transcript store for `backend` (empty means `in_memory`, as in Python).
/// `sql` and `sql_cached` write `agent_message` rows through `store`.
///
/// # Errors
/// `BEM-144` for an unknown key.
pub fn build_transcript_store(
    backend: &str,
    store: &Arc<dyn BeamStore>,
    max_stored_messages_per_session: usize,
) -> RiverbaseResult<Arc<dyn TranscriptStore>> {
    match backend.trim() {
        "" | "inmemory" | "in_memory" | "memory" => Ok(Arc::new(InMemoryTranscriptStore::new(
            max_stored_messages_per_session,
        ))),
        "sql" => Ok(Arc::new(SqlTranscriptStore::new(store.clone()))),
        "sql_cached" => Ok(Arc::new(CachedTranscriptStore::new(Arc::new(
            SqlTranscriptStore::new(store.clone()),
        )))),
        other => Err(crate::BEM_144.with_data(format!(
            "unknown MEMORY_BACKEND '{other}'; available: {}",
            MEMORY_BACKENDS.join(", ")
        ))),
    }
}

/// The configured backend plus durable incremental
/// summaries of everything older than the most recent [`KEEP_RECENT`] messages.
pub struct TranscriptManager {
    backend: Arc<dyn TranscriptStore>,
    page: usize,
    summaries: Arc<dyn SummaryStore>,
    keep_recent: usize,
    count: Arc<CountTokens>,
}

/// The conservative `ceil(chars / 4)` bound, for a manager built without the
/// model's counter.
fn approx_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(4)
}

impl TranscriptManager {
    #[must_use]
    pub fn new(backend: Arc<dyn TranscriptStore>) -> Self {
        Self {
            backend,
            page: DEFAULT_PAGE,
            summaries: Arc::new(SummaryBook::default()),
            keep_recent: KEEP_RECENT,
            count: Arc::new(approx_tokens),
        }
    }

    /// Count summary tokens with `count` (the model's [`CountTokens`]).
    #[must_use]
    pub fn with_counter(mut self, count: Arc<CountTokens>) -> Self {
        self.count = count;
        self
    }

    /// Keep summaries in `summaries` (plan 023 B3: Postgres on a Postgres
    /// beam, so they survive restarts and every worker shares them).
    #[must_use]
    pub fn with_summaries(mut self, summaries: Arc<dyn SummaryStore>) -> Self {
        self.summaries = summaries;
        self
    }

    #[must_use]
    pub fn backend(&self) -> &Arc<dyn TranscriptStore> {
        &self.backend
    }

    #[must_use]
    pub fn summaries(&self) -> &Arc<dyn SummaryStore> {
        &self.summaries
    }

    /// Recent messages; when the page is longer than [`KEEP_RECENT`], the older
    /// part is replaced by a (durable, incremental) summary system message.
    ///
    /// # Errors
    /// Store errors.
    pub async fn load_history(
        &self,
        session_id: Uuid,
        limit: Option<usize>,
    ) -> RiverbaseResult<Vec<AgentMessage>> {
        let page = limit.unwrap_or(self.page);
        let messages = self
            .backend
            .load(
                session_id,
                LoadRange {
                    limit: Some(page),
                    ..LoadRange::default()
                },
            )
            .await?;
        Ok(self.merge_summary(session_id, messages).await)
    }

    /// Bring the session's summary up to date after a turn (Python
    /// `TranscriptManager.compact`): everything older than the recent
    /// messages is covered.
    ///
    /// # Errors
    /// Store errors.
    pub async fn compact(&self, session_id: Uuid) -> RiverbaseResult<()> {
        self.load_history(session_id, None).await.map(|_| ())
    }

    /// # Errors
    /// Store errors.
    pub async fn save_turn(
        &self,
        agent_id: Uuid,
        session_id: Uuid,
        messages: &[AgentMessage],
        user_id: Option<&str>,
    ) -> RiverbaseResult<()> {
        // Media parts are saved as they are: no file service moves them out.
        self.backend
            .save(agent_id, session_id, messages, user_id)
            .await
    }

    /// An edit from `sequence` invalidates the summary covering it.
    ///
    /// # Errors
    /// Store errors.
    pub async fn edit_from(&self, session_id: Uuid, sequence: usize) -> RiverbaseResult<()> {
        self.summaries
            .invalidate(&session_id.to_string(), Some(sequence))
            .await
    }

    /// # Errors
    /// Store errors.
    pub async fn delete_session(&self, session_id: Uuid) -> RiverbaseResult<()> {
        self.backend.delete(session_id).await?;
        self.summaries
            .invalidate(&session_id.to_string(), None)
            .await
    }

    /// The page with its older part replaced by the session's summary. The
    /// summary covers transcript sequences (plan 023 B3: before, a position
    /// in the page, which slid once a session passed [`DEFAULT_PAGE`]).
    /// Every history read comes here, so a summary store that fails is
    /// logged and the read goes on: unable to read the stored summary, it
    /// summarizes the page afresh; unable to write one, it uses the summary
    /// it built.
    async fn merge_summary(
        &self,
        session_id: Uuid,
        messages: Vec<AgentMessage>,
    ) -> Vec<AgentMessage> {
        if messages.len() <= self.keep_recent {
            return messages;
        }
        let key = session_id.to_string();
        let split = messages.len() - self.keep_recent;
        let (uncovered, recent) = messages.split_at(split);
        let system = |text: &str| {
            let mut message = AgentMessage::text(AgentMessageRole::System, text);
            // Marks the merged summary: it quotes user turns, so the context
            // pipeline treats it as untrusted (Python does not mark it).
            message.metadata = serde_json::json!({ SUMMARY_MARKER: true });
            message
        };
        let count: &CountTokens = &*self.count;
        let seq = |m: &AgentMessage| usize::try_from(sequence_of(m)).unwrap_or(0);
        let last = uncovered.last().map_or(0, seq);
        let (existing, readable) = match self.summaries.get(&key).await {
            Ok(existing) => (existing, true),
            Err(e) => {
                tracing::warn!(session_id = %key, error = %e, "session summary unreadable");
                (None, false)
            }
        };
        let summary = if let Some(existing) = existing.filter(|e| !e.invalidated) {
            let delta: Vec<AgentMessage> = uncovered
                .iter()
                .filter(|m| seq(m) > existing.coverage_end)
                .cloned()
                .collect();
            if delta.is_empty() && !existing.source_hash.is_empty() {
                let mut out = vec![system(&existing.text)];
                out.extend_from_slice(recent);
                return out;
            }
            let summary = build_structured_summary(
                &delta,
                &key,
                existing.coverage_start,
                last,
                Some(&existing),
                count,
            );
            // A lost race leaves the other writer's summary; this page
            // still reads the one it built.
            self.save_summary(&summary, Some(existing.coverage_end))
                .await;
            summary
        } else {
            let first = uncovered.first().map_or(0, seq);
            let summary = build_structured_summary(uncovered, &key, first, last, None, count);
            if readable {
                self.save_summary(&summary, None).await;
            }
            summary
        };
        let mut out = vec![system(&summary.text)];
        out.extend_from_slice(recent);
        out
    }

    /// Store `summary` if the stored one still ends at `expected_end`; a
    /// failure is logged (see [`Self::merge_summary`]).
    async fn save_summary(&self, summary: &SessionSummary, expected_end: Option<usize>) {
        if let Err(e) = self.summaries.cas_put(summary.clone(), expected_end).await {
            tracing::warn!(
                session_id = %summary.session_id,
                error = %e,
                "session summary not saved"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_store::{ExecutionStore, InMemoryStore, SessionRow};

    fn msg(role: AgentMessageRole, text: &str) -> AgentMessage {
        AgentMessage::text(role, text)
    }

    fn turns(n: usize) -> Vec<AgentMessage> {
        (0..n)
            .map(|i| {
                let role = if i % 2 == 0 {
                    AgentMessageRole::Human
                } else {
                    AgentMessageRole::Ai
                };
                msg(role, &format!("m{i}"))
            })
            .collect()
    }

    fn texts(messages: &[AgentMessage]) -> Vec<String> {
        messages.iter().map(AgentMessage::text_content).collect()
    }

    #[tokio::test]
    async fn in_memory_sequences_go_on_past_the_cap() {
        let store = InMemoryTranscriptStore::new(2);
        let session = Uuid::new_v4();
        for _ in 0..4 {
            store
                .save(Uuid::nil(), session, &turns(1), None)
                .await
                .unwrap();
        }
        let all = store.load(session, LoadRange::default()).await.unwrap();
        assert_eq!(
            all.iter().map(sequence_of).collect::<Vec<_>>(),
            vec![2, 3],
            "each save numbers after the last, though the cap dropped older rows"
        );
    }

    #[tokio::test]
    async fn in_memory_sequences_ranges_and_cap() {
        let store = InMemoryTranscriptStore::new(4);
        let session = Uuid::new_v4();
        store
            .save(Uuid::nil(), session, &turns(3), Some("u1"))
            .await
            .unwrap();
        store
            .save(Uuid::nil(), session, &turns(3), None)
            .await
            .unwrap();
        // Capped to the latest 4; sequences stay those of the save.
        let all = store.load(session, LoadRange::default()).await.unwrap();
        assert_eq!(
            all.iter().map(sequence_of).collect::<Vec<_>>(),
            vec![2, 3, 4, 5]
        );
        let page = LoadRange {
            after: Some(2),
            through: Some(4),
            ..LoadRange::default()
        };
        assert_eq!(
            store
                .load(session, page)
                .await
                .unwrap()
                .iter()
                .map(sequence_of)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
        let latest = LoadRange {
            limit: Some(1),
            ..LoadRange::default()
        };
        assert_eq!(
            sequence_of(&store.load(session, latest).await.unwrap()[0]),
            5
        );
        assert_eq!(
            store.list_sessions("u1").await.unwrap(),
            vec![session.to_string()]
        );
        store.delete(session).await.unwrap();
        assert_eq!(store.sequence_length(session).await.unwrap(), 0);
        assert!(store.list_sessions("u1").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn sql_store_roundtrips_rows() {
        let beam: Arc<InMemoryStore> = Arc::new(InMemoryStore::new());
        let session = Uuid::new_v4();
        let agent = Uuid::new_v4();
        beam.ensure_session(&SessionRow {
            id: session,
            agent_id: agent,
            user_id: Some("u1".into()),
            profile_id: None,
            org_id: Some("org-1".into()),
            name: None,
            last_active_at: None,
        })
        .await
        .unwrap();
        let store = SqlTranscriptStore::new(beam.clone());
        let mut tool = msg(AgentMessageRole::Tool, "42");
        tool.tool_call_id = Some("c1".into());
        let mut messages = turns(2);
        messages.push(tool);
        store
            .save(agent, session, &messages, Some("u1"))
            .await
            .unwrap();

        let rows = beam.list_messages(session).await.unwrap();
        assert_eq!(
            rows.iter().map(|r| r.sequence).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(rows.iter().all(|r| r.org_id.as_deref() == Some("org-1")));
        assert_eq!(rows[2].msg_type, MessageType::ToolResult);

        let loaded = store
            .load(
                session,
                LoadRange {
                    limit: Some(2),
                    ..LoadRange::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(texts(&loaded), vec!["m1", "42"]);
        assert_eq!(loaded[1].tool_call_id.as_deref(), Some("c1"));
        assert_eq!(sequence_of(&loaded[1]), 2);
        assert_eq!(store.sequence_length(session).await.unwrap(), 3);
        assert_eq!(
            store.list_sessions("u1").await.unwrap(),
            vec![session.to_string()]
        );
        store.delete(session).await.unwrap();
        assert_eq!(store.sequence_length(session).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn cached_store_rereads_after_canonical_changes() {
        let canonical: Arc<dyn TranscriptStore> = Arc::new(InMemoryTranscriptStore::new(0));
        let cached = CachedTranscriptStore::new(canonical.clone());
        let session = Uuid::new_v4();
        cached
            .save(Uuid::nil(), session, &turns(2), None)
            .await
            .unwrap();
        assert_eq!(
            cached
                .load(session, LoadRange::default())
                .await
                .unwrap()
                .len(),
            2
        );
        // A write that bypasses the cache changes the canonical length.
        canonical
            .save(Uuid::nil(), session, &turns(1), None)
            .await
            .unwrap();
        assert_eq!(
            cached
                .load(session, LoadRange::default())
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn cached_store_never_serves_a_shorter_page_than_asked() {
        let canonical: Arc<dyn TranscriptStore> = Arc::new(InMemoryTranscriptStore::new(0));
        let cached = CachedTranscriptStore::with_capacity(canonical.clone(), 1);
        let session = Uuid::new_v4();
        canonical
            .save(Uuid::nil(), session, &turns(5), None)
            .await
            .unwrap();
        let small = LoadRange {
            limit: Some(2),
            ..LoadRange::default()
        };
        assert_eq!(cached.load(session, small).await.unwrap().len(), 2);
        assert_eq!(
            cached
                .load(session, LoadRange::default())
                .await
                .unwrap()
                .len(),
            5
        );
        let big = LoadRange {
            limit: Some(4),
            ..LoadRange::default()
        };
        assert_eq!(
            texts(&cached.load(session, big).await.unwrap()),
            ["m1", "m2", "m3", "m4"]
        );
        // Capacity 1: another session evicts this one, which reloads correctly.
        let other = Uuid::new_v4();
        canonical
            .save(Uuid::nil(), other, &turns(1), None)
            .await
            .unwrap();
        cached.load(other, LoadRange::default()).await.unwrap();
        assert_eq!(
            cached
                .load(session, LoadRange::default())
                .await
                .unwrap()
                .len(),
            5
        );
    }

    #[test]
    fn unknown_backend_is_rejected() {
        let beam: Arc<dyn BeamStore> = Arc::new(InMemoryStore::new());
        assert!(build_transcript_store("", &beam, 0).is_ok());
        assert!(build_transcript_store("memory", &beam, 0).is_ok());
        assert!(build_transcript_store("sql_cached", &beam, 0).is_ok());
        assert!(build_transcript_store("redis", &beam, 0).is_err());
    }

    #[tokio::test]
    async fn manager_summarizes_beyond_keep_recent_incrementally() {
        let manager = TranscriptManager::new(Arc::new(InMemoryTranscriptStore::new(0)));
        let session = Uuid::new_v4();
        manager
            .save_turn(Uuid::nil(), session, &turns(10), None)
            .await
            .unwrap();
        assert_eq!(manager.load_history(session, None).await.unwrap().len(), 10);

        manager
            .save_turn(Uuid::nil(), session, &turns(2), None)
            .await
            .unwrap();
        let history = manager.load_history(session, None).await.unwrap();
        assert_eq!(history.len(), 11);
        assert_eq!(history[0].role, AgentMessageRole::System);
        assert_eq!(
            history[0].text_content(),
            "Conversation summary (untrusted):\nFacts: m0\nCommitments: m1"
        );
        let summary = |manager: &TranscriptManager| {
            let summaries = manager.summaries().clone();
            let key = session.to_string();
            async move { summaries.get(&key).await.unwrap() }
        };
        let first = summary(&manager).await.unwrap();
        assert_eq!(first.coverage_end, 1);

        // Unchanged coverage reuses the stored summary.
        let again = manager.load_history(session, None).await.unwrap();
        assert_eq!(again[0].text_content(), history[0].text_content());

        manager
            .save_turn(Uuid::nil(), session, &turns(1), None)
            .await
            .unwrap();
        manager.load_history(session, None).await.unwrap();
        let next = summary(&manager).await.unwrap();
        assert_eq!((next.coverage_start, next.coverage_end), (0, 2));
        assert_eq!(next.facts, vec!["m0", "m2"]);

        manager.edit_from(session, 1).await.unwrap();
        assert!(summary(&manager).await.unwrap().invalidated);
        manager.delete_session(session).await.unwrap();
        assert!(summary(&manager).await.is_none());
        assert!(manager
            .load_history(session, None)
            .await
            .unwrap()
            .is_empty());
    }

    /// Plan 023 B3 (regression): a summary covers transcript sequences. A
    /// session past the page used to store a position in the page, which
    /// names other messages once the page slides.
    #[tokio::test]
    async fn a_long_sessions_summary_covers_sequences() {
        let manager = TranscriptManager::new(Arc::new(InMemoryTranscriptStore::new(0)));
        let session = Uuid::new_v4();
        manager
            .save_turn(Uuid::nil(), session, &turns(DEFAULT_PAGE + 50), None)
            .await
            .unwrap();
        manager.compact(session).await.unwrap();
        let summary = manager
            .summaries()
            .get(&session.to_string())
            .await
            .unwrap()
            .expect("a summary");
        // The page is the last 200 (sequences 50..=249); all but the recent
        // ones are covered, through sequence 249 - KEEP_RECENT.
        let last_covered = DEFAULT_PAGE + 50 - 1 - KEEP_RECENT;
        assert_eq!(summary.coverage_end, last_covered);
        assert_eq!(summary.coverage_start, 50);
    }
}
