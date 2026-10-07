-- riverbeam SQL schema (`ref_beam`).
--
-- Execution, catalog, and knowledge tables for `PgBeamStore`. Vector columns use
-- pgvector; embedding dimension matches BEAM_KNOWLEDGE_EMBEDDING_DIM (1536).

CREATE SCHEMA IF NOT EXISTS ref_beam;
CREATE EXTENSION IF NOT EXISTS vector;

-- ---------------------------------------------------------------------------
-- Catalog
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.model (
    _id         UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    provider    TEXT,
    status      TEXT NOT NULL DEFAULT 'active',
    metadata_json JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ref_beam.agent (
    _id         UUID PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    status      TEXT NOT NULL DEFAULT 'active',
    metadata_json JSONB NOT NULL DEFAULT '{}'
);

-- ---------------------------------------------------------------------------
-- Execution hierarchy: Session -> Interaction -> Run -> Step
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.agent_session (
    _id            UUID PRIMARY KEY,
    agent_id       UUID NOT NULL REFERENCES ref_beam.agent(_id),
    user_id        UUID,
    profile_id     UUID,
    name           TEXT,
    last_active_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_agent_session_agent_id ON ref_beam.agent_session(agent_id);

CREATE TABLE IF NOT EXISTS ref_beam.agent_interaction (
    _id         UUID PRIMARY KEY,
    session_id  UUID NOT NULL REFERENCES ref_beam.agent_session(_id),
    agent_id    UUID NOT NULL REFERENCES ref_beam.agent(_id),
    agent_name  TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'in_progress',
    started_at  TIMESTAMPTZ NOT NULL,
    ended_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_agent_interaction_session ON ref_beam.agent_interaction(session_id);

CREATE TABLE IF NOT EXISTS ref_beam.agent_run (
    _id            UUID PRIMARY KEY,
    session_id     UUID NOT NULL REFERENCES ref_beam.agent_session(_id),
    interaction_id UUID NOT NULL REFERENCES ref_beam.agent_interaction(_id),
    agent_id       UUID NOT NULL REFERENCES ref_beam.agent(_id),
    agent_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'running',
    started_at     TIMESTAMPTZ NOT NULL,
    ended_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_agent_run_session ON ref_beam.agent_run(session_id);

CREATE TABLE IF NOT EXISTS ref_beam.agent_step (
    _id            UUID PRIMARY KEY,
    session_id     UUID NOT NULL REFERENCES ref_beam.agent_session(_id),
    interaction_id UUID NOT NULL REFERENCES ref_beam.agent_interaction(_id),
    run_id         UUID NOT NULL REFERENCES ref_beam.agent_run(_id),
    agent_id       UUID NOT NULL REFERENCES ref_beam.agent(_id),
    status         TEXT NOT NULL DEFAULT 'pending',
    lc_step_id     TEXT,
    name           TEXT NOT NULL,
    input          JSONB NOT NULL,
    result         JSONB
);

-- ---------------------------------------------------------------------------
-- Messages & Actions (HITL)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.agent_message (
    _id            UUID PRIMARY KEY,
    sequence       INTEGER NOT NULL,
    role           TEXT NOT NULL,
    type           TEXT NOT NULL,
    content        JSONB NOT NULL,
    name           TEXT,
    tool_call_id   TEXT,
    tool_name      TEXT,
    tool_args      JSONB,
    tool_result    JSONB,
    tool_calls     JSONB NOT NULL DEFAULT '[]',
    phase          TEXT NOT NULL DEFAULT 'final',
    metadata_json  JSONB NOT NULL DEFAULT '{}',
    timestamp      TIMESTAMPTZ NOT NULL,
    session_id     UUID NOT NULL REFERENCES ref_beam.agent_session(_id),
    interaction_id UUID REFERENCES ref_beam.agent_interaction(_id),
    run_id         UUID REFERENCES ref_beam.agent_run(_id),
    agent_id       UUID NOT NULL REFERENCES ref_beam.agent(_id),
    step_id        UUID REFERENCES ref_beam.agent_step(_id),
    request_id     UUID
);
CREATE INDEX IF NOT EXISTS ix_agent_message_session ON ref_beam.agent_message(session_id);

CREATE TABLE IF NOT EXISTS ref_beam.agent_action (
    _id            UUID PRIMARY KEY,
    session_id     UUID NOT NULL REFERENCES ref_beam.agent_session(_id),
    interaction_id UUID NOT NULL REFERENCES ref_beam.agent_interaction(_id),
    run_id         UUID NOT NULL REFERENCES ref_beam.agent_run(_id),
    step_id        UUID REFERENCES ref_beam.agent_step(_id),
    message_id     UUID,
    agent_id       UUID NOT NULL REFERENCES ref_beam.agent(_id),
    type           TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    lc_action_id   TEXT,
    payload        JSONB NOT NULL DEFAULT '{}',
    result         JSONB,
    resolved_at    TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_agent_action_run ON ref_beam.agent_action(run_id);

-- ---------------------------------------------------------------------------
-- Knowledge & Memory (pgvector)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.agent_knowledge_document (
    _id           UUID PRIMARY KEY,
    scope         TEXT,
    agent_id      UUID REFERENCES ref_beam.agent(_id),
    user_id       UUID,
    title         TEXT NOT NULL,
    source        TEXT,
    type          TEXT NOT NULL,
    body          TEXT,
    media_id      UUID,
    status        TEXT NOT NULL DEFAULT 'pending',
    embedder_name TEXT,
    chunk_count   INTEGER NOT NULL DEFAULT 0,
    content_hash  VARCHAR(64),
    metadata_json JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_knowledge_chunk (
    _id          UUID PRIMARY KEY,
    document_id  UUID NOT NULL REFERENCES ref_beam.agent_knowledge_document(_id),
    scope        TEXT,
    agent_id     UUID REFERENCES ref_beam.agent(_id),
    user_id      UUID,
    source       TEXT,
    chunk_index  INTEGER NOT NULL DEFAULT 0,
    token_count  INTEGER,
    content_hash VARCHAR(64),
    content      TEXT NOT NULL,
    embedding    vector(1536) NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT uq_agent_knowledge_chunk_doc_idx UNIQUE (document_id, chunk_index)
);
CREATE INDEX IF NOT EXISTS ix_agent_knowledge_chunk_embedding_hnsw
    ON ref_beam.agent_knowledge_chunk USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

CREATE TABLE IF NOT EXISTS ref_beam.agent_memory (
    _id            UUID PRIMARY KEY,
    user_id        UUID NOT NULL,
    agent_id       UUID REFERENCES ref_beam.agent(_id),
    type           TEXT NOT NULL DEFAULT 'semantic',
    content        TEXT NOT NULL,
    embedding      vector(1536),
    embedder_name  TEXT,
    metadata_json  JSONB NOT NULL DEFAULT '{}',
    importance     INTEGER NOT NULL DEFAULT 1,
    last_accessed_at TIMESTAMPTZ,
    expires_at     TIMESTAMPTZ,
    version        INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS ix_agent_memory_embedding_hnsw
    ON ref_beam.agent_memory USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64)
    WHERE embedding IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Usage / Quota
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.agent_usage_plan (
    _id          UUID PRIMARY KEY,
    name         TEXT NOT NULL,
    overage_policy TEXT NOT NULL DEFAULT 'hard_block',
    status       TEXT NOT NULL DEFAULT 'active',
    limit_max_requests_per_day  INTEGER,
    limit_max_tokens_per_day    INTEGER,
    limit_max_tokens_per_request INTEGER,
    limit_allowed_agents     TEXT[],
    limit_allowed_modalities TEXT[],
    limit_allowed_models     TEXT[]
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_usage_profile (
    _id      UUID PRIMARY KEY,
    user_id  UUID NOT NULL,
    plan_id  UUID NOT NULL REFERENCES ref_beam.agent_usage_plan(_id),
    plan_key TEXT NOT NULL,
    active   BOOLEAN NOT NULL DEFAULT true,
    CONSTRAINT uq_agent_usage_profile UNIQUE (user_id, plan_id)
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_usage_counter (
    _id     UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    period  TEXT NOT NULL,
    field   TEXT NOT NULL,
    value   INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT uq_agent_usage_counter UNIQUE (user_id, period, field)
);

CREATE TABLE IF NOT EXISTS ref_beam.agent_usage_event (
    _id        UUID PRIMARY KEY,
    user_id    UUID NOT NULL,
    tokens     INTEGER NOT NULL DEFAULT 0,
    requests   INTEGER NOT NULL DEFAULT 1,
    model_used TEXT,
    modalities JSONB NOT NULL DEFAULT '[]'
);

-- ---------------------------------------------------------------------------
-- Audit
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.agent_event (
    _id            UUID PRIMARY KEY,
    action         TEXT NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL,
    agent_name     TEXT,
    user_id        UUID,
    session_id     UUID,
    run_id         UUID,
    model_used     TEXT,
    modalities_used JSONB NOT NULL DEFAULT '[]',
    token_usage    JSONB,
    latency_ms     DOUBLE PRECISION,
    status         TEXT NOT NULL DEFAULT 'ok',
    error          TEXT,
    input_preview  TEXT,
    output_preview TEXT,
    metadata_json  JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_agent_event_action ON ref_beam.agent_event(action);

-- ---------------------------------------------------------------------------
-- Security
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_beam.agent_security_decision (
    _id            UUID PRIMARY KEY,
    run_id         UUID,
    session_id     UUID,
    interaction_id UUID,
    stage          TEXT NOT NULL,
    action         TEXT NOT NULL,
    severity       TEXT,
    score          DOUBLE PRECISION,
    narration      TEXT,
    user_message   TEXT,
    violation_type TEXT,
    payload_json   JSONB NOT NULL DEFAULT '{}',
    matched_rules  JSONB NOT NULL DEFAULT '[]',
    metadata_json  JSONB NOT NULL DEFAULT '{}',
    recorded_at    TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_agent_security_decision_stage ON ref_beam.agent_security_decision(stage);

CREATE TABLE IF NOT EXISTS ref_beam.agent_security_rule_narration (
    _id         UUID PRIMARY KEY,
    decision_id UUID NOT NULL,
    rule        TEXT NOT NULL,
    severity    TEXT,
    message     TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_agent_security_rule_narration_decision ON ref_beam.agent_security_rule_narration(decision_id);
