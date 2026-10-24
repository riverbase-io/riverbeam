CREATE SCHEMA ref_beam;

CREATE TYPE ref_beam.actionstatus AS ENUM (
    'PENDING',
    'RESOLVED',
    'EXPIRED'
);

CREATE TYPE ref_beam.actiontype AS ENUM (
    'CONFIRMATION',
    'FORM',
    'APPROVAL',
    'QUESTION',
    'CHOICE',
    'TOOL_APPROVAL',
    'CLIENT_TOOL'
);

CREATE TYPE ref_beam.agentmessagephase AS ENUM (
    'FINAL',
    'THINKING'
);

CREATE TYPE ref_beam.agentmessagerole AS ENUM (
    'HUMAN',
    'AI',
    'SYSTEM',
    'TOOL'
);

CREATE TYPE ref_beam.agentmessagetype AS ENUM (
    'MESSAGE',
    'TOOL_RESULT',
    'ACTION',
    'ACTION_RESULT',
    'STEP'
);

CREATE TYPE ref_beam.agentstatus AS ENUM (
    'DRAFT',
    'VALIDATING',
    'ACTIVE',
    'INACTIVE',
    'DEPRECATED',
    'RETIRED',
    'draft',
    'validating',
    'deprecated',
    'retired'
);

CREATE TYPE ref_beam.auditaction AS ENUM (
    'REQUEST_RECEIVED',
    'AGENT_INVOKED',
    'AGENT_COMPLETED',
    'AGENT_FAILED',
    'AGENT_INTERRUPTED',
    'AGENT_RESUMED',
    'TOOL_CALLED',
    'MODEL_ROUTED',
    'QUOTA_CHECKED',
    'QUOTA_EXCEEDED',
    'SECURITY_EVENT',
    'AGENT_PUBLISHED',
    'POLICY_CHANGED',
    'CONTEXT_RESOURCE_ACCESSED',
    'MEMORY_WRITTEN',
    'MEMORY_DELETED',
    'TOOL_APPROVAL',
    'CREDENTIAL_USED',
    'OPERATOR_RECOVERY'
);

CREATE TYPE ref_beam.compatibilitypolicy AS ENUM (
    'STRICT',
    'COMPATIBLE',
    'MIGRATABLE'
);

CREATE TYPE ref_beam.executionstatus AS ENUM (
    'CREATED',
    'QUEUED',
    'RUNNING',
    'WAITING_FOR_INPUT',
    'RETRYING',
    'COMPLETED',
    'FAILED',
    'CANCELLED',
    'TIMED_OUT'
);

CREATE TYPE ref_beam.interactionstatus AS ENUM (
    'IN_PROGRESS',
    'COMPLETED',
    'FAILED'
);

CREATE TYPE ref_beam.knowledgedocumentstatus AS ENUM (
    'PENDING',
    'INDEXING',
    'INDEXED',
    'FAILED'
);

CREATE TYPE ref_beam.knowledgescope AS ENUM (
    'GLOBAL',
    'ORG',
    'AGENT',
    'USER'
);

CREATE TYPE ref_beam.memorytype AS ENUM (
    'SEMANTIC',
    'PREFERENCE',
    'EPISODIC'
);

CREATE TYPE ref_beam.modality AS ENUM (
    'TEXT',
    'IMAGE',
    'AUDIO',
    'VIDEO'
);

CREATE TYPE ref_beam.modelstatus AS ENUM (
    'ACTIVE',
    'INACTIVE',
    'DEPRECATED'
);

CREATE TYPE ref_beam.revisionstatus AS ENUM (
    'DRAFT',
    'VALIDATING',
    'ACTIVE',
    'DEPRECATED',
    'RETIRED'
);

CREATE TYPE ref_beam.runstatus AS ENUM (
    'RUNNING',
    'INTERRUPTED',
    'COMPLETED',
    'FAILED'
);

CREATE TYPE ref_beam.stepstatus AS ENUM (
    'PENDING',
    'RUNNING',
    'COMPLETED',
    'FAILED',
    'INTERRUPTED',
    'RESUMED'
);

CREATE TYPE ref_beam.suggestiontype AS ENUM (
    'ACTION',
    'QUESTION',
    'INSIGHT'
);

CREATE TABLE ref_beam.agent (
    name character varying NOT NULL,
    description character varying,
    supported_modalities jsonb NOT NULL,
    output_modalities jsonb NOT NULL,
    tags jsonb,
    version character varying,
    status ref_beam.agentstatus NOT NULL,
    active_revision_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((("left"((COALESCE(name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(description, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(version, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_action (
    session_id uuid NOT NULL,
    execution_id uuid,
    interaction_id uuid NOT NULL,
    run_id uuid NOT NULL,
    step_id uuid,
    message_id uuid,
    agent_id uuid NOT NULL,
    type ref_beam.actiontype NOT NULL,
    status ref_beam.actionstatus NOT NULL,
    lc_action_id character varying,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    result jsonb,
    resolved_at timestamp with time zone,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    tool_name character varying,
    expires_at timestamp with time zone,
    sweep_after timestamp with time zone,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"((COALESCE(lc_action_id, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(tool_name, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_artifact (
    execution_id uuid NOT NULL,
    session_id uuid,
    media_id uuid NOT NULL,
    name character varying,
    created_at timestamp with time zone NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.agent_context_manifest (
    operation_id character varying NOT NULL,
    execution_id uuid,
    run_id uuid,
    operation_kind character varying DEFAULT 'invoke'::character varying NOT NULL,
    revision_id character varying,
    policy_revision character varying,
    serializer_revision character varying,
    tokenizer_revision character varying,
    input_tokens integer DEFAULT 0 NOT NULL,
    output_tokens integer DEFAULT 0 NOT NULL,
    reserve_tokens integer DEFAULT 0 NOT NULL,
    actual_prompt_tokens integer,
    estimate_drift double precision,
    decisions jsonb DEFAULT '[]'::jsonb NOT NULL,
    view jsonb DEFAULT '{}'::jsonb NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    user_id uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((((((("left"((COALESCE(operation_id, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(operation_kind, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(revision_id, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(policy_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(serializer_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(tokenizer_revision, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_cost_event (
    execution_id uuid,
    agent_name character varying,
    model character varying NOT NULL,
    provider character varying,
    prompt_tokens integer DEFAULT 0 NOT NULL,
    completion_tokens integer DEFAULT 0 NOT NULL,
    usd double precision DEFAULT '0'::double precision NOT NULL,
    recorded_at timestamp with time zone NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED
);

CREATE TABLE ref_beam.agent_dead_letter (
    task_name character varying NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    error text,
    error_code character varying,
    attempts integer DEFAULT 0 NOT NULL,
    status character varying DEFAULT 'pending'::character varying NOT NULL,
    execution_id uuid,
    next_attempt_at timestamp with time zone,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((("left"((COALESCE(task_name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(status, ''::character varying))::text, 65536)) || ' '::text) || "left"(COALESCE(error, ''::text), 65536)) || ' '::text) || "left"((COALESCE(error_code, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_evaluation_case (
    dataset_id uuid NOT NULL,
    input_json jsonb NOT NULL,
    expected jsonb,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.agent_evaluation_dataset (
    name character varying NOT NULL,
    metadata_json jsonb,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.agent_evaluation_result (
    run_id uuid NOT NULL,
    case_id uuid NOT NULL,
    passed boolean NOT NULL,
    score double precision,
    details jsonb,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid
);

CREATE TABLE ref_beam.agent_evaluation_run (
    dataset_id uuid NOT NULL,
    agent_revision_id uuid NOT NULL,
    passed boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid
);

CREATE TABLE ref_beam.agent_event (
    action ref_beam.auditaction NOT NULL,
    recorded_at timestamp with time zone NOT NULL,
    agent_name character varying,
    user_id uuid,
    session_id uuid,
    run_id uuid,
    model_used character varying,
    modalities_used jsonb DEFAULT '[]'::jsonb NOT NULL,
    token_usage jsonb,
    latency_ms double precision,
    status character varying DEFAULT 'ok'::character varying NOT NULL,
    error text,
    input_preview text,
    output_preview text,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((((((("left"((COALESCE(status, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(agent_name, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(model_used, ''::character varying))::text, 65536)) || ' '::text) || "left"(COALESCE(error, ''::text), 65536)) || ' '::text) || "left"(COALESCE(input_preview, ''::text), 65536)) || ' '::text) || "left"(COALESCE(output_preview, ''::text), 65536)))) STORED
);

CREATE TABLE ref_beam.agent_execution (
    session_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    agent_revision_id uuid,
    status ref_beam.executionstatus NOT NULL,
    runtime_name character varying,
    runtime_adapter_version character varying,
    runtime_library_version character varying,
    runtime_ref jsonb,
    concurrency_key character varying,
    idempotency_key character varying,
    created_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    interrupted_at timestamp with time zone,
    completed_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    failed_at timestamp with time zone,
    timeout_at timestamp with time zone,
    current_attempt integer DEFAULT 0 NOT NULL,
    event_sequence integer DEFAULT 0 NOT NULL,
    last_error_code character varying,
    last_error_message character varying,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((((((((("left"((COALESCE(runtime_name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(last_error_code, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(runtime_adapter_version, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(runtime_library_version, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(concurrency_key, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(idempotency_key, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(last_error_message, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_execution_event (
    execution_id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED
);

CREATE TABLE ref_beam.agent_feedback (
    execution_id uuid NOT NULL,
    agent_revision_id uuid,
    rating integer,
    thumb character varying,
    comment text,
    created_at timestamp with time zone NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    org_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"((COALESCE(thumb, ''::character varying))::text, 65536) || ' '::text) || "left"(COALESCE(comment, ''::text), 65536)))) STORED
);

CREATE TABLE ref_beam.agent_interaction (
    session_id uuid NOT NULL,
    execution_id uuid,
    agent_id uuid NOT NULL,
    agent_name character varying NOT NULL,
    status ref_beam.interactionstatus NOT NULL,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    org_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, "left"((COALESCE(agent_name, ''::character varying))::text, 65536))) STORED
);

CREATE TABLE ref_beam.agent_knowledge_chunk (
    document_id uuid NOT NULL,
    scope ref_beam.knowledgescope,
    agent_id uuid,
    user_id uuid,
    source character varying,
    chunk_index integer DEFAULT 0 NOT NULL,
    token_count integer,
    content_hash character varying(64),
    content text NOT NULL,
    embedding public.vector(1536) NOT NULL,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    embedding_revision character varying,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((("left"((COALESCE(source, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(content_hash, ''::character varying))::text, 65536)) || ' '::text) || "left"(COALESCE(content, ''::text), 65536)))) STORED
);

CREATE TABLE ref_beam.agent_knowledge_document (
    scope ref_beam.knowledgescope,
    agent_id uuid,
    user_id uuid,
    title character varying NOT NULL,
    source character varying,
    type character varying NOT NULL,
    body text,
    media_id uuid,
    status ref_beam.knowledgedocumentstatus DEFAULT 'PENDING'::ref_beam.knowledgedocumentstatus NOT NULL,
    embedder_name character varying,
    chunk_count integer DEFAULT 0 NOT NULL,
    content_hash character varying(64),
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    document_revision character varying,
    embedding_revision character varying,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((((((((((("left"((COALESCE(title, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(source, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(type, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(embedder_name, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(content_hash, ''::character varying))::text, 65536)) || ' '::text) || "left"(COALESCE(body, ''::text), 65536)) || ' '::text) || "left"((COALESCE(document_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(embedding_revision, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_legal_hold (
    _id uuid NOT NULL,
    user_id character varying NOT NULL,
    org_id uuid,
    reason text,
    placed_by character varying,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone
);

CREATE TABLE ref_beam.agent_memory (
    user_id uuid NOT NULL,
    agent_id uuid,
    type ref_beam.memorytype DEFAULT 'SEMANTIC'::ref_beam.memorytype NOT NULL,
    content text NOT NULL,
    embedding public.vector(1536),
    embedder_name character varying,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    importance integer DEFAULT 1 NOT NULL,
    last_accessed_at timestamp with time zone,
    expires_at timestamp with time zone,
    version integer DEFAULT 1 NOT NULL,
    confidence double precision,
    source_execution_id uuid,
    source_message_id uuid,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"(COALESCE(content, ''::text), 65536) || ' '::text) || "left"((COALESCE(embedder_name, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_message (
    sequence integer NOT NULL,
    role ref_beam.agentmessagerole NOT NULL,
    type ref_beam.agentmessagetype NOT NULL,
    content jsonb NOT NULL,
    name character varying,
    tool_call_id character varying,
    tool_name character varying,
    tool_args jsonb,
    tool_result jsonb,
    tool_calls jsonb DEFAULT '[]'::jsonb NOT NULL,
    phase ref_beam.agentmessagephase NOT NULL,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    session_id uuid NOT NULL,
    execution_id uuid,
    interaction_id uuid,
    run_id uuid,
    agent_id uuid NOT NULL,
    step_id uuid,
    request_id uuid,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((("left"((COALESCE(name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(tool_call_id, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(tool_name, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_outbox_event (
    event_id uuid,
    execution_id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying DEFAULT 'event'::character varying NOT NULL,
    destination character varying DEFAULT 'redis'::character varying NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    sent boolean DEFAULT false NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone,
    last_error character varying,
    published_at timestamp with time zone,
    publisher_lease character varying,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED
);

CREATE TABLE ref_beam.agent_profile (
    user_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    role character varying,
    rules jsonb,
    metadata_json jsonb,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, "left"((COALESCE(role, ''::character varying))::text, 65536))) STORED
);

CREATE TABLE ref_beam.agent_revision (
    agent_id uuid,
    agent_name character varying NOT NULL,
    version character varying NOT NULL,
    revision_hash character varying NOT NULL,
    runtime_name character varying NOT NULL,
    runtime_adapter_version character varying,
    code_revision character varying,
    build_id character varying,
    prompt_revision character varying,
    toolset_revision character varying,
    model_policy_revision character varying,
    security_policy_revision character varying,
    memory_policy_revision character varying,
    knowledge_policy_revision character varying,
    input_schema_hash character varying,
    output_schema_hash character varying,
    runtime_config_snapshot jsonb,
    framework_config_snapshot jsonb,
    beam_version character varying,
    runtime_library_version character varying,
    application_build_id character varying,
    status ref_beam.revisionstatus NOT NULL,
    compatibility_policy ref_beam.compatibilitypolicy NOT NULL,
    compatible_with jsonb,
    created_at timestamp with time zone NOT NULL,
    activated_at timestamp with time zone,
    retired_at timestamp with time zone,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((((((((((((((((((((((((((((((("left"((COALESCE(agent_name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(version, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(revision_hash, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(runtime_name, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(runtime_adapter_version, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(runtime_library_version, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(build_id, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(toolset_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(beam_version, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(application_build_id, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(code_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(prompt_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(model_policy_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(security_policy_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(memory_policy_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(knowledge_policy_revision, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(input_schema_hash, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(output_schema_hash, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_run (
    session_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    execution_id uuid,
    attempt_no integer DEFAULT 1 NOT NULL,
    agent_id uuid NOT NULL,
    agent_name character varying NOT NULL,
    status ref_beam.runstatus NOT NULL,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    worker_id character varying,
    lease_id uuid,
    lease_expires_at timestamp with time zone,
    lease_fence integer DEFAULT 0 NOT NULL,
    retry_reason character varying,
    runtime_ref_snapshot jsonb,
    trace_id character varying,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((("left"((COALESCE(agent_name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(worker_id, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(retry_reason, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(trace_id, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_security_decision (
    run_id uuid,
    session_id uuid,
    interaction_id uuid,
    stage character varying NOT NULL,
    action character varying NOT NULL,
    severity character varying,
    score double precision,
    narration text,
    user_message text,
    violation_type character varying,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    matched_rules jsonb DEFAULT '[]'::jsonb NOT NULL,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    recorded_at timestamp with time zone NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((((((((("left"((COALESCE(stage, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(action, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(severity, ''::character varying))::text, 65536)) || ' '::text) || "left"(COALESCE(narration, ''::text), 65536)) || ' '::text) || "left"(COALESCE(user_message, ''::text), 65536)) || ' '::text) || "left"((COALESCE(violation_type, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_security_rule_narration (
    decision_id uuid NOT NULL,
    rule character varying NOT NULL,
    code integer NOT NULL,
    message text,
    ruleset character varying,
    revision integer,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.agent_session (
    agent_id uuid NOT NULL,
    user_id uuid,
    profile_id uuid,
    name character varying,
    resource character varying,
    resource_id uuid,
    last_active_at timestamp with time zone,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"((COALESCE(name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(resource, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_session_note (
    session_id uuid NOT NULL,
    key character varying NOT NULL,
    text text NOT NULL,
    pinned boolean DEFAULT false NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag character varying(64) DEFAULT (public.uuid_generate_v4())::character varying,
    _realm character varying
);

CREATE TABLE ref_beam.agent_session_summary (
    session_id uuid NOT NULL,
    coverage_end integer DEFAULT '-1'::integer NOT NULL,
    invalidated boolean DEFAULT false NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag character varying(64) DEFAULT (public.uuid_generate_v4())::character varying,
    _realm character varying
);

CREATE TABLE ref_beam.agent_step (
    session_id uuid NOT NULL,
    interaction_id uuid NOT NULL,
    run_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    status ref_beam.stepstatus NOT NULL,
    lc_step_id character varying,
    name character varying NOT NULL,
    input jsonb NOT NULL,
    result jsonb,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    org_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"((COALESCE(name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(lc_step_id, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_suggestion (
    session_id uuid NOT NULL,
    agent_id uuid NOT NULL,
    interaction_id uuid,
    type ref_beam.suggestiontype NOT NULL,
    status ref_beam.actionstatus NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    resolved_at timestamp with time zone,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    user_id uuid,
    org_id uuid,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED
);

CREATE TABLE ref_beam.agent_task (
    _id uuid NOT NULL,
    task character varying NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    status character varying DEFAULT 'QUEUED'::character varying NOT NULL,
    retryable boolean DEFAULT true NOT NULL,
    attempt_count integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 3 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    lease_owner character varying,
    lease_expires_at timestamp with time zone,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    result jsonb,
    error jsonb,
    user_id uuid,
    org_id uuid,
    execution_id uuid,
    src_cmd uuid,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"((COALESCE(task, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(status, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_tool_artifact (
    session_id uuid NOT NULL,
    execution_id uuid,
    tool_name character varying NOT NULL,
    call_id character varying NOT NULL,
    total_chars integer NOT NULL,
    text text NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag character varying(64) DEFAULT (public.uuid_generate_v4())::character varying,
    _realm character varying
);

CREATE TABLE ref_beam.agent_tool_grant (
    session_id uuid NOT NULL,
    tool_name character varying NOT NULL,
    user_id uuid,
    expires_at timestamp with time zone NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag character varying(64) DEFAULT (public.uuid_generate_v4())::character varying,
    _realm character varying
);

CREATE TABLE ref_beam.agent_tool_rule (
    scope character varying NOT NULL,
    user_id uuid,
    match jsonb DEFAULT '{}'::jsonb NOT NULL,
    mode character varying NOT NULL,
    locked boolean DEFAULT false NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag character varying(64) DEFAULT (public.uuid_generate_v4())::character varying,
    _realm character varying
);

CREATE TABLE ref_beam.agent_usage_counter (
    user_id uuid NOT NULL,
    period character varying NOT NULL,
    field character varying NOT NULL,
    value integer DEFAULT 0 NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.agent_usage_event (
    user_id uuid NOT NULL,
    tokens integer DEFAULT 0 NOT NULL,
    requests integer DEFAULT 1 NOT NULL,
    model_used character varying,
    modalities jsonb DEFAULT '[]'::jsonb NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED
);

CREATE TABLE ref_beam.agent_usage_plan (
    name character varying NOT NULL,
    overage_policy character varying DEFAULT 'hard_block'::character varying NOT NULL,
    status character varying DEFAULT 'active'::character varying NOT NULL,
    limit_max_requests_per_day integer,
    limit_max_tokens_per_day integer,
    limit_max_tokens_per_request integer,
    limit_allowed_agents character varying[],
    limit_allowed_modalities ref_beam.modality[],
    limit_allowed_models character varying[],
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((("left"((COALESCE(name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(status, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(overage_policy, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.agent_usage_profile (
    user_id uuid NOT NULL,
    plan_id uuid NOT NULL,
    plan_key character varying NOT NULL,
    active boolean DEFAULT true NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid GENERATED ALWAYS AS (org_id) STORED,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, "left"((COALESCE(plan_key, ''::character varying))::text, 65536))) STORED
);

CREATE TABLE ref_beam.agent_usage_reservation (
    user_id uuid NOT NULL,
    tokens integer DEFAULT 0 NOT NULL,
    status character varying DEFAULT 'reserved'::character varying NOT NULL,
    settled_tokens integer,
    execution_id uuid,
    expires_at timestamp with time zone,
    created_at timestamp with time zone,
    owner character varying,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.beam_vector_json (
    namespace character varying NOT NULL,
    text text NOT NULL,
    embedding jsonb DEFAULT '[]'::jsonb NOT NULL,
    metadata_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    user_id character varying,
    agent_id character varying,
    org_id character varying,
    document_id character varying,
    source character varying,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.execution_concurrency_slot (
    scope_key character varying NOT NULL,
    execution_id uuid NOT NULL,
    org_id uuid,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.execution_idempotency (
    key character varying NOT NULL,
    user_id uuid NOT NULL,
    agent_name character varying NOT NULL,
    execution_id uuid NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

CREATE TABLE ref_beam.model (
    name character varying NOT NULL,
    provider character varying NOT NULL,
    modalities jsonb DEFAULT '[]'::jsonb NOT NULL,
    capabilities jsonb DEFAULT '[]'::jsonb NOT NULL,
    cost_per_1k_tokens double precision DEFAULT '0'::double precision NOT NULL,
    max_tokens integer DEFAULT 4096 NOT NULL,
    provider_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    extra jsonb DEFAULT '{}'::jsonb NOT NULL,
    status ref_beam.modelstatus NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (("left"((COALESCE(name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(provider, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.runtime_state (
    state_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    run_id uuid NOT NULL,
    runtime_name character varying NOT NULL,
    run_format integer NOT NULL,
    adapter_version character varying NOT NULL,
    library_version character varying NOT NULL,
    status character varying NOT NULL,
    turn integer DEFAULT 0 NOT NULL,
    state jsonb NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE ref_beam.tool (
    tool_name character varying NOT NULL,
    tool_description character varying,
    tool_type character varying NOT NULL,
    tool_parameters jsonb NOT NULL,
    tool_output jsonb NOT NULL,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying,
    _tenant uuid,
    _txt tsvector GENERATED ALWAYS AS (to_tsvector('english'::regconfig, (((("left"((COALESCE(tool_name, ''::character varying))::text, 65536) || ' '::text) || "left"((COALESCE(tool_description, ''::character varying))::text, 65536)) || ' '::text) || "left"((COALESCE(tool_type, ''::character varying))::text, 65536)))) STORED
);

CREATE TABLE ref_beam.tool_call_idempotency (
    execution_id character varying NOT NULL,
    call_id character varying NOT NULL,
    tool_revision character varying NOT NULL,
    result_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    expires_at timestamp with time zone,
    _id uuid NOT NULL,
    _created timestamp with time zone DEFAULT now() NOT NULL,
    _updated timestamp with time zone,
    _creator uuid,
    _updater uuid,
    _deleted timestamp with time zone,
    _etag uuid DEFAULT gen_random_uuid() NOT NULL,
    _realm character varying
);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_artifact
    ADD CONSTRAINT agent_artifact_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_context_manifest
    ADD CONSTRAINT agent_context_manifest_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_cost_event
    ADD CONSTRAINT agent_cost_event_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_dead_letter
    ADD CONSTRAINT agent_dead_letter_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_case
    ADD CONSTRAINT agent_evaluation_case_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_dataset
    ADD CONSTRAINT agent_evaluation_dataset_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_result
    ADD CONSTRAINT agent_evaluation_result_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_run
    ADD CONSTRAINT agent_evaluation_run_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_event
    ADD CONSTRAINT agent_event_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_execution_event
    ADD CONSTRAINT agent_execution_event_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_execution
    ADD CONSTRAINT agent_execution_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_feedback
    ADD CONSTRAINT agent_feedback_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_interaction
    ADD CONSTRAINT agent_interaction_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_knowledge_chunk
    ADD CONSTRAINT agent_knowledge_chunk_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_knowledge_document
    ADD CONSTRAINT agent_knowledge_document_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_legal_hold
    ADD CONSTRAINT agent_legal_hold_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_memory
    ADD CONSTRAINT agent_memory_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_outbox_event
    ADD CONSTRAINT agent_outbox_event_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent
    ADD CONSTRAINT agent_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_profile
    ADD CONSTRAINT agent_profile_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_revision
    ADD CONSTRAINT agent_revision_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_run
    ADD CONSTRAINT agent_run_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_security_decision
    ADD CONSTRAINT agent_security_decision_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_security_rule_narration
    ADD CONSTRAINT agent_security_rule_narration_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_session_note
    ADD CONSTRAINT agent_session_note_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_session
    ADD CONSTRAINT agent_session_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_session_summary
    ADD CONSTRAINT agent_session_summary_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_step
    ADD CONSTRAINT agent_step_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_suggestion
    ADD CONSTRAINT agent_suggestion_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_task
    ADD CONSTRAINT agent_task_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_tool_artifact
    ADD CONSTRAINT agent_tool_artifact_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_tool_grant
    ADD CONSTRAINT agent_tool_grant_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_tool_rule
    ADD CONSTRAINT agent_tool_rule_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_usage_counter
    ADD CONSTRAINT agent_usage_counter_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_usage_counter
    ADD CONSTRAINT agent_usage_counter_user_id_period_field_key UNIQUE (user_id, period, field);

ALTER TABLE ONLY ref_beam.agent_usage_event
    ADD CONSTRAINT agent_usage_event_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_usage_plan
    ADD CONSTRAINT agent_usage_plan_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_usage_profile
    ADD CONSTRAINT agent_usage_profile_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_usage_profile
    ADD CONSTRAINT agent_usage_profile_user_id_plan_id_key UNIQUE (user_id, plan_id);

ALTER TABLE ONLY ref_beam.agent_usage_reservation
    ADD CONSTRAINT agent_usage_reservation_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.beam_vector_json
    ADD CONSTRAINT beam_vector_json_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.execution_concurrency_slot
    ADD CONSTRAINT execution_concurrency_slot_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.execution_idempotency
    ADD CONSTRAINT execution_idempotency_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.model
    ADD CONSTRAINT model_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.runtime_state
    ADD CONSTRAINT runtime_state_pkey PRIMARY KEY (state_id);

ALTER TABLE ONLY ref_beam.tool_call_idempotency
    ADD CONSTRAINT tool_call_idempotency_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.tool
    ADD CONSTRAINT tool_pkey PRIMARY KEY (_id);

ALTER TABLE ONLY ref_beam.agent_knowledge_chunk
    ADD CONSTRAINT uq_agent_knowledge_chunk_doc_idx UNIQUE (document_id, chunk_index);

ALTER TABLE ONLY ref_beam.agent_session_note
    ADD CONSTRAINT uq_agent_session_note_session_key UNIQUE (session_id, key);

ALTER TABLE ONLY ref_beam.agent_tool_grant
    ADD CONSTRAINT uq_agent_tool_grant_session_tool UNIQUE (session_id, tool_name);

ALTER TABLE ONLY ref_beam.execution_concurrency_slot
    ADD CONSTRAINT uq_execution_concurrency_slot_scope UNIQUE (scope_key);

ALTER TABLE ONLY ref_beam.execution_idempotency
    ADD CONSTRAINT uq_execution_idempotency_scope UNIQUE (key, user_id, agent_name);

ALTER TABLE ONLY ref_beam.tool_call_idempotency
    ADD CONSTRAINT uq_tool_call_idempotency UNIQUE (execution_id, call_id, tool_revision);

CREATE INDEX agent_action_txt_idx ON ref_beam.agent_action USING gin (_txt);

CREATE INDEX agent_action_user_idx ON ref_beam.agent_action USING btree (user_id);

CREATE INDEX agent_context_manifest_txt_idx ON ref_beam.agent_context_manifest USING gin (_txt);

CREATE INDEX agent_context_manifest_user_idx ON ref_beam.agent_context_manifest USING btree (user_id);

CREATE INDEX agent_dead_letter_txt_idx ON ref_beam.agent_dead_letter USING gin (_txt);

CREATE INDEX agent_event_txt_idx ON ref_beam.agent_event USING gin (_txt);

CREATE INDEX agent_execution_txt_idx ON ref_beam.agent_execution USING gin (_txt);

CREATE INDEX agent_execution_user_idx ON ref_beam.agent_execution USING btree (user_id);

CREATE INDEX agent_feedback_txt_idx ON ref_beam.agent_feedback USING gin (_txt);

CREATE INDEX agent_feedback_user_idx ON ref_beam.agent_feedback USING btree (user_id);

CREATE INDEX agent_interaction_txt_idx ON ref_beam.agent_interaction USING gin (_txt);

CREATE INDEX agent_interaction_user_idx ON ref_beam.agent_interaction USING btree (user_id);

CREATE INDEX agent_knowledge_chunk_txt_idx ON ref_beam.agent_knowledge_chunk USING gin (_txt);

CREATE INDEX agent_knowledge_document_txt_idx ON ref_beam.agent_knowledge_document USING gin (_txt);

CREATE UNIQUE INDEX agent_legal_hold_user_idx ON ref_beam.agent_legal_hold USING btree (user_id);

CREATE INDEX agent_memory_txt_idx ON ref_beam.agent_memory USING gin (_txt);

CREATE INDEX agent_message_txt_idx ON ref_beam.agent_message USING gin (_txt);

CREATE INDEX agent_message_user_idx ON ref_beam.agent_message USING btree (user_id);

CREATE INDEX agent_profile_txt_idx ON ref_beam.agent_profile USING gin (_txt);

CREATE INDEX agent_revision_txt_idx ON ref_beam.agent_revision USING gin (_txt);

CREATE INDEX agent_run_txt_idx ON ref_beam.agent_run USING gin (_txt);

CREATE INDEX agent_run_user_idx ON ref_beam.agent_run USING btree (user_id);

CREATE INDEX agent_security_decision_txt_idx ON ref_beam.agent_security_decision USING gin (_txt);

CREATE INDEX agent_security_decision_user_idx ON ref_beam.agent_security_decision USING btree (user_id);

CREATE INDEX agent_session_txt_idx ON ref_beam.agent_session USING gin (_txt);

CREATE INDEX agent_step_txt_idx ON ref_beam.agent_step USING gin (_txt);

CREATE INDEX agent_step_user_idx ON ref_beam.agent_step USING btree (user_id);

CREATE INDEX agent_suggestion_user_idx ON ref_beam.agent_suggestion USING btree (user_id);

CREATE INDEX agent_task_due_idx ON ref_beam.agent_task USING btree (next_attempt_at) WHERE (((status)::text = 'QUEUED'::text) AND (_deleted IS NULL));

CREATE INDEX agent_task_execution_idx ON ref_beam.agent_task USING btree (execution_id);

CREATE INDEX agent_task_finished_idx ON ref_beam.agent_task USING btree (finished_at) WHERE (finished_at IS NOT NULL);

CREATE INDEX agent_task_lease_idx ON ref_beam.agent_task USING btree (lease_expires_at) WHERE ((status)::text = 'RUNNING'::text);

CREATE INDEX agent_task_txt_idx ON ref_beam.agent_task USING gin (_txt);

CREATE INDEX agent_task_user_idx ON ref_beam.agent_task USING btree (user_id);

CREATE INDEX agent_txt_idx ON ref_beam.agent USING gin (_txt);

CREATE INDEX agent_usage_plan_txt_idx ON ref_beam.agent_usage_plan USING gin (_txt);

CREATE INDEX agent_usage_profile_txt_idx ON ref_beam.agent_usage_profile USING gin (_txt);

CREATE INDEX ix_agent_knowledge_chunk_embedding_hnsw ON ref_beam.agent_knowledge_chunk USING hnsw (embedding public.vector_cosine_ops) WITH (m='16', ef_construction='64');

CREATE INDEX ix_agent_memory_embedding_hnsw ON ref_beam.agent_memory USING hnsw (embedding public.vector_cosine_ops) WITH (m='16', ef_construction='64') WHERE (embedding IS NOT NULL);

CREATE INDEX ix_agent_outbox_event_pending ON ref_beam.agent_outbox_event USING btree (next_attempt_at) WHERE (sent = false);

CREATE INDEX ix_ref_beam_agent_action_agent_id ON ref_beam.agent_action USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_action_execution_id ON ref_beam.agent_action USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_action_interaction_id ON ref_beam.agent_action USING btree (interaction_id);

CREATE INDEX ix_ref_beam_agent_action_lc_action_id ON ref_beam.agent_action USING btree (lc_action_id);

CREATE INDEX ix_ref_beam_agent_action_message_id ON ref_beam.agent_action USING btree (message_id);

CREATE INDEX ix_ref_beam_agent_action_org_id ON ref_beam.agent_action USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_action_run_id ON ref_beam.agent_action USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_action_session_id ON ref_beam.agent_action USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_action_step_id ON ref_beam.agent_action USING btree (step_id);

CREATE INDEX ix_ref_beam_agent_artifact_execution_id ON ref_beam.agent_artifact USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_artifact_media_id ON ref_beam.agent_artifact USING btree (media_id);

CREATE INDEX ix_ref_beam_agent_artifact_session_id ON ref_beam.agent_artifact USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_context_manifest_execution_id ON ref_beam.agent_context_manifest USING btree (execution_id);

CREATE UNIQUE INDEX ix_ref_beam_agent_context_manifest_operation_id ON ref_beam.agent_context_manifest USING btree (operation_id);

CREATE INDEX ix_ref_beam_agent_context_manifest_org_id ON ref_beam.agent_context_manifest USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_context_manifest_run_id ON ref_beam.agent_context_manifest USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_cost_event_agent_name ON ref_beam.agent_cost_event USING btree (agent_name);

CREATE INDEX ix_ref_beam_agent_cost_event_execution_id ON ref_beam.agent_cost_event USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_cost_event_model ON ref_beam.agent_cost_event USING btree (model);

CREATE INDEX ix_ref_beam_agent_cost_event_org_id ON ref_beam.agent_cost_event USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_dead_letter_execution_id ON ref_beam.agent_dead_letter USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_dead_letter_org_id ON ref_beam.agent_dead_letter USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_dead_letter_status ON ref_beam.agent_dead_letter USING btree (status);

CREATE INDEX ix_ref_beam_agent_dead_letter_task_name ON ref_beam.agent_dead_letter USING btree (task_name);

CREATE INDEX ix_ref_beam_agent_evaluation_case_dataset_id ON ref_beam.agent_evaluation_case USING btree (dataset_id);

CREATE INDEX ix_ref_beam_agent_evaluation_dataset_name ON ref_beam.agent_evaluation_dataset USING btree (name);

CREATE INDEX ix_ref_beam_agent_evaluation_result_case_id ON ref_beam.agent_evaluation_result USING btree (case_id);

CREATE INDEX ix_ref_beam_agent_evaluation_result_run_id ON ref_beam.agent_evaluation_result USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_evaluation_run_agent_revision_id ON ref_beam.agent_evaluation_run USING btree (agent_revision_id);

CREATE INDEX ix_ref_beam_agent_evaluation_run_dataset_id ON ref_beam.agent_evaluation_run USING btree (dataset_id);

CREATE INDEX ix_ref_beam_agent_event_action ON ref_beam.agent_event USING btree (action);

CREATE INDEX ix_ref_beam_agent_event_agent_name ON ref_beam.agent_event USING btree (agent_name);

CREATE INDEX ix_ref_beam_agent_event_org_id ON ref_beam.agent_event USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_event_recorded_at ON ref_beam.agent_event USING btree (recorded_at);

CREATE INDEX ix_ref_beam_agent_event_run_id ON ref_beam.agent_event USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_event_session_id ON ref_beam.agent_event USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_event_user_id ON ref_beam.agent_event USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_execution_agent_id ON ref_beam.agent_execution USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_execution_agent_revision_id ON ref_beam.agent_execution USING btree (agent_revision_id);

CREATE INDEX ix_ref_beam_agent_execution_concurrency_key ON ref_beam.agent_execution USING btree (concurrency_key);

CREATE INDEX ix_ref_beam_agent_execution_event_execution_id ON ref_beam.agent_execution_event USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_execution_event_org_id ON ref_beam.agent_execution_event USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_execution_idempotency_key ON ref_beam.agent_execution USING btree (idempotency_key);

CREATE INDEX ix_ref_beam_agent_execution_org_id ON ref_beam.agent_execution USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_execution_session_id ON ref_beam.agent_execution USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_feedback_agent_revision_id ON ref_beam.agent_feedback USING btree (agent_revision_id);

CREATE INDEX ix_ref_beam_agent_feedback_execution_id ON ref_beam.agent_feedback USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_interaction_agent_id ON ref_beam.agent_interaction USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_interaction_execution_id ON ref_beam.agent_interaction USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_interaction_session_id ON ref_beam.agent_interaction USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_agent_id ON ref_beam.agent_knowledge_chunk USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_content_hash ON ref_beam.agent_knowledge_chunk USING btree (content_hash);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_document_id ON ref_beam.agent_knowledge_chunk USING btree (document_id);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_embedding_revision ON ref_beam.agent_knowledge_chunk USING btree (embedding_revision);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_org_id ON ref_beam.agent_knowledge_chunk USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_scope ON ref_beam.agent_knowledge_chunk USING btree (scope);

CREATE INDEX ix_ref_beam_agent_knowledge_chunk_user_id ON ref_beam.agent_knowledge_chunk USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_knowledge_document_agent_id ON ref_beam.agent_knowledge_document USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_knowledge_document_content_hash ON ref_beam.agent_knowledge_document USING btree (content_hash);

CREATE INDEX ix_ref_beam_agent_knowledge_document_embedding_revision ON ref_beam.agent_knowledge_document USING btree (embedding_revision);

CREATE INDEX ix_ref_beam_agent_knowledge_document_org_id ON ref_beam.agent_knowledge_document USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_knowledge_document_scope ON ref_beam.agent_knowledge_document USING btree (scope);

CREATE INDEX ix_ref_beam_agent_knowledge_document_user_id ON ref_beam.agent_knowledge_document USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_memory_agent_id ON ref_beam.agent_memory USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_memory_org_id ON ref_beam.agent_memory USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_memory_source_execution_id ON ref_beam.agent_memory USING btree (source_execution_id);

CREATE INDEX ix_ref_beam_agent_memory_type ON ref_beam.agent_memory USING btree (type);

CREATE INDEX ix_ref_beam_agent_memory_user_id ON ref_beam.agent_memory USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_message_agent_id ON ref_beam.agent_message USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_message_execution_id ON ref_beam.agent_message USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_message_interaction_id ON ref_beam.agent_message USING btree (interaction_id);

CREATE INDEX ix_ref_beam_agent_message_org_id ON ref_beam.agent_message USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_message_request_id ON ref_beam.agent_message USING btree (request_id);

CREATE INDEX ix_ref_beam_agent_message_run_id ON ref_beam.agent_message USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_message_session_id ON ref_beam.agent_message USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_message_step_id ON ref_beam.agent_message USING btree (step_id);

CREATE INDEX ix_ref_beam_agent_outbox_event_event_id ON ref_beam.agent_outbox_event USING btree (event_id);

CREATE INDEX ix_ref_beam_agent_outbox_event_execution_id ON ref_beam.agent_outbox_event USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_outbox_event_org_id ON ref_beam.agent_outbox_event USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_outbox_event_sent ON ref_beam.agent_outbox_event USING btree (sent);

CREATE INDEX ix_ref_beam_agent_profile_profile_id ON ref_beam.agent_profile USING btree (profile_id);

CREATE INDEX ix_ref_beam_agent_profile_user_id ON ref_beam.agent_profile USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_revision_agent_id ON ref_beam.agent_revision USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_revision_agent_name ON ref_beam.agent_revision USING btree (agent_name);

CREATE INDEX ix_ref_beam_agent_revision_revision_hash ON ref_beam.agent_revision USING btree (revision_hash);

CREATE INDEX ix_ref_beam_agent_run_agent_id ON ref_beam.agent_run USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_run_execution_id ON ref_beam.agent_run USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_run_interaction_id ON ref_beam.agent_run USING btree (interaction_id);

CREATE INDEX ix_ref_beam_agent_run_org_id ON ref_beam.agent_run USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_run_session_id ON ref_beam.agent_run USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_security_decision_interaction_id ON ref_beam.agent_security_decision USING btree (interaction_id);

CREATE INDEX ix_ref_beam_agent_security_decision_org_id ON ref_beam.agent_security_decision USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_security_decision_run_id ON ref_beam.agent_security_decision USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_security_decision_session_id ON ref_beam.agent_security_decision USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_security_decision_stage ON ref_beam.agent_security_decision USING btree (stage);

CREATE INDEX ix_ref_beam_agent_security_rule_narration_decision_id ON ref_beam.agent_security_rule_narration USING btree (decision_id);

CREATE INDEX ix_ref_beam_agent_session_agent_id ON ref_beam.agent_session USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_session_note_org_id ON ref_beam.agent_session_note USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_session_note_session_id ON ref_beam.agent_session_note USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_session_org_id ON ref_beam.agent_session USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_session_summary_org_id ON ref_beam.agent_session_summary USING btree (org_id);

CREATE UNIQUE INDEX ix_ref_beam_agent_session_summary_session_id ON ref_beam.agent_session_summary USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_step_agent_id ON ref_beam.agent_step USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_step_interaction_id ON ref_beam.agent_step USING btree (interaction_id);

CREATE INDEX ix_ref_beam_agent_step_lc_step_id ON ref_beam.agent_step USING btree (lc_step_id);

CREATE INDEX ix_ref_beam_agent_step_run_id ON ref_beam.agent_step USING btree (run_id);

CREATE INDEX ix_ref_beam_agent_step_session_id ON ref_beam.agent_step USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_suggestion_agent_id ON ref_beam.agent_suggestion USING btree (agent_id);

CREATE INDEX ix_ref_beam_agent_suggestion_interaction_id ON ref_beam.agent_suggestion USING btree (interaction_id);

CREATE INDEX ix_ref_beam_agent_suggestion_session_id ON ref_beam.agent_suggestion USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_tool_artifact_execution_id ON ref_beam.agent_tool_artifact USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_tool_artifact_org_id ON ref_beam.agent_tool_artifact USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_tool_artifact_session_id ON ref_beam.agent_tool_artifact USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_tool_grant_expires_at ON ref_beam.agent_tool_grant USING btree (expires_at);

CREATE INDEX ix_ref_beam_agent_tool_grant_org_id ON ref_beam.agent_tool_grant USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_tool_grant_session_id ON ref_beam.agent_tool_grant USING btree (session_id);

CREATE INDEX ix_ref_beam_agent_tool_rule_org_id ON ref_beam.agent_tool_rule USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_tool_rule_scope ON ref_beam.agent_tool_rule USING btree (scope);

CREATE INDEX ix_ref_beam_agent_tool_rule_user_id ON ref_beam.agent_tool_rule USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_usage_counter_org_id ON ref_beam.agent_usage_counter USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_usage_counter_user_id ON ref_beam.agent_usage_counter USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_usage_event_org_id ON ref_beam.agent_usage_event USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_usage_event_user_id ON ref_beam.agent_usage_event USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_usage_plan_name ON ref_beam.agent_usage_plan USING btree (name);

CREATE INDEX ix_ref_beam_agent_usage_profile_org_id ON ref_beam.agent_usage_profile USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_usage_profile_plan_id ON ref_beam.agent_usage_profile USING btree (plan_id);

CREATE INDEX ix_ref_beam_agent_usage_profile_plan_key ON ref_beam.agent_usage_profile USING btree (plan_key);

CREATE INDEX ix_ref_beam_agent_usage_profile_user_id ON ref_beam.agent_usage_profile USING btree (user_id);

CREATE INDEX ix_ref_beam_agent_usage_reservation_execution_id ON ref_beam.agent_usage_reservation USING btree (execution_id);

CREATE INDEX ix_ref_beam_agent_usage_reservation_org_id ON ref_beam.agent_usage_reservation USING btree (org_id);

CREATE INDEX ix_ref_beam_agent_usage_reservation_user_id ON ref_beam.agent_usage_reservation USING btree (user_id);

CREATE INDEX ix_ref_beam_beam_vector_json_namespace ON ref_beam.beam_vector_json USING btree (namespace);

CREATE INDEX ix_ref_beam_beam_vector_json_user_id ON ref_beam.beam_vector_json USING btree (user_id);

CREATE INDEX ix_ref_beam_execution_concurrency_slot_execution_id ON ref_beam.execution_concurrency_slot USING btree (execution_id);

CREATE INDEX ix_ref_beam_execution_concurrency_slot_org_id ON ref_beam.execution_concurrency_slot USING btree (org_id);

CREATE INDEX ix_ref_beam_execution_concurrency_slot_scope_key ON ref_beam.execution_concurrency_slot USING btree (scope_key);

CREATE INDEX ix_ref_beam_execution_idempotency_agent_name ON ref_beam.execution_idempotency USING btree (agent_name);

CREATE INDEX ix_ref_beam_execution_idempotency_execution_id ON ref_beam.execution_idempotency USING btree (execution_id);

CREATE INDEX ix_ref_beam_execution_idempotency_key ON ref_beam.execution_idempotency USING btree (key);

CREATE INDEX ix_ref_beam_execution_idempotency_user_id ON ref_beam.execution_idempotency USING btree (user_id);

CREATE UNIQUE INDEX ix_ref_beam_model_name ON ref_beam.model USING btree (name);

CREATE INDEX ix_ref_beam_tool_call_idempotency_execution_id ON ref_beam.tool_call_idempotency USING btree (execution_id);

CREATE INDEX model_txt_idx ON ref_beam.model USING gin (_txt);

CREATE INDEX runtime_state_execution_idx ON ref_beam.runtime_state USING btree (execution_id);

CREATE INDEX runtime_state_retention_idx ON ref_beam.runtime_state USING btree (status, _updated);

CREATE INDEX tool_txt_idx ON ref_beam.tool USING gin (_txt);

CREATE UNIQUE INDEX uq_agent_execution_event_execution_sequence ON ref_beam.agent_execution_event USING btree (execution_id, sequence);

CREATE UNIQUE INDEX uq_agent_outbox_event_execution_sequence ON ref_beam.agent_outbox_event USING btree (execution_id, sequence);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES ref_beam.agent_execution(_id);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_interaction_id_fkey FOREIGN KEY (interaction_id) REFERENCES ref_beam.agent_interaction(_id);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_run_id_fkey FOREIGN KEY (run_id) REFERENCES ref_beam.agent_run(_id);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_action
    ADD CONSTRAINT agent_action_step_id_fkey FOREIGN KEY (step_id) REFERENCES ref_beam.agent_step(_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_case
    ADD CONSTRAINT agent_evaluation_case_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES ref_beam.agent_evaluation_dataset(_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_result
    ADD CONSTRAINT agent_evaluation_result_run_id_fkey FOREIGN KEY (run_id) REFERENCES ref_beam.agent_evaluation_run(_id);

ALTER TABLE ONLY ref_beam.agent_evaluation_run
    ADD CONSTRAINT agent_evaluation_run_dataset_id_fkey FOREIGN KEY (dataset_id) REFERENCES ref_beam.agent_evaluation_dataset(_id);

ALTER TABLE ONLY ref_beam.agent_execution
    ADD CONSTRAINT agent_execution_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_execution
    ADD CONSTRAINT agent_execution_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_interaction
    ADD CONSTRAINT agent_interaction_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_interaction
    ADD CONSTRAINT agent_interaction_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES ref_beam.agent_execution(_id);

ALTER TABLE ONLY ref_beam.agent_interaction
    ADD CONSTRAINT agent_interaction_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_knowledge_chunk
    ADD CONSTRAINT agent_knowledge_chunk_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_knowledge_chunk
    ADD CONSTRAINT agent_knowledge_chunk_document_id_fkey FOREIGN KEY (document_id) REFERENCES ref_beam.agent_knowledge_document(_id);

ALTER TABLE ONLY ref_beam.agent_knowledge_document
    ADD CONSTRAINT agent_knowledge_document_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_memory
    ADD CONSTRAINT agent_memory_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES ref_beam.agent_execution(_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_interaction_id_fkey FOREIGN KEY (interaction_id) REFERENCES ref_beam.agent_interaction(_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_run_id_fkey FOREIGN KEY (run_id) REFERENCES ref_beam.agent_run(_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_message
    ADD CONSTRAINT agent_message_step_id_fkey FOREIGN KEY (step_id) REFERENCES ref_beam.agent_step(_id);

ALTER TABLE ONLY ref_beam.agent_revision
    ADD CONSTRAINT agent_revision_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_run
    ADD CONSTRAINT agent_run_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_run
    ADD CONSTRAINT agent_run_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES ref_beam.agent_execution(_id);

ALTER TABLE ONLY ref_beam.agent_run
    ADD CONSTRAINT agent_run_interaction_id_fkey FOREIGN KEY (interaction_id) REFERENCES ref_beam.agent_interaction(_id);

ALTER TABLE ONLY ref_beam.agent_run
    ADD CONSTRAINT agent_run_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_session
    ADD CONSTRAINT agent_session_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_step
    ADD CONSTRAINT agent_step_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_step
    ADD CONSTRAINT agent_step_interaction_id_fkey FOREIGN KEY (interaction_id) REFERENCES ref_beam.agent_interaction(_id);

ALTER TABLE ONLY ref_beam.agent_step
    ADD CONSTRAINT agent_step_run_id_fkey FOREIGN KEY (run_id) REFERENCES ref_beam.agent_run(_id);

ALTER TABLE ONLY ref_beam.agent_step
    ADD CONSTRAINT agent_step_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_suggestion
    ADD CONSTRAINT agent_suggestion_agent_id_fkey FOREIGN KEY (agent_id) REFERENCES ref_beam.agent(_id);

ALTER TABLE ONLY ref_beam.agent_suggestion
    ADD CONSTRAINT agent_suggestion_interaction_id_fkey FOREIGN KEY (interaction_id) REFERENCES ref_beam.agent_interaction(_id);

ALTER TABLE ONLY ref_beam.agent_suggestion
    ADD CONSTRAINT agent_suggestion_session_id_fkey FOREIGN KEY (session_id) REFERENCES ref_beam.agent_session(_id);

ALTER TABLE ONLY ref_beam.agent_usage_profile
    ADD CONSTRAINT agent_usage_profile_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES ref_beam.agent_usage_plan(_id);

ALTER TABLE ONLY ref_beam.execution_concurrency_slot
    ADD CONSTRAINT execution_concurrency_slot_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES ref_beam.agent_execution(_id);

ALTER TABLE ONLY ref_beam.execution_idempotency
    ADD CONSTRAINT execution_idempotency_execution_id_fkey FOREIGN KEY (execution_id) REFERENCES ref_beam.agent_execution(_id);
