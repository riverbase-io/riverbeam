use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityStage {
    BeforeInput,
    BeforeContextInjection,
    BeforeLlm,
    AfterLlm,
    BeforeTool,
    AfterTool,
    BeforeMemoryWrite,
    BeforeOutput,
    BeforeHitlResume,
    Runtime,
}

/// A decision's action. Every variant stays so stored decisions read back;
/// the rules produce `Allow`, `Deny`, `Warn`, `RequireApproval` and `Mask`
/// only (`Sanitize`, `Quarantine` and `Terminate` are not produced).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityAction {
    Allow,
    Deny,
    Warn,
    Sanitize,
    RequireApproval,
    Quarantine,
    Mask,
    Terminate,
}

impl SecurityAction {
    #[must_use]
    pub fn denies(self) -> bool {
        matches!(self, Self::Deny | Self::Terminate | Self::Quarantine)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecuritySeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// What a decision found. Every variant stays so stored decisions read back;
/// the rules do not produce `OutputUnsafeContent`, `ToolSchemaViolation`,
/// `MemoryScopeViolation`, `RagTenantFilterViolation`, `RagSensitiveDocument`,
/// `RunTimeoutExceeded`, `RunLoopDetected`, `RunRecursionExceeded`,
/// `SecurityViolation` or `IdentityPropagationFailed`. `InjectionRagChunk`
/// labels indirect injection (in a tool's output).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ViolationType {
    InjectionUserInput,
    InjectionRagChunk,
    InjectionRoleOverride,
    OutputSecretDetected,
    OutputPiiDetected,
    OutputUnsafeContent,
    UnauthorizedTool,
    ToolSchemaViolation,
    HitlRequiredNotObtained,
    MemorySensitiveData,
    MemoryScopeViolation,
    MemoryPoisoningSuspected,
    RagTenantFilterViolation,
    RagSensitiveDocument,
    RunMaxStepsExceeded,
    RunTimeoutExceeded,
    RunTokenBudgetExceeded,
    RunLoopDetected,
    RunRecursionExceeded,
    SecurityViolation,
    IdentityMissing,
    IdentityPropagationFailed,
}
