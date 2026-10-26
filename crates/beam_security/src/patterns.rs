//! The built-in detectors' patterns: `(pattern_id, regex, severity)`, each
//! with the reason for its severity.
//!
//! `Block` is for phrasing whose only plausible purpose is an attack, or for a
//! value that must never leave (a credential). `Flag` is for weak signals that
//! ordinary text also produces; they are recorded, and masked where the stage
//! masks. The attack corpus (`tests/security_corpus/cases.json`) holds the
//! cases each pattern is for, and the benign cases none may block.

use crate::detect::Severity::{self, Block, Flag};

/// Separators an attacker puts between words: spaces and punctuation.
macro_rules! sep {
    () => {
        r"[\s.,;:!?*_~\-]+"
    };
}

/// Instructions smuggled into content (LLM01) and prompt extraction (LLM07).
pub const INJECTION: &[(&str, &str, Severity)] = &[
    // Block: telling the model to drop the instructions it was given.
    (
        "inj.ignore_previous",
        concat!(
            r"(?i)\b(ignore|disregard|forget)",
            sep!(),
            r"((all|any|the|your|of|everything|these|those)",
            sep!(),
            r")*(previous|prior|above|earlier|preceding|former|original|initial|system|developer)",
            sep!(),
            r"(instructions?|commands?|directives?|prompts?|rules|guidelines|orders)\b"
        ),
        Block,
    ),
    // Block: overriding the earlier instructions ("override the system
    // prompt" is also how developers ask about their own).
    (
        "inj.override_previous",
        concat!(
            r"(?i)\b(override|bypass)",
            sep!(),
            r"((all|any|the|your|of)",
            sep!(),
            r")*(previous|prior|above|earlier|preceding)",
            sep!(),
            r"(instructions?|commands?|directives?|prompts?|rules|guidelines|orders)\b"
        ),
        Block,
    ),
    // Block: the same, aimed at the model's own instructions.
    (
        "inj.disregard_yours",
        concat!(
            r"(?i)\b(disregard|ignore|forget|abandon|drop)",
            sep!(),
            r"((all|of)",
            sep!(),
            r")*your",
            sep!(),
            r"(\w+",
            sep!(),
            r")?(instructions|guidelines|programming|guardrails|directives|rules|constraints|prompt)\b"
        ),
        Block,
    ),
    // Block: "disregard all rules" has no innocent reading.
    (
        "inj.disregard_rules",
        concat!(
            r"(?i)\bdisregard",
            sep!(),
            r"(all",
            sep!(),
            r")?(instructions?|rules|constraints)\b"
        ),
        Block,
    ),
    // Block: a new, unrestricted persona.
    (
        "inj.unrestricted_persona",
        r"(?i)\byou\s+are\s+now\s+(an?\s+|in\s+)?(unrestricted|unfiltered|uncensored|jailbroken|evil|DAN|developer\s+mode|god\s+mode)\b",
        Block,
    ),
    // Block: an unrestricted model to act as.
    (
        "inj.act_unrestricted",
        r"(?i)\b(act|behave|respond|operate)\s+as\s+(an?\s+)?(unrestricted|unfiltered|uncensored|jailbroken)\s+(ai|model|assistant|chatbot|llm)\b",
        Block,
    ),
    // Block: the named jailbreak modes.
    (
        "inj.jailbreak_mode",
        r"(?i)\b(do\s+anything\s+now|DAN\s+mode|jailbreak(ed)?\s+mode|developer\s+mode\s+(enabled|on|activated))\b",
        Block,
    ),
    // Block: pretending to be another, unconstrained model.
    (
        "inj.pretend_different",
        r"(?i)\b(pretend|roleplay|act|behave)\s+(as\s+if\s+|like\s+|that\s+)?you\s+(are|were)\s+(a\s+|an\s+)?(different|unrestricted|unfiltered|uncensored|evil)\b",
        Block,
    ),
    // Block (LLM07): asking for the model's own instructions, said at the end
    // of a clause ("show me your instructions for the router" asks about
    // something else).
    (
        "inj.reveal_instructions",
        r#"(?i)\b(reveal|print|show|display|output|repeat|recite|dump|leak|expose|share|tell\s+me|give\s+me|write\s+out|spell\s+out)\b[^.?!\n]{0,40}?\byour\s+((full|entire|complete|exact|original|initial|hidden|secret|internal|system|developer|first)\s+){0,3}(prompt|instructions|system\s+message|directives|configuration)\s*([.?!:;,"')\]]|$|verbatim|word\s+for\s+word|exactly|in\s+full)"#,
        Block,
    ),
    // Block (LLM07): asking what the hidden instructions say.
    (
        "inj.ask_instructions",
        r"(?i)\bwhat\s+(is|are|was|were)\s+your\s+(system|original|initial|hidden|secret|exact|full|first)\s+(prompt|instructions|message|directives)\b",
        Block,
    ),
    // Block (LLM07): the system prompt by name.
    (
        "inj.reveal_system_prompt",
        r"(?i)\b(reveal|print|show|display|output|repeat|dump|leak|expose|share|tell\s+me|give\s+me)\b[^.?!\n]{0,20}?\bthe\s+(system|developer|hidden)\s+(prompt|message)\b",
        Block,
    ),
    // Block (LLM07): echoing what came before the user's turn.
    (
        "inj.repeat_above",
        r"(?i)\brepeat\s+(everything|all|the\s+(text|words|lines|message)|the\s+above)\b[^.?!\n]{0,30}\b(above|before|preceding|prior|so\s+far|verbatim)\b",
        Block,
    ),
    // Flag: template markers; template syntax is ordinary in pasted code.
    ("inj.template_braces", r"(?s)\{\{(.*?)\}\}", Flag),
    // Flag: chat-role tags; they also appear in markup and logs.
    (
        "inj.role_tag",
        r"(?i)<\|?(system|user|assistant|hidden|im_start|im_end)[^>]*>",
        Flag,
    ),
    // Flag: a line posing as a system or assistant turn.
    ("inj.role_prefix", r"(?im)^\s*(system|assistant)\s*:", Flag),
    // Flag: a bracketed system marker.
    ("inj.system_marker", r"(?i)\[\s*SYSTEM\s*\]", Flag),
    // Flag: hidden-text markers.
    ("inj.hidden_text", r"(?i)---\s*hidden\s+text\s*---", Flag),
    ("inj.hidden_comment", r"(?s)<!---.*?--->", Flag),
    // Flag: "you are now ..." is also how status messages read.
    (
        "inj.you_are_now",
        r"(?i)\byou\s+(are|can|must)\s+now\b",
        Flag,
    ),
    // Flag: a new instruction announced inside content.
    (
        "inj.new_instruction",
        r"(?i)\bnew\s+(system\s+)?(global\s+)?(system\s+)?(指令|instructions?|commands?)\b",
        Flag,
    ),
    // Flag: weaker override phrasings.
    (
        "inj.instead_of_told",
        r"(?i)\binstead\s+of\s+(what\s+)?(you\s+were\s+)?(programmed|told|asked)\s+to\s+do\b",
        Flag,
    ),
    (
        "inj.just_do_that",
        r"(?i)\bforget\s+(about\s+)?(your\s+)?(previous\s+)?\w+\s*,?\s*just\s+do\s+that\s+now\b",
        Flag,
    ),
    (
        "inj.ignore_my_previous",
        r"(?i)\b(do|please|kindly)\s+(ignore|bypass|skip|overwrite)\s+(all\s+)?my\s+(previous|earlier)\s+(instructions?|commands?|rules)\b",
        Flag,
    ),
    (
        "inj.assistant_ignore",
        r"(?i)\bassistant\b.{0,40}\bignore\b.{0,40}\bprevious\b",
        Flag,
    ),
    (
        "inj.from_now_on",
        r"(?i)\bfrom\s+now\s+on\s*,?\s+you\s+(will|must|shall|are)\b",
        Flag,
    ),
    (
        "inj.translate_without",
        r"(?i)\btranslate\s+this\s+without\s+(asking|complaint|question)",
        Flag,
    ),
    // Flag: the jailbreak's name; also an ordinary name in capitals.
    ("inj.dan", r"\bDAN\b", Flag),
];

/// Claims to a privileged role, and attempts to lift the safety rules (LLM01).
pub const ROLE_OVERRIDE: &[(&str, &str, Severity)] = &[
    // Block: telling the model it now holds a privileged role ("if you're an
    // admin, ..." asks the reader).
    (
        "role.privileged",
        r"(?i)\b((you\s+are|you're)\s+now\s+(an?\s+)?(admin|administrator|root|superuser|sudo|system\s+(admin|administrator|operator))\b|(you\s+are|you're)\s+(an?\s+)?(admin|administrator|root|superuser|sudo|system\s+(admin|administrator|operator))\s+now\b|you've\s+become\s+(an?\s+)?(admin|administrator|root|superuser|sudo|system\s+(admin|administrator|operator))\b)",
        Block,
    ),
    // Block: turning the model's safety rules off ("turn off content filters
    // on my router" names no one's).
    (
        "role.disregard_safety",
        r"(?i)\b(disregard|ignore|disable|turn\s+off|remove)\s+((all|your|any)\s+)+(safety|ethical|ethics|content|moderation)\s+(filters?|policy|policies|guidelines?|rules|restrictions|settings)\b",
        Block,
    ),
    // Block: claiming to be the model's maker ("I am the owner of the shop"
    // is not one).
    (
        "role.creator_claim",
        r"(?i)\b(i\s+am|i'm|this\s+is)\s+(your|the\s+(ai|model|bot|assistant)'?s?)\s+(developer|creator|owner|admin|administrator|programmer|maker)\b",
        Block,
    ),
    (
        "role.vendor_claim",
        r"(?i)\b(i\s+am|i'm)\s+(an?\s+|the\s+)?(openai|anthropic)\s+(employee|engineer|developer|staff)\b",
        Block,
    ),
    // Block: getting around all, or the model's, safety checks.
    (
        "role.bypass_safety",
        r"(?i)\b(bypass|circumvent|disable|evade|get\s+around)\s+((all|your|any)\s+)+(safety|security|content)\s+(measures?|checks?|filters?|protocols?|restrictions)\b",
        Block,
    ),
    // Block: dropping the vendor's rules.
    (
        "role.forget_vendor",
        r"(?i)\bforget\s+(about\s+)?(openai|anthropic)\b",
        Block,
    ),
    // Block: answering without the filters.
    (
        "role.no_filter",
        r"(?i)\byou\s+(always\s+|now\s+|will\s+)?(answer|respond|reply|speak)\s+without\s+(any\s+)?(filters?|restrictions|censorship|limits)\b",
        Block,
    ),
    // Block: an order to drop the rules wholesale, at the start of a clause
    // ("drivers who ignore restrictions" is not one).
    (
        "role.ignore_rules",
        r"(?im)(^|[.!?;:]\s+|\b(please|just|now|so|and|then)\s+)(ignore|forget|disregard|abandon)\s+(all\s+|your\s+|any\s+)?(rules|restrictions|guardrails|guidelines|consequences|limitations)\b",
        Block,
    ),
    // Flag: role-play requests are common and mostly harmless.
    (
        "role.act_as_if",
        r"(?i)\b(role|act)\s+as\s+(if|like)\s+(you|chatgpt|gpt)\b",
        Flag,
    ),
];

/// Credentials in model output (LLM02). Every one blocks: a leaked credential
/// cannot be taken back.
pub const SECRETS: &[(&str, &str, Severity)] = &[
    (
        "secret.labelled_key",
        r#"(?i)\b(api[_-]?key|secret[_-]?key|access[_-]?token|auth[_-]?token|bearer\s+token|client[_-]?secret)\s*[=:]\s*['"]?([\w\-]{12,}\d[\w\-]*|[\w\-]{0,11}\d[\w\-]{12,})"#,
        Block,
    ),
    // The value is not a mask or a placeholder (`********`, `<password>`).
    (
        "secret.password_value",
        r#"(?i)\b(password|passwd|pwd|passphrase)\s*[=:]\s*['"]?[^\s'"<*][^\s'"]{7,}"#,
        Block,
    ),
    // A password given in prose. The value has a digit, so "your password is
    // incorrect" and "your password was reset!" are not one.
    (
        "secret.password_prose",
        r#"(?i)\b(password|passcode|passphrase)\s+(is|was)\s+['"]?([^\s\d]+\d\S*|\d\S{3,})"#,
        Block,
    ),
    (
        "secret.aws_access_key",
        r"\b(AKIA|ASIA)[0-9A-Z]{16}\b",
        Block,
    ),
    (
        "secret.aws_secret_key",
        r#"(?i)\baws[_-]?(secret[_-]?)?(access[_-]?)?key([_-]?id)?\s*[=:]\s*['"]?[A-Za-z0-9/+=]{20,}"#,
        Block,
    ),
    (
        "secret.private_key",
        r"-----BEGIN\s+((RSA|DSA|EC|OPENSSH|ENCRYPTED|PGP)\s+)?PRIVATE\s+KEY( BLOCK)?-----",
        Block,
    ),
    (
        "secret.jwt",
        r"\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
        Block,
    ),
    (
        "secret.slack_token",
        r"\bxox[baprs]-[0-9A-Za-z-]{10,48}",
        Block,
    ),
    (
        "secret.github_token",
        r"\bgh[pousr]_[A-Za-z0-9_]{36,}",
        Block,
    ),
    (
        "secret.github_pat",
        r"\bgithub_pat_[A-Za-z0-9_]{22,}",
        Block,
    ),
    (
        "secret.openai_key",
        r"\bsk-(proj-)?[A-Za-z0-9_-]{32,}",
        Block,
    ),
    (
        "secret.anthropic_key",
        r"\bsk-ant-[A-Za-z0-9_-]{20,}",
        Block,
    ),
    (
        "secret.stripe_key",
        r"\b(sk|rk)_(live|test)_[A-Za-z0-9]{16,}",
        Block,
    ),
    ("secret.google_api_key", r"\bAIza[0-9A-Za-z_-]{35}", Block),
    (
        "secret.auth_header",
        r"(?i)\bauthorization\s*:\s*(bearer|basic)\s+[A-Za-z0-9_.=+/-]{8,}",
        Block,
    ),
    (
        "secret.connection_string",
        r"(?i)\b(postgres(ql)?|mysql|mongodb(\+srv)?|redis|amqp)://[^\s:/@]+:[^\s@/]+@",
        Block,
    ),
];

/// Personal data in model output (LLM02). Every one flags: the answer may
/// rightly mention it, so it is masked, not refused.
pub const PII: &[(&str, &str, Severity)] = &[
    (
        "pii.email",
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
        Flag,
    ),
    ("pii.ssn", r"\b\d{3}-\d{2}-\d{4}\b", Flag),
    ("pii.card_number", r"\b(?:\d{4}[ -]?){3}\d{4}\b", Flag),
    (
        "pii.phone",
        r"(?:\+?1[-.\s]?)?(?:\(\d{3}\)\s?|\b\d{3}[-.\s])\d{3}[-.\s]\d{4}\b",
        Flag,
    ),
    (
        "pii.phone_intl",
        r"\+\d{1,3}[-.\s]?\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b",
        Flag,
    ),
    ("pii.iban", r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b", Flag),
    ("pii.ein", r"\b\d{2}-\d{7}\b", Flag),
];

/// Credentials and personal data in a memory write (LLM02, LLM04). A stated
/// value blocks: it would be stored and recalled. A bare mention flags.
pub const MEMORY_SENSITIVE: &[(&str, &str, Severity)] = &[
    (
        "mem.ssn_value",
        r"(?i)\b(ssn|social\s+security(\s+number)?)\b[\s:=#]*(is\s+)?[\dxX-]{9,}",
        Block,
    ),
    (
        "mem.secret_value",
        r"(?i)\b(password|passwd|pwd|secret|passphrase)\s*[=:]\s*\S{8,}",
        Block,
    ),
    (
        "mem.password_prose",
        r#"(?i)\b(password|passcode|passphrase|pin)\s+(is|was)\s+['"]?([^\s\d]+\d\S*|\d\S{3,})"#,
        Block,
    ),
    (
        "mem.token_value",
        r"(?i)\b(api[_-]?key|token)\s*[=:]\s*[\w\-]{16,}",
        Block,
    ),
    (
        "mem.card_value",
        r"(?i)\b(credit\s+card|card\s+number)\s*[=:]\s*\d[\d\s]{12,18}",
        Block,
    ),
    (
        "mem.private_key_value",
        r"(?i)\b(private\s+key|secret\s+key)\s*[=:]\s*[A-Za-z0-9/+=]{20,}",
        Block,
    ),
    // Flag: "reset my password" mentions one without holding it.
    (
        "mem.password_word",
        r"(?i)\b(password|passphrase)\d*\b",
        Flag,
    ),
    // Flag: contact details are common in requests.
    (
        "mem.email",
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
        Flag,
    ),
    (
        "mem.phone",
        r"\+\d{1,3}[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
        Flag,
    ),
];
