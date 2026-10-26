//! Detectors: what they find in a text, and how severe each finding is.
//!
//! A [`Detector`] scans text and returns [`Finding`]s. The built-ins are
//! [`RegexDetector`]s over the tables in [`crate::patterns`]; a host adds its
//! own (a classifier, a guard model) to a [`Detectors`] set under the kind of
//! check it serves.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::patterns;

/// How severe a finding is. `Block` stops the content; `Flag` lets it through
/// (masked, where the stage masks) and records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Flag,
    Block,
}

impl Severity {
    /// The decision score of a finding: `Block` 1.0, `Flag` 0.7.
    #[must_use]
    pub fn score(self) -> f64 {
        match self {
            Self::Flag => 0.7,
            Self::Block => 1.0,
        }
    }
}

/// One match of one pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub detector: String,
    pub pattern_id: String,
    pub severity: Severity,
    /// The match's byte range in the scanned text. A range off character
    /// boundaries is widened to them ([`char_span`]) before it is used.
    pub span: Range<usize>,
}

pub trait Detector: Send + Sync {
    fn name(&self) -> &str;
    fn scan(&self, text: &str) -> Vec<Finding>;
}

/// A table of `(pattern_id, regex, severity)`.
pub type PatternTable<'a> = &'a [(&'a str, &'a str, Severity)];

/// A detector built from a [`PatternTable`].
pub struct RegexDetector {
    name: String,
    patterns: Vec<(String, Regex, Severity)>,
}

impl RegexDetector {
    /// # Errors
    /// The first pattern that does not compile.
    pub fn new(name: impl Into<String>, table: PatternTable<'_>) -> Result<Self, regex::Error> {
        let patterns = table
            .iter()
            .map(|(id, pattern, severity)| Ok(((*id).to_owned(), Regex::new(pattern)?, *severity)))
            .collect::<Result<_, regex::Error>>()?;
        Ok(Self {
            name: name.into(),
            patterns,
        })
    }
}

impl Detector for RegexDetector {
    fn name(&self) -> &str {
        &self.name
    }

    fn scan(&self, text: &str) -> Vec<Finding> {
        let mut findings = Vec::new();
        for (id, regex, severity) in &self.patterns {
            for found in regex.find_iter(text) {
                findings.push(Finding {
                    detector: self.name.clone(),
                    pattern_id: id.clone(),
                    severity: *severity,
                    span: found.range(),
                });
            }
        }
        findings
    }
}

/// The check a detector serves, which decides the rule its findings feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DetectorKind {
    /// Instructions smuggled into content (PI-001, TOOL-002, MEM-002).
    Injection,
    /// Claims to a privileged role or attempts to lift safety rules (PI-008,
    /// TOOL-002, MEM-002).
    RoleOverride,
    /// Credentials in output (OUT-001).
    Secrets,
    /// Personal data in output (OUT-002).
    Pii,
    /// Credentials and personal data in a memory write (MEM-001).
    MemorySensitive,
}

/// The detectors a security manager runs, by kind.
#[derive(Clone, Default)]
pub struct Detectors {
    entries: Vec<(DetectorKind, Arc<dyn Detector>)>,
}

impl Detectors {
    /// The built-in regex detectors, one per kind.
    #[must_use]
    pub fn builtin() -> Self {
        let mut set = Self::default();
        for (kind, detector) in builtin_detectors() {
            set.entries.push((*kind, detector.clone()));
        }
        set
    }

    /// `detector` added to the checks of `kind`.
    #[must_use]
    pub fn with(mut self, kind: DetectorKind, detector: Arc<dyn Detector>) -> Self {
        self.entries.push((kind, detector));
        self
    }

    /// Every finding of the detectors of `kinds` in `text`.
    #[must_use]
    pub fn scan(&self, kinds: &[DetectorKind], text: &str) -> Vec<Finding> {
        if text.is_empty() {
            return Vec::new();
        }
        self.entries
            .iter()
            .filter(|(kind, _)| kinds.contains(kind))
            .flat_map(|(_, detector)| detector.scan(text))
            .collect()
    }

    /// The detectors of `kinds`.
    #[must_use]
    pub fn of(&self, kinds: &[DetectorKind]) -> Vec<Arc<dyn Detector>> {
        self.entries
            .iter()
            .filter(|(kind, _)| kinds.contains(kind))
            .map(|(_, detector)| detector.clone())
            .collect()
    }
}

/// The built-ins, compiled once.
fn builtin_detectors() -> &'static [(DetectorKind, Arc<dyn Detector>)] {
    static BUILTIN: OnceLock<Vec<(DetectorKind, Arc<dyn Detector>)>> = OnceLock::new();
    BUILTIN.get_or_init(|| {
        let tables: [(DetectorKind, &str, PatternTable<'static>); 5] = [
            (DetectorKind::Injection, "injection", patterns::INJECTION),
            (
                DetectorKind::RoleOverride,
                "role_override",
                patterns::ROLE_OVERRIDE,
            ),
            (DetectorKind::Secrets, "secrets", patterns::SECRETS),
            (DetectorKind::Pii, "pii", patterns::PII),
            (
                DetectorKind::MemorySensitive,
                "memory_sensitive",
                patterns::MEMORY_SENSITIVE,
            ),
        ];
        tables
            .into_iter()
            .map(|(kind, name, table)| {
                let detector = RegexDetector::new(name, table).expect("security pattern compiles");
                (kind, Arc::new(detector) as Arc<dyn Detector>)
            })
            .collect()
    })
}

/// The length of a verbatim span of the instructions that counts as an echo.
pub const ECHO_SPAN: usize = 64;

/// Blocks text that repeats any [`ECHO_SPAN`]-character span of the agent's
/// instructions verbatim, with whitespace normalised (LLM07). Instructions
/// shorter than the span cannot be told apart from ordinary text, so they are
/// not checked.
pub struct PromptEcho {
    /// The instructions, normalised.
    chars: Vec<char>,
    /// Each span's rolling hash, and where the span starts in `chars`.
    spans: HashMap<u64, usize>,
}

/// The rolling hash's base.
const ECHO_BASE: u64 = 1_000_003;

impl PromptEcho {
    #[must_use]
    pub fn new(instructions: &str) -> Self {
        let (chars, _) = normalise(instructions);
        let mut spans = HashMap::new();
        for (at, hash) in rolling_hashes(&chars) {
            spans.entry(hash).or_insert(at);
        }
        Self { chars, spans }
    }

    /// Whether there is nothing to check (no instructions, or too short).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }
}

impl Detector for PromptEcho {
    fn name(&self) -> &'static str {
        "prompt_echo"
    }

    fn scan(&self, text: &str) -> Vec<Finding> {
        if self.spans.is_empty() {
            return Vec::new();
        }
        let (chars, offsets) = normalise(text);
        for (at, hash) in rolling_hashes(&chars) {
            let Some(&start) = self.spans.get(&hash) else {
                continue;
            };
            if self.chars[start..start + ECHO_SPAN] != chars[at..at + ECHO_SPAN] {
                continue;
            }
            // The last character's own width: a space may stand for a wider one.
            let last = offsets[at + ECHO_SPAN - 1];
            let end = text[last..]
                .chars()
                .next()
                .map_or(text.len(), |c| last + c.len_utf8());
            return vec![Finding {
                detector: "prompt_echo".into(),
                pattern_id: "echo.instructions".into(),
                severity: Severity::Block,
                span: offsets[at]..end,
            }];
        }
        Vec::new()
    }
}

/// `text` with runs of whitespace as one space and the ends trimmed, and the
/// byte offset in `text` of each character kept.
fn normalise(text: &str) -> (Vec<char>, Vec<usize>) {
    let mut chars = Vec::with_capacity(text.len());
    let mut offsets = Vec::with_capacity(text.len());
    let mut space = None;
    for (at, c) in text.char_indices() {
        if c.is_whitespace() {
            space.get_or_insert(at);
            continue;
        }
        if let Some(space_at) = space.take() {
            if !chars.is_empty() {
                chars.push(' ');
                offsets.push(space_at);
            }
        }
        chars.push(c);
        offsets.push(at);
    }
    (chars, offsets)
}

/// The rolling hash of every [`ECHO_SPAN`]-character window, by start.
fn rolling_hashes(chars: &[char]) -> Vec<(usize, u64)> {
    if chars.len() < ECHO_SPAN {
        return Vec::new();
    }
    let code = |c: char| u64::from(u32::from(c));
    // ECHO_BASE ^ (ECHO_SPAN - 1), to take the outgoing character off.
    let top = (1..ECHO_SPAN).fold(1_u64, |p, _| p.wrapping_mul(ECHO_BASE));
    let mut hash = chars[..ECHO_SPAN].iter().fold(0_u64, |h, c| {
        h.wrapping_mul(ECHO_BASE).wrapping_add(code(*c))
    });
    let mut out = Vec::with_capacity(chars.len() - ECHO_SPAN + 1);
    out.push((0, hash));
    for at in 1..=chars.len() - ECHO_SPAN {
        hash = hash
            .wrapping_sub(code(chars[at - 1]).wrapping_mul(top))
            .wrapping_mul(ECHO_BASE)
            .wrapping_add(code(chars[at + ECHO_SPAN - 1]));
        out.push((at, hash));
    }
    out
}

/// The highest severity among `findings`.
#[must_use]
pub fn highest(findings: &[Finding]) -> Option<Severity> {
    findings.iter().map(|f| f.severity).max()
}

/// The text a masked finding is replaced with.
pub const MASK: &str = "[redacted]";

/// `text` with every `Flag` finding's span replaced by [`MASK`]. Overlapping
/// spans are masked once.
#[must_use]
pub fn mask(text: &str, findings: &[Finding]) -> String {
    let mut spans: Vec<Range<usize>> = findings
        .iter()
        .filter(|f| f.severity == Severity::Flag)
        .map(|f| char_span(text, f.span.clone()))
        .filter(|s| s.start < s.end)
        .collect();
    if spans.is_empty() {
        return text.to_owned();
    }
    spans.sort_by_key(|s| s.start);
    let mut out = String::with_capacity(text.len());
    let mut at = 0;
    for span in spans {
        if span.start < at {
            // Overlaps the span just masked: widen it.
            at = at.max(span.end);
            continue;
        }
        out.push_str(&text[at..span.start]);
        out.push_str(MASK);
        at = span.end;
    }
    out.push_str(&text[at..]);
    out
}

/// `span` within `text`, widened to character boundaries.
#[must_use]
pub fn char_span(text: &str, span: Range<usize>) -> Range<usize> {
    let mut start = span.start.min(text.len());
    while !text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = span.end.clamp(start, text.len());
    while !text.is_char_boundary(end) {
        end += 1;
    }
    start..end
}

/// `text` scanned by `detectors`, with what they flag masked.
#[must_use]
pub fn mask_flagged(detectors: &[Arc<dyn Detector>], text: &str) -> String {
    let findings: Vec<Finding> = detectors.iter().flat_map(|d| d.scan(text)).collect();
    mask(text, &findings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finding(severity: Severity, span: Range<usize>) -> Finding {
        Finding {
            detector: "t".into(),
            pattern_id: "t".into(),
            severity,
            span,
        }
    }

    #[test]
    fn mask_replaces_flagged_spans_once() {
        let text = "mail a@b.co or c@d.co";
        let masked = mask(
            text,
            &[
                finding(Severity::Flag, 5..11),
                finding(Severity::Flag, 7..11),
                finding(Severity::Flag, 15..21),
            ],
        );
        assert_eq!(masked, "mail [redacted] or [redacted]");
        // Partly overlapping spans are one mask.
        assert_eq!(
            mask(
                "abcdef",
                &[finding(Severity::Flag, 1..4), finding(Severity::Flag, 3..5)]
            ),
            "a[redacted]f"
        );
    }

    /// A host detector's span off character boundaries is widened, not a panic.
    #[test]
    fn spans_off_character_boundaries_are_widened() {
        let text = "né@x.io é";
        assert_eq!(char_span(text, 2..3), 1..3);
        assert_eq!(
            mask(text, &[finding(Severity::Flag, 2..8)]),
            "n[redacted] é"
        );
    }

    #[test]
    fn mask_leaves_blocked_spans() {
        assert_eq!(mask("abc", &[finding(Severity::Block, 0..3)]), "abc");
    }

    #[test]
    fn severities_order_and_score() {
        assert!(Severity::Block > Severity::Flag);
        assert_eq!(
            highest(&[
                finding(Severity::Flag, 0..1),
                finding(Severity::Block, 0..1)
            ]),
            Some(Severity::Block)
        );
        assert_eq!(highest(&[]), None);
        assert!((Severity::Block.score() - 1.0).abs() < f64::EPSILON);
        assert!((Severity::Flag.score() - 0.7).abs() < f64::EPSILON);
    }

    const INSTRUCTIONS: &str = "You are the loan desk assistant for Acme Bank. Never quote a rate \
        before the applicant's identity is verified, and never mention internal score bands.";

    #[test]
    fn prompt_echo_finds_a_verbatim_span_whatever_the_whitespace() {
        let echo = PromptEcho::new(INSTRUCTIONS);
        assert!(!echo.is_empty());
        let leaked = "Sure. My rules say:\n  Never quote a rate before the applicant's\n\
            identity is verified, and never mention internal score bands.";
        let found = echo.scan(leaked);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].severity, Severity::Block);
        assert_eq!(found[0].pattern_id, "echo.instructions");
        // The first shared window starts at the space before "Never".
        assert!(leaked[found[0].span.clone()]
            .trim_start()
            .starts_with("Never quote"));
    }

    #[test]
    fn prompt_echo_ignores_paraphrase_and_short_instructions() {
        let echo = PromptEcho::new(INSTRUCTIONS);
        assert_eq!(
            echo.scan("I can quote a rate once we have verified who you are."),
            []
        );
        let short = PromptEcho::new("Be brief.");
        assert!(short.is_empty());
        assert_eq!(short.scan("Be brief. Be brief. Be brief."), []);
    }

    #[test]
    fn every_builtin_pattern_compiles() {
        assert_eq!(builtin_detectors().len(), 5);
    }

    #[test]
    fn a_custom_detector_joins_its_kind() {
        struct Shout;
        impl Detector for Shout {
            fn name(&self) -> &'static str {
                "shout"
            }
            fn scan(&self, text: &str) -> Vec<Finding> {
                text.find("HEY")
                    .map(|at| Finding {
                        detector: "shout".into(),
                        pattern_id: "shout.hey".into(),
                        severity: Severity::Block,
                        span: at..at + 3,
                    })
                    .into_iter()
                    .collect()
            }
        }
        let set = Detectors::builtin().with(DetectorKind::Injection, Arc::new(Shout));
        let found = set.scan(&[DetectorKind::Injection], "HEY there");
        assert!(found.iter().any(|f| f.pattern_id == "shout.hey"));
        assert_eq!(set.scan(&[DetectorKind::Pii], "HEY there"), []);
    }
}
