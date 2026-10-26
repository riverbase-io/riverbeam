//! The stream guard (plan 030 D2): a run's model text is held back until the
//! output detectors have scanned it.
//!
//! Text deltas are appended to a buffer, and the buffer is scanned together
//! with the last characters already released, so a match split across frames
//! is still found. Everything but the last `holdback` characters is released,
//! never cutting through a match. A `Block` finding stops the run with
//! `BEM-061`, and nothing after it is released; a `Flag` finding is masked in
//! the released text. Any other frame first releases the buffer after a last
//! scan, so frames keep their order. A complete model message (not a delta)
//! is scanned whole.
//!
//! A secret longer than the window can be partly released before it matches,
//! so the window should be at least as long as the longest secret expected.

use std::sync::Arc;

use beam_security::{char_span, mask, Detector, Finding, Severity};
use beam_types::{AgentMessageRole, AgentMessageType, ContentPart, StreamChunk, StreamChunkType};
use riverbase_core::RiverbaseResult;
use uuid::Uuid;

/// The guard of one run's stream.
pub(in crate::river_beam) struct OutputGuard {
    detectors: Vec<Arc<dyn Detector>>,
    /// Characters held back until more text has been scanned after them.
    holdback: usize,
    /// Text of the current message not released yet.
    held: String,
    /// The last released characters of the current message (unmasked),
    /// scanned again as context.
    tail: String,
    /// The latest delta of the current message: released text goes out in
    /// its shape.
    template: Option<StreamChunk>,
    /// The finding that stopped the stream, for the audit.
    blocked: Option<Finding>,
}

impl OutputGuard {
    pub(in crate::river_beam) fn new(detectors: Vec<Arc<dyn Detector>>, holdback: usize) -> Self {
        Self {
            detectors,
            holdback,
            held: String::new(),
            tail: String::new(),
            template: None,
            blocked: None,
        }
    }

    /// The finding that stopped the stream, once [`Self::admit`] or
    /// [`Self::finish`] refused it.
    pub(in crate::river_beam) fn blocked(&self) -> Option<&Finding> {
        self.blocked.as_ref()
    }

    /// The frames to send for `chunk`, in order.
    ///
    /// # Errors
    /// `BEM-061` when the text holds a blocking finding.
    pub(in crate::river_beam) fn admit(
        &mut self,
        mut chunk: StreamChunk,
    ) -> RiverbaseResult<Vec<StreamChunk>> {
        let Some(text) = model_text(&chunk) else {
            let mut out = self.finish()?;
            out.push(chunk);
            return Ok(out);
        };
        let is_delta = chunk.data.as_ref().is_some_and(|m| m.is_delta);
        if is_delta && text.is_empty() {
            // Nothing to scan: the held text stays held, the tail stays.
            return Ok(vec![chunk]);
        }
        if !is_delta {
            let mut out = self.finish()?;
            let findings = self.scan(&text);
            if let Some(blocking) = findings.iter().find(|f| f.severity == Severity::Block) {
                return Err(self.block(blocking.clone()));
            }
            let masked = mask(&text, &findings);
            if masked != text {
                set_text(&mut chunk, masked);
            }
            out.push(chunk);
            return Ok(out);
        }
        let mut out = Vec::new();
        if self
            .template
            .as_ref()
            .is_some_and(|held| !same_stream(held, &chunk))
        {
            out = self.finish()?;
        }
        self.held.push_str(&text);
        self.template = Some(chunk);
        out.extend(self.release(false)?);
        Ok(out)
    }

    /// The held text after a last scan; the next message starts afresh.
    ///
    /// # Errors
    /// `BEM-061` when the text holds a blocking finding.
    pub(in crate::river_beam) fn finish(&mut self) -> RiverbaseResult<Vec<StreamChunk>> {
        let released = if self.held.is_empty() {
            None
        } else {
            self.release(true)?
        };
        self.tail.clear();
        self.template = None;
        Ok(released
            .map(|mut chunk| {
                // The template's own frame may already have gone out.
                chunk.id = Some(Uuid::new_v4().to_string());
                chunk
            })
            .into_iter()
            .collect())
    }

    /// Every detector's findings, with spans on character boundaries.
    fn scan(&self, text: &str) -> Vec<Finding> {
        self.detectors
            .iter()
            .flat_map(|d| d.scan(text))
            .map(|f| Finding {
                span: char_span(text, f.span.clone()),
                ..f
            })
            .collect()
    }

    /// Stop the stream on `finding`: nothing held goes out.
    fn block(&mut self, finding: Finding) -> riverbase_core::RiverbaseError {
        tracing::warn!(
            detector = %finding.detector,
            pattern = %finding.pattern_id,
            "streamed output blocked by security policy"
        );
        self.held.clear();
        self.template = None;
        self.blocked = Some(finding);
        crate::BEM_061.with_data("This request could not be completed for security reasons.")
    }

    /// Release what the window allows (`all`: everything held), masked.
    fn release(&mut self, all: bool) -> RiverbaseResult<Option<StreamChunk>> {
        let base = self.tail.len();
        let window = format!("{}{}", self.tail, self.held);
        // The findings that reach into the held text, in its offsets.
        let live: Vec<Finding> = self
            .scan(&window)
            .into_iter()
            .filter(|f| f.span.end > base)
            .map(|f| Finding {
                span: f.span.start.saturating_sub(base)..f.span.end - base,
                ..f
            })
            .collect();
        if let Some(blocking) = live.iter().find(|f| f.severity == Severity::Block) {
            return Err(self.block(blocking.clone()));
        }
        let mut cut = if all {
            self.held.len()
        } else {
            let chars = self.held.chars().count();
            chars
                .checked_sub(self.holdback)
                .and_then(|n| self.held.char_indices().nth(n).map(|(at, _)| at))
                .unwrap_or(0)
        };
        // Never cut through a match: it is released whole, masked, next time.
        while let Some(start) = live
            .iter()
            .filter(|f| f.span.start < cut && cut < f.span.end)
            .map(|f| f.span.start)
            .min()
        {
            cut = start;
        }
        if cut == 0 {
            return Ok(None);
        }
        let raw: String = self.held.drain(..cut).collect();
        let masked = mask(
            &raw,
            &live
                .into_iter()
                .filter(|f| f.span.start < cut)
                .collect::<Vec<_>>(),
        );
        self.tail.push_str(&raw);
        let keep = self.tail.chars().count().saturating_sub(self.holdback);
        if let Some((at, _)) = self.tail.char_indices().nth(keep) {
            self.tail.drain(..at);
        }
        Ok(self.template.clone().map(|mut chunk| {
            set_text(&mut chunk, masked);
            chunk
        }))
    }
}

/// The text of a model message frame (empty when it has none), or `None`
/// for any other frame.
fn model_text(chunk: &StreamChunk) -> Option<String> {
    if chunk.chunk_type != StreamChunkType::Chunk {
        return None;
    }
    let message = chunk.data.as_ref()?;
    if message.role != AgentMessageRole::Ai || message.msg_type != AgentMessageType::Message {
        return None;
    }
    Some(
        message
            .content
            .iter()
            .filter_map(|p| p.text.as_deref())
            .collect(),
    )
}

/// Deltas of one model message: the same run, agent and phase.
fn same_stream(held: &StreamChunk, next: &StreamChunk) -> bool {
    held.run_id == next.run_id
        && held.agent_name == next.agent_name
        && held.data.as_ref().map(|m| m.phase) == next.data.as_ref().map(|m| m.phase)
}

fn set_text(chunk: &mut StreamChunk, text: String) {
    if let Some(message) = chunk.data.as_mut() {
        message.content = vec![ContentPart::text(text)];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_security::{SecurityManagement, SecurityManager, SecurityPolicy};
    use beam_types::AgentMessage;

    fn guard(holdback: usize) -> OutputGuard {
        let detectors = SecurityManager::new(SecurityPolicy::default()).output_detectors();
        OutputGuard::new(detectors, holdback)
    }

    fn delta(text: &str) -> StreamChunk {
        let mut chunk = StreamChunk::error_frame(String::new());
        chunk.chunk_type = StreamChunkType::Chunk;
        chunk.error = None;
        chunk.data = Some(AgentMessage {
            role: AgentMessageRole::Ai,
            content: vec![ContentPart::text(text)],
            is_delta: true,
            ..AgentMessage::default()
        });
        chunk
    }

    fn text_of(chunks: &[StreamChunk]) -> String {
        chunks.iter().filter_map(model_text).collect()
    }

    /// Feed `parts` as deltas, then a done frame; the text that went out.
    fn stream(guard: &mut OutputGuard, parts: &[&str]) -> RiverbaseResult<String> {
        let mut out = Vec::new();
        for part in parts {
            out.extend(guard.admit(delta(part))?);
        }
        let mut done = StreamChunk::error_frame(String::new());
        done.chunk_type = StreamChunkType::Done;
        out.extend(guard.admit(done)?);
        assert_eq!(
            out.last().map(|c| c.chunk_type),
            Some(StreamChunkType::Done)
        );
        Ok(text_of(&out))
    }

    #[test]
    fn clean_text_comes_out_whole_and_in_order() {
        let mut g = guard(8);
        let parts = ["The quick ", "brown fox ", "jumps over ", "the lazy dog."];
        assert_eq!(
            stream(&mut g, &parts).unwrap(),
            "The quick brown fox jumps over the lazy dog."
        );
    }

    /// An empty delta between the parts of a secret releases nothing.
    #[test]
    fn an_empty_delta_keeps_the_held_text_held() {
        let mut g = guard(256);
        let mut sent = String::new();
        let mut result = Ok(());
        for part in ["Your key is AK", "", "IAIOSFODNN7EXAMPLE, keep it safe."] {
            match g.admit(delta(part)) {
                Ok(out) => sent.push_str(&text_of(&out)),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        assert_eq!(result.unwrap_err().errcode.as_str(), "BEM-061");
        assert_eq!(sent, "");
        assert_eq!(
            g.blocked().map(|f| f.pattern_id.as_str()),
            Some("secret.aws_access_key")
        );
    }

    #[test]
    fn the_last_characters_wait_for_the_next_delta() {
        let mut g = guard(10);
        let out = g.admit(delta("0123456789abcdef")).unwrap();
        assert_eq!(text_of(&out), "012345");
        let out = g.admit(delta("ghij")).unwrap();
        assert_eq!(text_of(&out), "6789");
    }

    /// Acceptance 4: a secret split across chunks never reaches the client.
    #[test]
    fn a_secret_split_across_deltas_stops_the_stream() {
        let mut g = guard(256);
        let mut sent = String::new();
        let mut result = Ok(());
        for part in ["Your key is AK", "IAIOSFOD", "NN7EXAMPLE, keep it safe."] {
            match g.admit(delta(part)) {
                Ok(out) => sent.push_str(&text_of(&out)),
                Err(e) => {
                    result = Err(e);
                    break;
                }
            }
        }
        let err = result.unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-061");
        assert!(!sent.contains("AKIA"), "{sent}");
    }

    /// The label went out before the value came: the tail still finds it.
    #[test]
    fn a_secret_whose_label_was_released_still_blocks() {
        let mut g = guard(16);
        let first = g
            .admit(delta(&format!("password:{}", " ".repeat(20))))
            .unwrap();
        assert!(text_of(&first).starts_with("password:"));
        let err = g.admit(delta("hunter2hunter2 ok")).unwrap_err();
        assert_eq!(err.errcode.as_str(), "BEM-061");
    }

    #[test]
    fn pii_is_masked_as_it_is_released() {
        let mut g = guard(32);
        let out = stream(
            &mut g,
            &[
                "Write to jane.d",
                "oe@example.com or call ",
                "555-123-4567 today.",
            ],
        )
        .unwrap();
        assert_eq!(out, "Write to [redacted] or call [redacted] today.");
    }

    #[test]
    fn a_complete_message_is_scanned_whole() {
        let mut g = guard(32);
        let mut message = delta("mail jane@example.com");
        if let Some(m) = message.data.as_mut() {
            m.is_delta = false;
        }
        let out = g.admit(message).unwrap();
        assert_eq!(text_of(&out), "mail [redacted]");
        let mut secret = delta("AKIAIOSFODNN7EXAMPLE");
        if let Some(m) = secret.data.as_mut() {
            m.is_delta = false;
        }
        assert!(g.admit(secret).is_err());
    }

    #[test]
    fn other_frames_release_the_held_text_first() {
        let mut g = guard(64);
        assert_eq!(g.admit(delta("Let me look that up.")).unwrap().len(), 0);
        let mut tool = delta("");
        if let Some(m) = tool.data.as_mut() {
            m.msg_type = AgentMessageType::ToolResult;
            m.role = AgentMessageRole::Tool;
        }
        let out = g.admit(tool).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(text_of(&out[..1]), "Let me look that up.");
    }
}
