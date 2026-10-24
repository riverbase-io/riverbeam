//! Who may subscribe to a stream channel (Python `stream/rtc.py`
//! `BeamStreamRTCBridge`, and the replay route's check in `stream/sse.py`).

use uuid::Uuid;

/// The RTC bridge's channel prefix.
pub const CHANNEL_PREFIX: &str = "ws.channel.";

/// `(session_id, request_id)` when the
/// channel is `ws.channel.{session uuid}:{request uuid}` (hyphenated, any case).
#[must_use]
pub fn parse_beam_stream_channel(channel: &str) -> Option<(Uuid, Uuid)> {
    let key = channel.strip_prefix(CHANNEL_PREFIX)?;
    let (session, request) = key.split_once(':')?;
    let hyphenated = |s: &str| s.len() == 36 && s.bytes().filter(|b| *b == b'-').count() == 4;
    if !hyphenated(session) || !hyphenated(request) {
        return None;
    }
    Some((
        Uuid::parse_str(session).ok()?,
        Uuid::parse_str(request).ok()?,
    ))
}

/// The owners of a session row.
#[derive(Debug, Clone, Copy, Default)]
pub struct SessionOwner<'a> {
    pub user_id: Option<&'a str>,
    pub profile_id: Option<&'a str>,
    pub org_id: Option<&'a str>,
}

/// The subscriber's user (the token
/// subject) or profile owns the session. A missing session is refused.
#[must_use]
pub fn may_subscribe(
    session: Option<SessionOwner<'_>>,
    user_sub: Option<&str>,
    profile_id: Option<&str>,
) -> bool {
    let Some(session) = session else { return false };
    let same = |a: Option<&str>, b: Option<&str>| matches!((a, b), (Some(a), Some(b)) if !a.is_empty() && a == b);
    same(session.user_id, user_sub) || same(session.profile_id, profile_id)
}

/// The replay route's check: an owned session is readable by
/// its user (or a principal equal to its org) and by anyone in its org. A
/// session with no owner, or no session yet, is open.
#[must_use]
pub fn may_replay(
    session: Option<SessionOwner<'_>>,
    claim_user: Option<&str>,
    claim_org: Option<&str>,
) -> bool {
    let Some(session) = session else { return true };
    let user = session.user_id.unwrap_or_default();
    let org = session.org_id.unwrap_or_default();
    if user.is_empty() && org.is_empty() {
        return true;
    }
    let claim_user = claim_user.unwrap_or_default();
    let claim_org = claim_org.unwrap_or_default();
    let same_principal = !claim_user.is_empty() && (claim_user == user || claim_user == org);
    let same_org = !claim_org.is_empty() && !org.is_empty() && claim_org == org;
    same_principal || same_org
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_uuid_channels() {
        let (s, r) = (Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(
            parse_beam_stream_channel(&format!("ws.channel.{s}:{r}")),
            Some((s, r))
        );
        let upper = format!("ws.channel.{}:{}", s.to_string().to_uppercase(), r);
        assert_eq!(parse_beam_stream_channel(&upper), Some((s, r)));
        assert_eq!(
            parse_beam_stream_channel(&format!("ws.channel.{}:{r}", s.simple())),
            None
        );
        assert_eq!(parse_beam_stream_channel(&format!("other.{s}:{r}")), None);
        assert_eq!(parse_beam_stream_channel(&format!("ws.channel.{s}")), None);
    }

    /// Another user's channel is refused.
    #[test]
    fn only_the_owner_subscribes() {
        let session = Some(SessionOwner {
            user_id: Some("u1"),
            profile_id: Some("p1"),
            org_id: None,
        });
        assert!(may_subscribe(session, Some("u1"), None));
        assert!(may_subscribe(session, None, Some("p1")));
        assert!(!may_subscribe(session, Some("u2"), Some("p2")));
        assert!(!may_subscribe(None, Some("u1"), None));
        assert!(!may_subscribe(
            Some(SessionOwner::default()),
            Some(""),
            None
        ));
    }

    #[test]
    fn replay_is_open_to_the_owner_and_its_org() {
        let session = Some(SessionOwner {
            user_id: Some("u1"),
            profile_id: None,
            org_id: Some("o1"),
        });
        assert!(may_replay(session, Some("u1"), None));
        assert!(may_replay(session, Some("u2"), Some("o1")));
        assert!(!may_replay(session, Some("u2"), Some("o2")));
        assert!(may_replay(Some(SessionOwner::default()), None, None));
        assert!(may_replay(None, None, None));
    }
}
