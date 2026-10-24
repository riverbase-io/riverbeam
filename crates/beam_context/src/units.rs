//! Atomic human/assistant and tool-call/result groups (Python `context/units.py`).
//! The composer selects or drops a unit whole, so a tool call is never split
//! from its results.

use beam_types::{AgentMessage, AgentMessagePhase, AgentMessageRole};

use crate::types::ContextItem;

fn is_tool_result(m: &AgentMessage) -> bool {
    m.role == AgentMessageRole::Tool || m.tool_call_id.is_some()
}

fn absorb_tools<'a>(
    messages: &'a [AgentMessage],
    group: &mut Vec<&'a AgentMessage>,
    mut index: usize,
) -> usize {
    while let Some(next) = messages.get(index).filter(|m| is_tool_result(m)) {
        group.push(next);
        index += 1;
    }
    index
}

/// Thinking messages are skipped; a human turn joins
/// the AI reply after it, and an AI turn with tool calls absorbs the tool
/// results that follow.
#[must_use]
pub fn history_units(messages: &[AgentMessage]) -> Vec<Vec<&AgentMessage>> {
    let mut units = Vec::new();
    let mut index = 0;
    while index < messages.len() {
        let message = &messages[index];
        if message.phase == AgentMessagePhase::Thinking {
            index += 1;
            continue;
        }
        let mut group = vec![message];
        let next = messages.get(index + 1);
        if message.role == AgentMessageRole::Human
            && next.is_some_and(|n| n.role == AgentMessageRole::Ai)
        {
            let reply = &messages[index + 1];
            group.push(reply);
            index += 2;
            if !reply.tool_calls.is_empty() {
                index = absorb_tools(messages, &mut group, index);
            }
        } else if message.role == AgentMessageRole::Ai && !message.tool_calls.is_empty() {
            index = absorb_tools(messages, &mut group, index + 1);
        } else {
            index += 1;
        }
        units.push(group);
    }
    units
}

/// Items by `unit_id` (else id), in first-seen order.
#[must_use]
pub fn group_items(items: &[ContextItem]) -> Vec<Vec<ContextItem>> {
    let mut order: Vec<&str> = Vec::new();
    let mut groups: Vec<Vec<ContextItem>> = Vec::new();
    for item in items {
        let key = item.unit_id.as_deref().unwrap_or(&item.id);
        if let Some(i) = order.iter().position(|k| *k == key) {
            groups[i].push(item.clone());
        } else {
            order.push(key);
            groups.push(vec![item.clone()]);
        }
    }
    groups
}

/// The tokens of a unit's items.
#[must_use]
pub fn unit_cost(items: &[ContextItem]) -> u64 {
    items.iter().map(|i| i.token_estimate).sum()
}
