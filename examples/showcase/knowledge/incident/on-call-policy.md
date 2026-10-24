# On-call policy

## Severities

- **critical**: customers cannot complete a core action (for example checkout), or an SLO is broken by a wide margin. Respond within 5 minutes, at any hour.
- **high**: a core action is degraded for some customers, or an internal service customers depend on is down. Respond within 15 minutes.
- **warning**: a metric is heading the wrong way but customers are not hurt yet. Look at it within 1 hour in working hours.

## One incident per service

Open one incident per service. If an incident for the service is not resolved, add to its timeline instead of opening another.

## Who approves what

- The on-call SRE opens incidents, investigates, and approves rollbacks and restarts.
- A rollback or a restart never runs without that approval.
- The incident commander is named for every critical incident, takes over communication, decides on capacity changes and on restarting stateful services, and is the only one who writes the postmortem.
- Either of them can resolve an incident, once it is mitigated and the metrics are normal.

## Communication

- Critical: a status update every 15 minutes in the incident channel, even if the update is "no change". The timeline carries the same facts.
- High: an update every 30 minutes.
- Every action goes in the timeline with the time, who and what: findings, decisions, rollbacks, restarts.

## After the incident

- Critical and high incidents get a postmortem within 2 business days. It is blameless: it describes what happened and what to change, not who to blame. It follows the postmortem template.
- Action items have an owner and a due date, and are tracked until done.
