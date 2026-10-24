# Postmortem template

A postmortem is written by the incident commander after the incident is resolved. It is blameless. It is stored as `postmortems/<date>-<service>.md` and committed to the postmortem repository.

## Sections, in this order

1. **Summary**: two or three sentences. What broke, for how long, and what fixed it.
2. **Impact**: who was affected and how much. Use numbers from monitoring: error rate, latency, minutes, requests or orders failed.
3. **Timeline**: copied from the incident. Times in UTC, one line per event: detection, the first finding, each decision, each remediation, resolution.
4. **Root cause**: the technical cause, and why it was not caught before production. State what was found, not guesses; mark anything unconfirmed as unconfirmed.
5. **What went well**: what worked in detection, response and tooling.
6. **Action items**: each one concrete and checkable, for example "Add a load test of the payment client to the CI pipeline". Prevent the cause, shorten detection, or shorten recovery.

## Style

- Plain sentences, no blame. Write "the deploy was approved without a load test", not "X forgot to test".
- Name services, versions and times exactly.
