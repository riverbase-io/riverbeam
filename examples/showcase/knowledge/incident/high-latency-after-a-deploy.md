# Runbook: high latency after a deploy

Use this when a latency alert (p99 above the SLO) starts shortly after a deploy.

## Check first

1. Read the service's recent deploys (`recent_deploys`). Note the version, the time and the change.
2. Read the latency and error-rate series (`query_metrics`, window 1h). If the numbers were flat before the deploy time and jump right after it, the deploy is the likely cause.
3. Check that no other service has an alert that started earlier. If one has, treat that service as the cause and use its runbook.

## Mitigate: roll back first, debug later

- If the deploy and the jump line up, roll the service back to the previous version. Do not spend time debugging on a live incident while customers are affected.
- A rollback changes production, so the on-call SRE approves it before it runs.
- Add a timeline entry with the deploy, the metric that jumped and the version you are going back to.

## Confirm

- Within 10 minutes of the rollback, latency p99 and the 5xx rate should return to the numbers from before the deploy. If they do not, the deploy was not the only cause: add that to the timeline, restart the service only by the restart runbook, and tell the incident commander.
- When the metrics are normal, resolve the incident with a summary: which deploy, which change, which version it went back to.

## Afterwards

- The change that caused the incident is fixed in a new version and tested under load before it is deployed again.
- A critical incident gets a postmortem within 2 business days (see the on-call policy).
