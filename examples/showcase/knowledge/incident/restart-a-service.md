# Runbook: restart a service safely

A restart clears a stuck or leaking process. It does not fix the cause, and it drops in-flight requests. Use it after the other options.

## Before you restart

- Check recent deploys. After a bad deploy, roll back instead: a restart brings the same bad code back.
- Check that a restart will help: memory growing, threads or connections stuck, one instance unhealthy.
- Check the load: do not restart a service at the traffic peak if the incident is not critical.
- Write the reason in the timeline before you ask for the restart.

## How

- A restart changes production, so the on-call SRE approves it before it runs.
- Restart instances one at a time, with a pause long enough for the restarted instance to pass its health check. Never restart all instances at once.
- For a stateful service (a database, a queue), do not restart without the incident commander's approval.

## After

- Watch latency, error rate and memory for 10 minutes. If the problem returns within an hour, the restart was only a pause: say so in the timeline and raise the severity discussion with the incident commander.
- Resolve the incident only when the cause is understood or an action item to find it is written down.
