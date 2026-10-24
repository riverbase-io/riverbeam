# Runbook: elevated 5xx

Use this when the 5xx rate of a service is above 1% for more than 5 minutes.

## Find the shape

1. Read the error-rate series (`query_metrics`, metric `error_rate`). A step change at one moment points to a deploy or a configuration change. A slow climb points to load or a leak.
2. Read the service's recent deploys (`recent_deploys`). A deploy within 30 minutes before the step is the first suspect: follow the runbook "high latency after a deploy".
3. Read latency, CPU and requests per second for the same window. Errors with high latency usually mean the service is waiting on something (a database, a payment provider, a connection pool). Errors with high CPU usually mean it is out of capacity.

## Mitigate

- Bad deploy: roll back to the previous version (approval required).
- Out of capacity with no deploy: ask the incident commander before changing capacity.
- One bad instance among healthy ones: restart only that instance, by the runbook "restart a service safely".
- Do not restart every instance of a service at once.

## Record

- Add a timeline entry for each finding and each action, with the time and the numbers you saw.
- Tell the incident commander when the error rate is back under 1%, then resolve the incident.
