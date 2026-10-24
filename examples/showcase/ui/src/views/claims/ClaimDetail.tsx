// The picked claim's detail, shared by the policyholder's and the adjuster's
// views: the facts, the estimate, the review's findings and the payout.

import { Row } from "../../lib/api";
import { ShowcaseRecord, money } from "../../lib/showcase";
import { Fields } from "../../components/Workbench";

const LABELS: [string, string][] = [
  ["policy_number", "Policy"],
  ["vehicle", "Vehicle"],
  ["loss_type", "Loss type"],
  ["loss_date", "Date of loss"],
  ["location", "Where"],
  ["description", "What happened"],
  ["other_party", "Other party"],
  ["police_report", "Police report"],
  ["drivable", "Drivable"],
  ["severity", "Severity"],
  ["estimated_repair_cost", "Repair estimate"],
];

function format(key: string, value: unknown) {
  if (key === "estimated_repair_cost") return value === undefined ? "—" : money(value);
  if (key === "drivable") return value === undefined ? "—" : value ? "yes" : "no";
  return String(value ?? "—");
}

export function ClaimDetail(props: { claim: ShowcaseRecord; /** Show the review's findings (adjusters). */ review?: boolean }) {
  const { claim } = props;
  const documents = (Array.isArray(claim.data.documents) ? claim.data.documents : []) as Row[];
  const decision = claim.data.decision as Row | undefined;
  const payout = claim.data.payout as Row | undefined;
  return (
    <>
      <h3>{claim.title}</h3>
      <Fields data={claim.data} labels={LABELS} format={format} />
      {documents.length > 0 && (
        <div className="docs">
          <div className="eyebrow">Documents</div>
          {documents.map((d, i) => {
            const read = d.extracted as Row | undefined;
            return (
              <div key={i} className="doc">
                📄 {String(d.file_name)} <span className="muted small">{String(d.document_type ?? "")}</span>
                {read?.estimated_repair_cost ? (
                  <span className="muted small">
                    {" "}
                    · read: {money(read.estimated_repair_cost)}, {String(read.severity ?? "?")}
                  </span>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
      {decision && (
        <div className="panel">
          <h3>Decision: {String(decision.outcome).replace(/_/g, " ")}</h3>
          {decision.note ? <p>{String(decision.note)}</p> : null}
          {props.review && decision.rationale ? <p className="muted">{String(decision.rationale)}</p> : null}
          {props.review && (
            <>
              <table className="grid">
                <tbody>
                  {((decision.findings as Row[]) ?? []).map((f) => (
                    <tr key={String(f.rule)}>
                      <td>{String(f.rule)}</td>
                      <td>{String(f.value)}</td>
                      <td className="muted">{String(f.limit)}</td>
                      <td>{f.pass ? "✓" : "exception"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <table className="grid">
                <tbody>
                  {((decision.signals as Row[]) ?? []).map((s) => (
                    <tr key={String(s.signal)}>
                      <td>{String(s.signal)}</td>
                      <td className="muted">{String(s.detail)}</td>
                      <td>{s.fired ? "fired" : "clear"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </div>
      )}
      {payout && (
        <div className="panel">
          <h3>Paid {money(payout.amount)}</h3>
          <div className="muted small">
            {String(payout.reference)} · estimate minus the {money(payout.deductible)} deductible
          </div>
        </div>
      )}
    </>
  );
}
