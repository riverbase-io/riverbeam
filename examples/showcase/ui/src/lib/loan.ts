// The loan desk's own API (`/api/loan/…`): personas, programs, applications,
// documents, legal holds, erasure and packets.

import { Api, Row } from "./api";

export type Role = "borrower" | "officer" | "admin";

export type Persona = {
  key: string;
  user_id: string;
  name: string;
  role: Role;
  plan: string;
  roles: string[];
};

export type Program = {
  id: string;
  name: string;
  summary: string;
  min_credit_score: number;
  max_ltv_pct: number;
  max_dti_pct: number;
  min_down_payment_pct: number;
  max_loan: number;
};

export type Application = {
  id: string;
  owner: string;
  org?: string;
  program_id: string;
  status: "draft" | "submitted" | "precheck" | "approved" | "referred" | "declined";
  fields: Row;
  documents: Row[];
  decision?: Row | null;
  held: boolean;
  created_at: string;
  updated_at: string;
  missing_fields: string[];
  has_pay_stub: boolean;
};

/** The client tools the borrower view runs (src/tools.rs CLIENT_TOOLS). */
export const BORROWER_CLIENT_TOOLS = [
  "fill_form_fields",
  "highlight_field",
  "navigate_step",
  "show_program_comparison",
  "request_input",
  "request_document",
];

export async function personas(): Promise<{ org: string; personas: Persona[] }> {
  const res = await fetch("/api/loan/personas");
  if (!res.ok) throw new Error(`personas ${res.status}`);
  return res.json();
}

export class Loan {
  constructor(private api: Api) {}

  programs(): Promise<Program[]> {
    return this.api.http("GET", "/api/loan/programs");
  }

  applications(): Promise<Application[]> {
    return this.api.http("GET", "/api/loan/applications");
  }

  application(id: string): Promise<Application> {
    return this.api.http("GET", `/api/loan/applications/${id}`);
  }

  create(programId: string): Promise<Application> {
    return this.api.http("POST", "/api/loan/applications", { program_id: programId });
  }

  update(id: string, fields: Row): Promise<Application> {
    return this.api.http("PATCH", `/api/loan/applications/${id}`, { fields });
  }

  addDocument(id: string, document: Row): Promise<Application> {
    return this.api.http("POST", `/api/loan/applications/${id}/documents`, document);
  }

  hold(id: string, held: boolean): Promise<Application> {
    return this.api.http("POST", `/api/loan/applications/${id}/hold`, { held });
  }

  erase(userId: string): Promise<Row> {
    return this.api.http("POST", "/api/loan/erase", { user_id: userId });
  }

  /** The eval datasets the admin runs (`loan-desk-smoke`). */
  evals(): Promise<Row[]> {
    return this.api.http("GET", "/api/loan/evals");
  }

  packet(id: string): Promise<{ files: { path: string; body: string }[]; log: string[] }> {
    return this.api.http("GET", `/api/loan/applications/${id}/packet`);
  }
}

/** The form's fields, by step. */
export const FORM: { step: "borrower" | "income" | "review"; title: string; fields: { name: string; label: string; type?: "number" | "email" | "tel" }[] }[] = [
  {
    step: "borrower",
    title: "About you",
    fields: [
      { name: "first_name", label: "First name" },
      { name: "last_name", label: "Last name" },
      { name: "email", label: "Email", type: "email" },
      { name: "phone", label: "Phone", type: "tel" },
    ],
  },
  {
    step: "income",
    title: "Income and home",
    fields: [
      { name: "employer", label: "Employer" },
      { name: "gross_monthly_income", label: "Gross monthly income ($)", type: "number" },
      { name: "monthly_debts", label: "Monthly debts ($)", type: "number" },
      { name: "purchase_price", label: "Purchase price ($)", type: "number" },
      { name: "down_payment", label: "Down payment ($)", type: "number" },
    ],
  },
  { step: "review", title: "Review and submit", fields: [] },
];

export const money = (n: unknown) =>
  typeof n === "number" ? n.toLocaleString(undefined, { style: "currency", currency: "USD", maximumFractionDigits: 0 }) : String(n ?? "—");
