// The loan desk's views by persona role.

import { ComponentType } from "react";
import { ViewProps } from "../../lib/showcase";
import { Admin } from "./Admin";
import { Borrower } from "./Borrower";
import { Officer } from "./Officer";

const adapt =
  (View: typeof Borrower): ComponentType<ViewProps> =>
  (p) => <View api={p.api} rtc={p.rtc} agents={p.agents} userId={p.persona.user_id} onStatus={p.onStatus} />;

export const views: Record<string, ComponentType<ViewProps>> = {
  borrower: adapt(Borrower),
  officer: adapt(Officer),
  admin: adapt(Admin),
};
