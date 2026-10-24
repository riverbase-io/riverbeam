// Every showcase's views by persona role: the shell picks one from the
// showcase and the persona's role.

import { ComponentType } from "react";
import { ViewProps } from "../lib/showcase";
import { views as loanDesk } from "./loan_desk";
import { views as support } from "./support";
import { views as claims } from "./claims";
import { views as incident } from "./incident";
import { views as invoice } from "./invoice";

export const VIEWS: Record<string, Record<string, ComponentType<ViewProps>>> = {
  loan_desk: loanDesk,
  support,
  claims,
  incident,
  invoice,
};
