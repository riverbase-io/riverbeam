// The support desk's views by persona role.

import { ComponentType } from "react";
import { ViewProps } from "../../lib/showcase";
import { Agent } from "./Agent";
import { Customer } from "./Customer";

export const views: Record<string, ComponentType<ViewProps>> = {
  customer: Customer,
  agent: Agent,
};
