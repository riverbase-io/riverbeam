// The incident desk's views by persona role.

import { ComponentType } from "react";
import { ViewProps } from "../../lib/showcase";
import { Commander } from "./Commander";
import { Sre } from "./Sre";

export const views: Record<string, ComponentType<ViewProps>> = {
  sre: Sre,
  commander: Commander,
};
