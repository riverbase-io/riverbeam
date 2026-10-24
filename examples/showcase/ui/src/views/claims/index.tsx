// The claims showcase's views by persona role.

import { ComponentType } from "react";
import { ViewProps } from "../../lib/showcase";
import { Adjuster } from "./Adjuster";
import { Policyholder } from "./Policyholder";

export const views: Record<string, ComponentType<ViewProps>> = {
  policyholder: Policyholder,
  adjuster: Adjuster,
};
