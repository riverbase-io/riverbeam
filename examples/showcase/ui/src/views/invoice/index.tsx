// Accounts payable's views by persona role.

import { ComponentType } from "react";
import { ViewProps } from "../../lib/showcase";
import { Clerk } from "./Clerk";
import { Controller } from "./Controller";

export const views: Record<string, ComponentType<ViewProps>> = {
  clerk: Clerk,
  controller: Controller,
};
