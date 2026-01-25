export type LinkUnderstandingOutput = {
  url: string;
  text: string;
  source?: string;
};

export type LinkUnderstandingModelDecision = {
  type: "cli";
  command?: string;
  outcome: "success" | "skipped" | "failed";
  reason?: string;
};

export type LinkUnderstandingUrlDecision = {
  url: string;
  attempts: LinkUnderstandingModelDecision[];
  chosen?: LinkUnderstandingModelDecision;
};

export type LinkUnderstandingDecisionOutcome =
  | "success"
  | "skipped"
  | "disabled"
  | "scope-deny"
  | "no-links";

export type LinkUnderstandingDecision = {
  outcome: LinkUnderstandingDecisionOutcome;
  urls: LinkUnderstandingUrlDecision[];
};
