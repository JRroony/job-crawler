import "server-only";

export const collectionNames = {
  searches: "searches",
  searchSessions: "searchSessions",
  jobs: "jobs",
  crawlRuns: "crawlRuns",
  crawlControls: "crawlControls",
  crawlQueue: "crawlQueue",
  crawlSourceResults: "crawlSourceResults",
  crawlRunJobEvents: "crawlRunJobEvents",
  searchSessionJobEvents: "searchSessionJobEvents",
  indexedJobEvents: "indexedJobEvents",
  linkValidations: "linkValidations",
  sourceInventory: "sourceInventory",
} as const;
