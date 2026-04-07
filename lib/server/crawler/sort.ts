import "server-only";

import { getTitleMatchResult } from "@/lib/server/crawler/helpers";
import type { JobListing } from "@/lib/types";

type SortableJob = Pick<JobListing, "postedAt" | "sourcePlatform" | "title">;

export function sortJobs<T extends SortableJob>(jobs: T[], titleQuery?: string) {
  return [...jobs].sort((left, right) => {
    const titleRelevanceComparison = compareTitleRelevance(left, right, titleQuery);
    if (titleRelevanceComparison !== 0) {
      return titleRelevanceComparison;
    }

    if (left.postedAt && right.postedAt && left.postedAt !== right.postedAt) {
      return left.postedAt > right.postedAt ? -1 : 1;
    }

    if (left.postedAt && !right.postedAt) {
      return -1;
    }

    if (!left.postedAt && right.postedAt) {
      return 1;
    }

    const sourceComparison = left.sourcePlatform.localeCompare(right.sourcePlatform);
    if (sourceComparison !== 0) {
      return sourceComparison;
    }

    return left.title.localeCompare(right.title);
  });
}

function compareTitleRelevance<T extends SortableJob>(
  left: T,
  right: T,
  titleQuery?: string,
) {
  const normalizedQuery = titleQuery?.trim();
  if (!normalizedQuery) {
    return 0;
  }

  const leftScore = getTitleMatchResult(left.title, normalizedQuery).score;
  const rightScore = getTitleMatchResult(right.title, normalizedQuery).score;
  return rightScore - leftScore;
}
