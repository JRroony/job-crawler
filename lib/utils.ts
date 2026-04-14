import type { ExperienceLevel, JobListing, LinkStatus } from "@/lib/types";

export function cn(...values: Array<string | false | null | undefined>) {
  return values.filter(Boolean).join(" ");
}

export function formatPostedDate(value?: string) {
  if (!value) {
    return "Date unavailable";
  }

  try {
    return new Intl.DateTimeFormat("en-US", {
      dateStyle: "medium",
    }).format(new Date(value));
  } catch {
    return "Date unavailable";
  }
}

export function formatRelativeMoment(value?: string) {
  if (!value) {
    return "Never";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "Never";
  }

  const diffMs = Date.now() - date.getTime();
  const diffMinutes = Math.round(diffMs / 60_000);
  if (diffMinutes < 1) {
    return "Just now";
  }
  if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  }

  const diffHours = Math.round(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }

  const diffDays = Math.round(diffHours / 24);
  return `${diffDays}d ago`;
}

export function labelForLinkStatus(status: LinkStatus) {
  if (status === "valid") {
    return "Valid";
  }

  if (status === "invalid") {
    return "Invalid";
  }

  if (status === "stale") {
    return "Stale";
  }

  return "Unknown";
}

export function labelForExperience(level?: string) {
  if (!level) {
    return "Unspecified";
  }

  const labels: Record<ExperienceLevel, string> = {
    intern: "Intern",
    new_grad: "New grad",
    junior: "Junior",
    mid: "Mid",
    senior: "Senior",
    lead: "Lead / Manager",
    staff: "Staff",
    principal: "Principal",
  };

  return labels[level as ExperienceLevel] ?? level;
}

export function jobPostingUrl(
  job: Pick<JobListing, "sourceUrl" | "canonicalUrl" | "resolvedUrl" | "applyUrl">,
) {
  return job.sourceUrl || job.canonicalUrl || job.resolvedUrl || job.applyUrl;
}
