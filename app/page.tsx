import { JobCrawlerApp } from "@/components/job-crawler-app";
import { listRecentSearches } from "@/lib/server/search/service";

export default async function Page() {
  try {
    const recentSearches = await listRecentSearches();
    return <JobCrawlerApp initialSearches={recentSearches} />;
  } catch (error) {
    return (
      <JobCrawlerApp
        initialSearches={[]}
        initialError={
          error instanceof Error
            ? error.message
            : "The app could not reach MongoDB yet. You can still load the page and connect the database before crawling."
        }
      />
    );
  }
}
