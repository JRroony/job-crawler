import { vi } from "vitest";

process.env.MONGODB_URI = process.env.MONGODB_URI ?? "mongodb://127.0.0.1:27017/job_crawler";

vi.mock("server-only", () => ({}));
