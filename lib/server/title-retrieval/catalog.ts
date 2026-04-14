import "server-only";

import {
  containsNormalizedPhrase,
  normalizeTitleText,
} from "@/lib/server/title-retrieval/normalize";
import type {
  TitleAliasDefinition,
  TitleConceptDefinition,
  TitleConceptId,
  TitleFamilyDefinition,
  TitleRoleGroup,
} from "@/lib/server/title-retrieval/types";

export const titleFamilyCatalog: readonly TitleFamilyDefinition[] = [
  {
    id: "software_engineering",
    label: "Software Engineering",
    roleGroup: "engineering",
    anchorTitle: "software engineer",
    broadDiscoveryQueries: [
      "software engineer",
      "software developer",
      "software development engineer",
      "backend engineer",
      "frontend engineer",
      "full stack engineer",
      "application developer",
      "application engineer",
      "application software engineer",
      "web application developer",
      "platform engineer",
      "mobile engineer",
      "java developer",
      "api developer",
      "server engineer",
      "service engineer",
      "distributed systems engineer",
      "swe",
    ],
    positiveKeywords: [
      "software",
      "backend",
      "frontend",
      "full stack",
      "platform",
      "mobile",
      "ios",
      "android",
      "java",
      "web",
      "application",
      "api",
      "server",
      "distributed",
      "devops",
      "sre",
      "infrastructure",
      "cloud",
      "systems",
    ],
    negativeKeywords: [
      "sales",
      "support",
      "customer",
      "recruit",
      "talent",
      "writer",
      "documentation",
      "quality",
      "qa",
      "test",
      "data engineer",
      "analytics engineer",
      "data platform",
      "data warehouse",
      "etl",
      "dbt",
      "data scientist",
      "marketing",
      "finance",
    ],
  },
  {
    id: "data_engineering",
    label: "Data Engineering",
    roleGroup: "engineering",
    anchorTitle: "data engineer",
    broadDiscoveryQueries: [
      "data engineer",
      "analytics engineer",
      "data platform engineer",
      "etl engineer",
      "data warehouse engineer",
      "data pipeline engineer",
    ],
    positiveKeywords: [
      "data engineer",
      "analytics engineer",
      "data platform",
      "data warehouse",
      "data pipeline",
      "etl",
      "elt",
      "dbt",
      "spark",
      "bigquery",
      "warehouse",
      "lakehouse",
    ],
    negativeKeywords: [
      "software engineer",
      "frontend",
      "backend",
      "full stack",
      "mobile",
      "sales",
      "support",
      "recruit",
      "writer",
      "product manager",
      "program manager",
      "analyst",
    ],
  },
  {
    id: "data_analytics",
    label: "Data Analytics",
    roleGroup: "analysis",
    anchorTitle: "data analyst",
    broadDiscoveryQueries: [
      "data analyst",
      "business analyst",
      "business intelligence analyst",
      "reporting analyst",
      "insights analyst",
      "operations analyst",
      "bi analyst",
    ],
    positiveKeywords: [
      "data",
      "analytics",
      "business intelligence",
      "reporting",
      "insights",
      "decision",
    ],
    negativeKeywords: [
      "engineer",
      "developer",
      "platform",
      "pipeline",
      "etl",
      "dbt",
    ],
  },
  {
    id: "product",
    label: "Product",
    roleGroup: "management",
    anchorTitle: "product manager",
    broadDiscoveryQueries: [
      "product manager",
      "technical product manager",
      "growth product manager",
      "associate product manager",
      "apm",
    ],
    positiveKeywords: ["product", "growth", "apm"],
  },
  {
    id: "program_management",
    label: "Program Management",
    roleGroup: "management",
    anchorTitle: "program manager",
    broadDiscoveryQueries: [
      "program manager",
      "technical program manager",
      "delivery manager",
      "implementation manager",
      "tpm",
    ],
    positiveKeywords: ["program", "delivery", "implementation", "tpm"],
  },
  {
    id: "recruiting",
    label: "Recruiting",
    roleGroup: "recruiting",
    anchorTitle: "recruiter",
    broadDiscoveryQueries: [
      "recruiter",
      "technical recruiter",
      "talent acquisition partner",
      "talent acquisition",
      "sourcer",
    ],
    positiveKeywords: [
      "recruit",
      "recruiter",
      "sourcer",
      "sourcing",
      "talent acquisition",
      "talent",
    ],
  },
  {
    id: "quality_assurance",
    label: "Quality Assurance",
    roleGroup: "quality",
    anchorTitle: "qa engineer",
    broadDiscoveryQueries: [
      "qa engineer",
      "quality assurance engineer",
      "test engineer",
      "software engineer in test",
      "sdet",
    ],
    positiveKeywords: [
      "qa",
      "quality assurance",
      "quality",
      "test",
      "testing",
      "sdet",
      "automation",
      "validation",
    ],
  },
  {
    id: "writing_documentation",
    label: "Writing and Documentation",
    roleGroup: "writing",
    anchorTitle: "technical writer",
    broadDiscoveryQueries: [
      "technical writer",
      "documentation writer",
      "documentation specialist",
      "api writer",
    ],
    positiveKeywords: [
      "writer",
      "writing",
      "documentation",
      "docs",
      "api docs",
      "content",
    ],
  },
  {
    id: "support",
    label: "Support",
    roleGroup: "support",
    anchorTitle: "support engineer",
    broadDiscoveryQueries: [
      "support engineer",
      "technical support engineer",
      "customer support engineer",
      "support specialist",
    ],
    positiveKeywords: [
      "support",
      "customer support",
      "technical support",
      "application support",
    ],
  },
  {
    id: "sales",
    label: "Sales",
    roleGroup: "sales",
    anchorTitle: "sales engineer",
    broadDiscoveryQueries: [
      "sales engineer",
      "solutions engineer",
      "pre sales engineer",
      "solutions consultant",
    ],
    positiveKeywords: ["sales", "solutions", "pre sales", "presales"],
  },
  {
    id: "operations",
    label: "Operations",
    roleGroup: "operations",
    anchorTitle: "operations analyst",
    broadDiscoveryQueries: [
      "operations analyst",
      "business operations analyst",
      "operations manager",
      "operations coordinator",
    ],
    positiveKeywords: ["operations", "ops", "coordinator", "specialist"],
  },
] satisfies readonly TitleFamilyDefinition[];

export const titleConceptCatalog: readonly TitleConceptDefinition[] = [
  {
    id: "software_engineer",
    family: "software_engineering",
    canonicalTitle: "software engineer",
    aliases: [
      "software developer",
      "software development engineer",
      "software engineering",
      "application developer",
      "applications developer",
      "application engineer",
      "applications engineer",
      "application software engineer",
      "web application developer",
    ],
    abbreviations: ["swe", "sde"],
    tokenSynonyms: [
      ["software", "application", "applications"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: [
      "backend_engineer",
      "frontend_engineer",
      "full_stack_engineer",
      "platform_engineer",
      "mobile_engineer",
      "java_developer",
      "member_of_technical_staff",
    ],
    broadDiscoveryQueries: [
      "software engineer",
      "software developer",
      "software development engineer",
      "application developer",
      "application engineer",
      "application software engineer",
      "web application developer",
      "backend engineer",
      "frontend engineer",
      "full stack engineer",
      "platform engineer",
      "mobile engineer",
      "java developer",
      "member of technical staff",
      "mts",
      "swe",
    ],
  },
  {
    id: "backend_engineer",
    family: "software_engineering",
    canonicalTitle: "backend engineer",
    aliases: [
      "back end engineer",
      "backend developer",
      "back end developer",
      "backend engineering",
      "back end engineering",
      "server side engineer",
      "api engineer",
      "api developer",
      "server engineer",
      "backend software engineer",
    ],
    tokenSynonyms: [
      ["backend", "server", "api"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: [
      "software_engineer",
      "full_stack_engineer",
      "platform_engineer",
      "java_developer",
    ],
    broadDiscoveryQueries: [
      "backend engineer",
      "backend developer",
      "server side engineer",
      "api engineer",
      "api developer",
      "server engineer",
      "software engineer",
    ],
  },
  {
    id: "frontend_engineer",
    family: "software_engineering",
    canonicalTitle: "frontend engineer",
    aliases: [
      "front end engineer",
      "frontend developer",
      "front end developer",
      "frontend engineering",
      "front end engineering",
      "ui engineer",
      "web engineer",
      "frontend software engineer",
    ],
    tokenSynonyms: [
      ["frontend", "ui", "web"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: [
      "software_engineer",
      "full_stack_engineer",
      "mobile_engineer",
    ],
    broadDiscoveryQueries: [
      "frontend engineer",
      "frontend developer",
      "ui engineer",
      "web engineer",
      "software engineer",
    ],
  },
  {
    id: "full_stack_engineer",
    family: "software_engineering",
    canonicalTitle: "full stack engineer",
    aliases: [
      "fullstack engineer",
      "full stack developer",
      "fullstack developer",
      "full stack engineering",
      "fullstack engineering",
      "full stack software engineer",
    ],
    tokenSynonyms: [
      ["full stack", "fullstack"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: [
      "software_engineer",
      "backend_engineer",
      "frontend_engineer",
    ],
    broadDiscoveryQueries: [
      "full stack engineer",
      "fullstack engineer",
      "full stack developer",
      "backend engineer",
      "frontend engineer",
      "software engineer",
    ],
  },
  {
    id: "platform_engineer",
    family: "software_engineering",
    canonicalTitle: "platform engineer",
    aliases: [
      "platform developer",
      "platform engineering",
      "infrastructure engineer",
      "distributed systems engineer",
      "devops engineer",
      "site reliability engineer",
      "sre",
    ],
    abbreviations: ["sre"],
    tokenSynonyms: [
      ["platform", "infrastructure", "systems", "service"],
      ["engineer", "developer"],
      ["site reliability engineer", "sre"],
    ],
    adjacentConceptIds: [
      "software_engineer",
      "backend_engineer",
      "mobile_engineer",
    ],
    broadDiscoveryQueries: [
      "platform engineer",
      "infrastructure engineer",
      "distributed systems engineer",
      "devops engineer",
      "site reliability engineer",
      "service engineer",
      "software engineer",
    ],
  },
  {
    id: "mobile_engineer",
    family: "software_engineering",
    canonicalTitle: "mobile engineer",
    aliases: [
      "mobile developer",
      "mobile engineering",
      "ios engineer",
      "android engineer",
      "ios developer",
      "android developer",
    ],
    tokenSynonyms: [
      ["mobile", "ios", "android"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: ["software_engineer", "frontend_engineer"],
    broadDiscoveryQueries: [
      "mobile engineer",
      "mobile developer",
      "ios engineer",
      "android engineer",
      "software engineer",
    ],
  },
  {
    id: "java_developer",
    family: "software_engineering",
    canonicalTitle: "java developer",
    aliases: ["java engineer", "java software engineer", "java development", "jvm engineer"],
    tokenSynonyms: [
      ["java", "jvm"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: ["backend_engineer", "software_engineer"],
    broadDiscoveryQueries: [
      "java developer",
      "java engineer",
      "backend engineer",
      "software engineer",
    ],
  },
  {
    id: "member_of_technical_staff",
    family: "software_engineering",
    canonicalTitle: "member of technical staff",
    aliases: [
      "member technical staff",
      "technical staff member",
    ],
    abbreviations: ["mts"],
    tokenSynonyms: [
      ["member of technical staff", "mts"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: [
      "software_engineer",
      "backend_engineer",
      "platform_engineer",
    ],
    broadDiscoveryQueries: [
      "member of technical staff",
      "mts",
      "software engineer",
      "backend engineer",
      "platform engineer",
    ],
  },
  {
    id: "data_engineer",
    family: "data_engineering",
    canonicalTitle: "data engineer",
    aliases: [
      "etl engineer",
      "data pipeline engineer",
      "data warehouse engineer",
      "big data engineer",
      "data developer",
      "software engineer data engineering",
    ],
    tokenSynonyms: [
      ["data", "big data", "data warehouse", "data pipeline"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: ["analytics_engineer", "data_platform_engineer"],
    broadDiscoveryQueries: [
      "data engineer",
      "analytics engineer",
      "data platform engineer",
      "etl engineer",
      "data warehouse engineer",
      "data pipeline engineer",
    ],
    negativeConceptIds: [
      "software_engineer",
      "backend_engineer",
      "frontend_engineer",
      "full_stack_engineer",
      "mobile_engineer",
    ],
    negativeKeywords: [
      "frontend",
      "backend",
      "full stack",
      "mobile",
      "ios",
      "android",
      "web",
      "sales",
      "recruiter",
    ],
  },
  {
    id: "analytics_engineer",
    family: "data_engineering",
    canonicalTitle: "analytics engineer",
    aliases: [
      "bi engineer",
      "business intelligence engineer",
      "dbt engineer",
      "analytics developer",
    ],
    tokenSynonyms: [
      ["analytics", "business intelligence", "bi"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: ["data_engineer", "data_platform_engineer"],
    broadDiscoveryQueries: [
      "analytics engineer",
      "data engineer",
      "data platform engineer",
      "bi engineer",
      "dbt engineer",
    ],
    negativeConceptIds: [
      "software_engineer",
      "backend_engineer",
      "frontend_engineer",
    ],
    negativeKeywords: ["frontend", "backend", "full stack", "mobile", "sales"],
  },
  {
    id: "data_platform_engineer",
    family: "data_engineering",
    canonicalTitle: "data platform engineer",
    aliases: [
      "data infrastructure engineer",
      "data systems engineer",
      "data platform developer",
      "software engineer data platform",
      "platform engineer data",
    ],
    tokenSynonyms: [
      ["data platform", "data infrastructure", "data systems"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: ["data_engineer", "analytics_engineer"],
    broadDiscoveryQueries: [
      "data platform engineer",
      "data engineer",
      "analytics engineer",
      "data infrastructure engineer",
      "data systems engineer",
    ],
    negativeConceptIds: [
      "software_engineer",
      "platform_engineer",
      "backend_engineer",
    ],
    negativeKeywords: ["frontend", "mobile", "sales", "support"],
  },
  {
    id: "data_analyst",
    family: "data_analytics",
    canonicalTitle: "data analyst",
    aliases: [
      "analytics analyst",
      "business intelligence analyst",
      "reporting analyst",
      "insights analyst",
      "product analyst",
      "decision scientist",
      "analytics consultant",
    ],
    abbreviations: ["bi analyst"],
    tokenSynonyms: [
      ["data", "analytics", "insights", "reporting"],
      ["business intelligence", "bi"],
      ["analyst", "scientist"],
    ],
    adjacentConceptIds: ["business_analyst", "operations_analyst"],
    broadDiscoveryQueries: [
      "data analyst",
      "analytics analyst",
      "business intelligence analyst",
      "reporting analyst",
      "insights analyst",
      "product analyst",
      "decision scientist",
      "business analyst",
      "operations analyst",
      "bi analyst",
    ],
  },
  {
    id: "business_analyst",
    family: "data_analytics",
    canonicalTitle: "business analyst",
    aliases: [
      "business systems analyst",
      "systems analyst",
      "process analyst",
      "business process analyst",
      "functional analyst",
    ],
    tokenSynonyms: [
      ["business", "process", "functional", "systems"],
      ["analyst", "consultant"],
    ],
    adjacentConceptIds: ["data_analyst", "operations_analyst"],
    broadDiscoveryQueries: [
      "business analyst",
      "business systems analyst",
      "systems analyst",
      "business process analyst",
      "functional analyst",
      "data analyst",
      "operations analyst",
    ],
  },
  {
    id: "operations_analyst",
    family: "operations",
    canonicalTitle: "operations analyst",
    aliases: ["business operations analyst", "ops analyst"],
    adjacentConceptIds: ["business_analyst", "data_analyst", "operations_manager"],
    broadDiscoveryQueries: [
      "operations analyst",
      "business operations analyst",
      "business analyst",
      "data analyst",
    ],
  },
  {
    id: "product_manager",
    family: "product",
    canonicalTitle: "product manager",
    aliases: ["product owner", "product lead", "product mgr"],
    tokenSynonyms: [
      ["product", "platform", "growth"],
      ["manager", "owner", "lead"],
    ],
    adjacentConceptIds: [
      "technical_product_manager",
      "growth_product_manager",
      "associate_product_manager",
    ],
    broadDiscoveryQueries: [
      "product manager",
      "technical product manager",
      "growth product manager",
      "associate product manager",
      "apm",
    ],
  },
  {
    id: "technical_product_manager",
    family: "product",
    canonicalTitle: "technical product manager",
    aliases: ["platform product manager", "technical product owner", "technical product mgr"],
    tokenSynonyms: [
      ["technical", "platform"],
      ["product", "pm"],
      ["manager", "owner"],
    ],
    adjacentConceptIds: [
      "product_manager",
      "growth_product_manager",
      "associate_product_manager",
    ],
    broadDiscoveryQueries: [
      "technical product manager",
      "product manager",
      "platform product manager",
      "growth product manager",
    ],
  },
  {
    id: "growth_product_manager",
    family: "product",
    canonicalTitle: "growth product manager",
    aliases: ["growth pm", "growth product lead"],
    tokenSynonyms: [
      ["growth", "product"],
      ["manager", "lead", "pm"],
    ],
    adjacentConceptIds: [
      "product_manager",
      "technical_product_manager",
      "associate_product_manager",
    ],
    broadDiscoveryQueries: [
      "growth product manager",
      "growth pm",
      "product manager",
      "technical product manager",
    ],
  },
  {
    id: "associate_product_manager",
    family: "product",
    canonicalTitle: "associate product manager",
    abbreviations: ["apm"],
    tokenSynonyms: [
      ["associate", "apm"],
      ["product", "pm"],
      ["manager", "owner"],
    ],
    adjacentConceptIds: [
      "product_manager",
      "technical_product_manager",
      "growth_product_manager",
    ],
    broadDiscoveryQueries: [
      "associate product manager",
      "apm",
      "product manager",
      "technical product manager",
    ],
  },
  {
    id: "program_manager",
    family: "program_management",
    canonicalTitle: "program manager",
    aliases: ["program lead", "program management"],
    tokenSynonyms: [
      ["program", "project"],
      ["manager", "lead"],
    ],
    adjacentConceptIds: [
      "technical_program_manager",
      "delivery_manager",
      "implementation_manager",
    ],
    broadDiscoveryQueries: [
      "program manager",
      "technical program manager",
      "delivery manager",
      "implementation manager",
      "tpm",
    ],
  },
  {
    id: "technical_program_manager",
    family: "program_management",
    canonicalTitle: "technical program manager",
    aliases: ["engineering program manager", "program manager technical"],
    abbreviations: ["tpm"],
    adjacentConceptIds: [
      "program_manager",
      "delivery_manager",
      "implementation_manager",
    ],
    broadDiscoveryQueries: [
      "technical program manager",
      "tpm",
      "program manager",
      "delivery manager",
      "implementation manager",
    ],
  },
  {
    id: "delivery_manager",
    family: "program_management",
    canonicalTitle: "delivery manager",
    aliases: ["delivery lead", "delivery program manager"],
    adjacentConceptIds: [
      "program_manager",
      "technical_program_manager",
      "implementation_manager",
    ],
    broadDiscoveryQueries: [
      "delivery manager",
      "program manager",
      "technical program manager",
      "implementation manager",
    ],
  },
  {
    id: "implementation_manager",
    family: "program_management",
    canonicalTitle: "implementation manager",
    aliases: ["implementation lead", "implementation program manager"],
    adjacentConceptIds: [
      "program_manager",
      "technical_program_manager",
      "delivery_manager",
    ],
    broadDiscoveryQueries: [
      "implementation manager",
      "program manager",
      "technical program manager",
      "delivery manager",
    ],
  },
  {
    id: "recruiter",
    family: "recruiting",
    canonicalTitle: "recruiter",
    aliases: [
      "talent acquisition partner",
      "talent acquisition recruiter",
      "talent acquisition specialist",
      "sourcer",
      "talent sourcer",
    ],
    tokenSynonyms: [
      ["recruiter", "talent acquisition", "sourcer"],
      ["partner", "specialist"],
    ],
    adjacentConceptIds: ["technical_recruiter"],
    broadDiscoveryQueries: [
      "recruiter",
      "technical recruiter",
      "talent acquisition partner",
      "talent acquisition",
      "sourcer",
    ],
  },
  {
    id: "technical_recruiter",
    family: "recruiting",
    canonicalTitle: "technical recruiter",
    aliases: ["engineering recruiter", "tech recruiter"],
    tokenSynonyms: [
      ["technical", "engineering", "tech"],
      ["recruiter", "sourcer"],
    ],
    adjacentConceptIds: ["recruiter"],
    broadDiscoveryQueries: [
      "technical recruiter",
      "recruiter",
      "engineering recruiter",
      "talent acquisition",
      "sourcer",
    ],
  },
  {
    id: "technical_writer",
    family: "writing_documentation",
    canonicalTitle: "technical writer",
    aliases: [
      "documentation writer",
      "documentation specialist",
      "api writer",
      "developer documentation writer",
      "docs writer",
    ],
    tokenSynonyms: [
      ["technical", "developer", "api", "documentation"],
      ["writer", "specialist"],
    ],
    broadDiscoveryQueries: [
      "technical writer",
      "documentation writer",
      "documentation specialist",
      "api writer",
    ],
  },
  {
    id: "qa_engineer",
    family: "quality_assurance",
    canonicalTitle: "qa engineer",
    aliases: [
      "quality assurance engineer",
      "quality engineer",
      "qa analyst",
      "test automation engineer",
    ],
    tokenSynonyms: [
      ["qa", "quality", "quality assurance", "test"],
      ["engineer", "analyst"],
    ],
    adjacentConceptIds: ["test_engineer", "software_engineer_in_test"],
    broadDiscoveryQueries: [
      "qa engineer",
      "quality assurance engineer",
      "quality engineer",
      "test engineer",
      "software engineer in test",
      "sdet",
    ],
  },
  {
    id: "test_engineer",
    family: "quality_assurance",
    canonicalTitle: "test engineer",
    aliases: ["quality assurance tester", "validation engineer", "software tester"],
    tokenSynonyms: [
      ["test", "testing", "validation", "quality"],
      ["engineer", "tester"],
    ],
    adjacentConceptIds: ["qa_engineer", "software_engineer_in_test"],
    broadDiscoveryQueries: [
      "test engineer",
      "qa engineer",
      "quality assurance engineer",
      "software engineer in test",
      "sdet",
    ],
  },
  {
    id: "software_engineer_in_test",
    family: "quality_assurance",
    canonicalTitle: "software engineer in test",
    aliases: [
      "software developer in test",
      "software development engineer in test",
    ],
    abbreviations: ["sdet"],
    tokenSynonyms: [
      ["software engineer in test", "software developer in test", "sdet"],
      ["engineer", "developer"],
    ],
    adjacentConceptIds: ["qa_engineer", "test_engineer"],
    broadDiscoveryQueries: [
      "software engineer in test",
      "software developer in test",
      "sdet",
      "qa engineer",
      "test engineer",
    ],
  },
  {
    id: "support_engineer",
    family: "support",
    canonicalTitle: "support engineer",
    aliases: [
      "technical support engineer",
      "customer support engineer",
      "application support engineer",
      "support specialist",
    ],
    tokenSynonyms: [
      ["support", "technical support", "customer support", "application support"],
      ["engineer", "specialist"],
    ],
    broadDiscoveryQueries: [
      "support engineer",
      "technical support engineer",
      "customer support engineer",
      "support specialist",
    ],
  },
  {
    id: "sales_engineer",
    family: "sales",
    canonicalTitle: "sales engineer",
    aliases: [
      "solutions engineer",
      "pre sales engineer",
      "presales engineer",
      "solutions consultant",
    ],
    tokenSynonyms: [
      ["sales", "solutions", "pre sales"],
      ["engineer", "consultant"],
    ],
    broadDiscoveryQueries: [
      "sales engineer",
      "solutions engineer",
      "pre sales engineer",
      "solutions consultant",
    ],
  },
  {
    id: "operations_manager",
    family: "operations",
    canonicalTitle: "operations manager",
    aliases: [
      "business operations manager",
      "ops manager",
      "operations coordinator",
    ],
    tokenSynonyms: [
      ["operations", "business operations", "ops"],
      ["manager", "coordinator"],
    ],
    adjacentConceptIds: ["operations_analyst"],
    broadDiscoveryQueries: [
      "operations manager",
      "business operations manager",
      "operations coordinator",
      "operations analyst",
    ],
  },
] satisfies readonly TitleConceptDefinition[];

const roleGroupByFamily = new Map<string, TitleRoleGroup>(
  titleFamilyCatalog.map((family) => [family.id, family.roleGroup] as const),
);

const titleConceptById = new Map<TitleConceptId, TitleConceptDefinition>(
  titleConceptCatalog.map((concept) => [concept.id, concept] as const),
);

const titleFamilyById = new Map<string, TitleFamilyDefinition>(
  titleFamilyCatalog.map((family) => [family.id, family] as const),
);

const aliasKindPriority = {
  canonical: 3,
  alias: 2,
  abbreviation: 1,
} as const satisfies Record<TitleAliasDefinition["kind"], number>;

const titleAliasCatalog = titleConceptCatalog
  .flatMap((concept) => {
    const roleGroup = concept.roleGroup ?? roleGroupByFamily.get(concept.family) ?? "engineering";

    return [
      {
        conceptId: concept.id,
        family: concept.family,
        roleGroup,
        canonicalTitle: concept.canonicalTitle,
        kind: "canonical" as const,
        phrase: normalizeTitleText(concept.canonicalTitle),
      },
      ...(concept.aliases ?? []).map((phrase) => ({
        conceptId: concept.id,
        family: concept.family,
        roleGroup,
        canonicalTitle: concept.canonicalTitle,
        kind: "alias" as const,
        phrase: normalizeTitleText(phrase),
      })),
      ...(concept.abbreviations ?? []).map((phrase) => ({
        conceptId: concept.id,
        family: concept.family,
        roleGroup,
        canonicalTitle: concept.canonicalTitle,
        kind: "abbreviation" as const,
        phrase: normalizeTitleText(phrase),
      })),
    ];
  })
  .sort(
    (left, right) =>
      right.phrase.split(" ").length - left.phrase.split(" ").length ||
      right.phrase.length - left.phrase.length ||
      aliasKindPriority[right.kind] - aliasKindPriority[left.kind],
  );

export function listTitleFamilies() {
  return titleFamilyCatalog;
}

export function listTitleConcepts() {
  return titleConceptCatalog;
}

export function getTitleFamily(id?: string) {
  return id ? titleFamilyById.get(id) : undefined;
}

export function getTitleConcept(id?: TitleConceptId) {
  return id ? titleConceptById.get(id) : undefined;
}

export function getTitleFamilyRoleGroup(family?: string): TitleRoleGroup | undefined {
  return family ? roleGroupByFamily.get(family as typeof titleFamilyCatalog[number]["id"]) : undefined;
}

export function findMatchingTitleAliases(value: string) {
  const normalized = normalizeTitleText(value);
  if (!normalized) {
    return [] as TitleAliasDefinition[];
  }

  return titleAliasCatalog.filter((alias) => containsNormalizedPhrase(normalized, alias.phrase));
}

export function areTitleConceptsAdjacent(
  leftConceptId?: TitleConceptId,
  rightConceptId?: TitleConceptId,
) {
  if (!leftConceptId || !rightConceptId || leftConceptId === rightConceptId) {
    return false;
  }

  const left = getTitleConcept(leftConceptId);
  const right = getTitleConcept(rightConceptId);

  return Boolean(
    left?.adjacentConceptIds?.includes(rightConceptId) ||
      right?.adjacentConceptIds?.includes(leftConceptId),
  );
}
