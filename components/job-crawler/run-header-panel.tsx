"use client";

type RunHeaderPanelProps = {
  title: string;
  description: string;
  badges: string[];
  platformScope: string;
  validationMode: string;
  experienceMode: string;
  includeUnspecified: boolean;
  updatedLabel: string;
};

export function RunHeaderPanel(props: RunHeaderPanelProps) {
  const metadata = [
    {
      label: "Platform scope",
      value: props.platformScope,
    },
    {
      label: "Validation mode",
      value: props.validationMode,
    },
    {
      label: "Experience mode",
      value: props.experienceMode,
    },
    {
      label: "Unspecified levels",
      value: props.includeUnspecified ? "Included" : "Excluded",
    },
    {
      label: "Last updated",
      value: props.updatedLabel,
    },
  ];

  return (
    <section className="rounded-[28px] border border-ink/10 bg-white/88 p-5 shadow-soft backdrop-blur sm:p-6">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="max-w-3xl">
          <p className="font-mono text-xs uppercase tracking-[0.24em] text-ember">
            Active search
          </p>
          <h2 className="mt-3 text-3xl font-semibold text-ink">{props.title}</h2>
          <p className="mt-3 max-w-2xl text-sm leading-7 text-slate sm:text-base">
            {props.description}
          </p>
        </div>

        <div className="rounded-[24px] border border-ink/10 bg-[rgba(244,239,230,0.72)] px-4 py-4 text-sm leading-6 text-slate xl:max-w-sm">
          This run summary reflects the exact scope that was requested. Use it to confirm the
          active provider families, experience policy, and validation behavior before judging the
          saved result set.
        </div>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
        {metadata.map((item) => (
          <MetaCard key={item.label} label={item.label} value={item.value} />
        ))}
      </div>

      <div className="mt-5 flex flex-wrap gap-2">
        {props.badges.map((badge) => (
          <span
            key={badge}
            className="rounded-full border border-ink/10 bg-sand/55 px-3 py-1 text-xs font-medium text-ink/85"
          >
            {badge}
          </span>
        ))}
      </div>
    </section>
  );
}

function MetaCard(props: { label: string; value: string }) {
  return (
    <div className="rounded-[22px] border border-ink/10 bg-sand/45 px-4 py-4">
      <div className="font-mono text-[11px] uppercase tracking-[0.18em] text-slate">
        {props.label}
      </div>
      <div className="mt-2 text-sm font-medium leading-6 text-ink">{props.value}</div>
    </div>
  );
}
