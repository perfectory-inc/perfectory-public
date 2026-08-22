// apps/web/components/panels/complex/skeletons.tsx
"use client";
import { Skeleton } from "@gongzzang/ui";
import { useTranslations } from "next-intl";

export function ComplexLoadingSkeleton() {
  return (
    <div className="flex flex-col gap-3 p-6">
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-6 w-48" />
      <Skeleton className="h-4 w-64" />
      <Skeleton className="h-24 w-full" />
    </div>
  );
}

export function ComplexErrorCard({ error }: { error: unknown }) {
  const t = useTranslations("panels.complex");
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="p-6">
      <div className="text-[length:var(--text-body-md)] font-semibold text-[var(--color-error)]">
        {t("errors.loadFailed")}
      </div>
      <div className="mt-2 text-[length:var(--text-caption)] text-[var(--color-muted)]">{msg}</div>
    </div>
  );
}

export function ComplexEmptyCard() {
  const t = useTranslations("panels.complex");
  return <div className="p-6 text-center text-[var(--color-muted)]">{t("empty")}</div>;
}
