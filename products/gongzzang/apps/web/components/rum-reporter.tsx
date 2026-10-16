// apps/web/components/rum-reporter.tsx
"use client";
import { useReportWebVitals } from "next/web-vitals";
import { useEffect } from "react";
import { flushRum, recordWebVital } from "@/lib/rum";

/** Root ADR-0255: Core Web Vitals into the real-user beacon, sent when the page is hidden. */
export function RumReporter() {
  useReportWebVitals((metric) => recordWebVital(metric.name, metric.value));
  // Declared after useReportWebVitals, so web-vitals' own hide listeners run first and their
  // final LCP/CLS/INP values are queued before this one sends.
  useEffect(() => {
    const onHide = () => {
      if (document.visibilityState === "hidden") flushRum();
    };
    addEventListener("visibilitychange", onHide);
    return () => removeEventListener("visibilitychange", onHide);
  }, []);
  return null;
}
