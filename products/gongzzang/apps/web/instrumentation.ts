// SP6-i: OpenTelemetry SDK init.
// SP-Obs T5 (Rust backend): Sentry init 적용 -- backend panic/error capture
// production 으로 routing. _sentry_guard 가 main lifetime 동안 유지.
//
// SP-Obs FU (frontend Sentry, 별도 commit): @sentry/nextjs dep 추가 + 본
// register 안에서 dynamic import + Sentry.init({ dsn: NEXT_PUBLIC_SENTRY_DSN,
// release, environment, ... }). 1차 = OTLP + backend Sentry 만. frontend
// Sentry 는 SP8 IaC 가 DSN 주입 + source map upload CI step + release tagging
// 같이.

export async function register() {
  // The read-only preview (lib/preview.ts) has no collector. The flag is spelled out, not called, so
  // the bundler folds it and drops the SDK (gRPC included) to fit the free Worker size.
  if (
    process.env.NEXT_RUNTIME === "nodejs" &&
    process.env.NEXT_PUBLIC_GONGZZANG_READ_ONLY_PREVIEW !== "1"
  ) {
    const { NodeSDK } = await import("@opentelemetry/sdk-node");
    const { FetchInstrumentation } = await import("@opentelemetry/instrumentation-fetch");
    const sdk = new NodeSDK({
      serviceName: "gongzzang-web",
      instrumentations: [new FetchInstrumentation()],
    });
    sdk.start();
  }
}
