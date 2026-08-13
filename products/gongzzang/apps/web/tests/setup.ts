import "@testing-library/jest-dom/vitest";

// Provide required SP6-i env vars so static imports of @/lib/env do not throw.
// Tests that exercise env validation use vi.resetModules() + dynamic import()
// and set / delete these vars in their own beforeEach/it scope.
process.env.ZITADEL_ISSUER = "http://localhost:8443";
process.env.ZITADEL_CLIENT_ID = "test-client";
process.env.ZITADEL_AUDIENCE = "test-client";
process.env.ZITADEL_REDIRECT_URI = "http://localhost:3000/api/auth/callback";
// Respect an externally provided REDIS_URL so the integration lane can target
// a non-default port (e.g. Windows reserves 6379 in some configurations).
process.env.REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
process.env.SESSION_SECRET = "test-secret-placeholder-32-chars-x";
process.env.NEXT_PUBLIC_NAVER_MAPS_CLIENT_ID = "test-naver-client";
process.env.FOUNDATION_PLATFORM_WEBHOOK_SECRET = "test-foundation-platform-webhook-secret-32-valid";
