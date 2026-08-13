// apps/web/lib/panel/panel-entry-view.test.tsx
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import type React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PanelEntryView } from "./panel-entry-view";
import { _resetRegistryForTests, defineKind, defineView } from "./registry";
import type { PanelStackEntry } from "./types";

// Mock telemetry to avoid OTEL noise
vi.mock("./telemetry", () => ({
  reportPanelOpened: vi.fn(),
  reportUrlDecodeFailed: vi.fn(),
}));

// Mock usePanelStack to avoid Next.js navigation
vi.mock("./use-panel-stack", () => ({
  usePanelStack: () => ({
    stack: { v: 1, entries: [] },
    push: () => {},
    pop: () => {},
    truncate: () => {},
  }),
}));

afterEach(() => {
  _resetRegistryForTests();
  vi.clearAllMocks();
  vi.restoreAllMocks();
});

const ThrowingComponent = () => {
  throw new Error("registry component blew up");
};
const ErrorCard = ({ error }: { error: unknown }) => (
  <div data-testid="panel-error-card">
    ERROR: {error instanceof Error ? error.message : String(error)}
  </div>
);
const Loading = () => <div>L</div>;
const Empty = () => <div>E</div>;
const messages = {
  panel: {
    labels: {
      parcel: {
        summary: "Parcel summary",
        buildings: "Parcel buildings",
        listings: "Parcel listings",
      },
      listing: {
        summary: "Listing summary",
      },
    },
  },
};

function makeRegistry({
  summaryComponent = ThrowingComponent,
  summaryFetcher = async () => ({ ok: true }),
}: {
  summaryComponent?: React.ComponentType<{
    entry: Extract<PanelStackEntry, { kind: "parcel" }>;
    data: { ok: boolean };
  }>;
  summaryFetcher?: (id: string, signal?: AbortSignal) => Promise<{ ok: boolean }>;
} = {}) {
  defineKind({
    kind: "parcel",
    idPattern: /^\d{19}$/,
    views: {
      summary: defineView({
        component: summaryComponent,
        fetcher: summaryFetcher,
        staleTime: 1000,
        links: [],
      }),
      buildings: {
        component: () => null,
        fetcher: async () => ({ items: [] }),
        staleTime: 1000,
        links: [],
      },
      floors: {
        component: () => null,
        fetcher: async () => ({ buildings: [] }),
        staleTime: 1000,
        links: [],
      },
      listings: {
        component: () => null,
        fetcher: async () => ({
          listings: [],
          total: 0,
          page: 0,
          size: 0,
          has_next: false,
        }),
        staleTime: 1000,
        links: [],
      },
    },
    loadingComponent: Loading,
    errorComponent: ErrorCard,
    emptyComponent: Empty,
    authGate: { required: false },
    i18nNamespace: "panels.parcel",
    telemetryAttrs: () => ({}),
  });
}

function renderWithQuery(ui: React.ReactNode) {
  // disable retries so the test does not loop
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <NextIntlClientProvider locale="ko" messages={messages}>
      <QueryClientProvider client={qc}>{ui}</QueryClientProvider>
    </NextIntlClientProvider>,
  );
}

describe("PanelEntryView error handling", () => {
  // Suppress React's expected error logging for this test
  beforeEach(() => {
    vi.spyOn(console, "error").mockImplementation(() => {});
  });

  it("routes a synchronous registry render exception through the error card", async () => {
    makeRegistry();
    renderWithQuery(
      <PanelEntryView
        entry={{ kind: "parcel", id: "9999900501107370000", view: "summary" }}
        depth={1}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId("panel-error-card")).toHaveTextContent(
        "ERROR: registry component blew up",
      );
    });
  });

  it("leaves rejected fetches on the TanStack Query error path", async () => {
    makeRegistry({
      summaryComponent: () => <div>registry view rendered</div>,
      summaryFetcher: async () => {
        throw new Error("panel fetch failed");
      },
    });

    renderWithQuery(
      <PanelEntryView
        entry={{ kind: "parcel", id: "9999900501107370000", view: "summary" }}
        depth={1}
      />,
    );

    await waitFor(() => {
      expect(screen.getByTestId("panel-error-card")).toHaveTextContent("ERROR: panel fetch failed");
    });
    expect(screen.queryByText("registry view rendered")).not.toBeInTheDocument();
  });
});
