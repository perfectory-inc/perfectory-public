import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { DawneerClient, loadSession, signOut } from "./api/client";
import { MENU, movedTo, resolveRoute } from "./lib/routes";
import { ComplexesPage } from "./pages/ComplexesPage";
import { FlowPage } from "./pages/FlowPage";
import { ReviewItemPage } from "./pages/ReviewItemPage";
import { ReviewListPage } from "./pages/ReviewListPage";
import { TilesPage } from "./pages/TilesPage";

function useHashRoute(): string {
  const [hash, setHash] = useState(window.location.hash);
  useEffect(() => {
    const onChange = () => setHash(window.location.hash);
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  const path = hash.replace(/^#/, "") || "/";
  // An old address (운영 현황, 데이터 카탈로그) is rewritten to where it lives now, without a history entry.
  const moved = movedTo(path);
  useEffect(() => {
    if (moved) window.history.replaceState(null, "", `#${moved}`);
  }, [moved]);
  return moved ?? path;
}

export function App() {
  const session = useQuery({ queryKey: ["session"], queryFn: () => loadSession() });
  const route = useHashRoute();
  const client = useMemo(() => (session.data ? new DawneerClient(session.data) : null), [session.data]);

  if (session.isPending) return <p className="p-8 text-slate-500">불러오는 중…</p>;
  if (session.isError) return <p className="p-8 text-red-700">서버에 연결할 수 없습니다: {session.error.message}</p>;
  if (!session.data || !client) {
    return (
      <main className="mx-auto mt-32 max-w-sm rounded-xl border border-slate-200 bg-white p-8 text-center shadow-sm">
        <h1 className="text-2xl font-semibold">더니어</h1>
        <p className="mt-2 text-sm text-slate-500">직원 통합 콘솔</p>
        <a
          href="/auth/login"
          className="mt-6 inline-block w-full rounded-lg bg-slate-900 px-4 py-2 text-white hover:bg-slate-700"
        >
          로그인
        </a>
      </main>
    );
  }

  const page = resolveRoute(route);
  const current = session.data;
  const onSignOut = async () => {
    window.location.href = await signOut(current);
  };

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-[1600px] items-center justify-between px-6 py-3">
          <nav className="flex items-center gap-6">
            <a href="#/" className="text-lg font-semibold">
              더니어
            </a>
            {MENU.map((item) => (
              <a
                key={item.href}
                href={item.href}
                className={`text-sm hover:text-slate-900 ${(item.page as readonly string[]).includes(page.page) ? "font-semibold text-slate-900" : "text-slate-600"}`}
              >
                {item.label}
              </a>
            ))}
          </nav>
          <div className="flex items-center gap-3 text-sm">
            <span className="text-slate-600">{current.name || current.email}</span>
            <button type="button" onClick={onSignOut} className="rounded border border-slate-300 px-3 py-1 hover:bg-slate-100">
              로그아웃
            </button>
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-[1600px] px-6 py-6">
        {page.page === "review-item" ? (
          <ReviewItemPage client={client} itemId={page.itemId} me={current.sub} />
        ) : page.page === "flow" ? (
          <FlowPage client={client} tab={page.tab} />
        ) : page.page === "tiles" ? (
          <TilesPage client={client} />
        ) : page.page === "complexes" ? (
          <ComplexesPage client={client} />
        ) : (
          <ReviewListPage client={client} />
        )}
      </main>
    </div>
  );
}
