import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { DawneerClient, loadSession, signOut } from "./api/client";
import { ComplexesPage } from "./pages/ComplexesPage";
import { DataCatalogPage } from "./pages/DataCatalogPage";
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
  return hash.replace(/^#/, "") || "/";
}

/** The console's menus; each is a platform's own screen composed here (root ADR-0114). */
const MENU = [
  { href: "#/", label: "필지 계보 검토", active: (route: string) => route === "/" || route.startsWith("/items/") },
  { href: "#/catalog", label: "데이터 카탈로그", active: (route: string) => route === "/catalog" },
  { href: "#/tiles", label: "지도 타일 발행", active: (route: string) => route === "/tiles" },
  { href: "#/complexes", label: "산업단지", active: (route: string) => route === "/complexes" },
] as const;

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

  const itemId = route.match(/^\/items\/([0-9a-f-]{36})$/)?.[1];
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
                className={`text-sm hover:text-slate-900 ${item.active(route) ? "font-semibold text-slate-900" : "text-slate-600"}`}
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
        {itemId ? (
          <ReviewItemPage client={client} itemId={itemId} me={current.sub} />
        ) : route === "/catalog" ? (
          <DataCatalogPage client={client} />
        ) : route === "/tiles" ? (
          <TilesPage client={client} />
        ) : route === "/complexes" ? (
          <ComplexesPage client={client} />
        ) : (
          <ReviewListPage client={client} />
        )}
      </main>
    </div>
  );
}
