"use client";
import { Input } from "@gongzzang/ui";
import { debounce } from "es-toolkit";
import { useTranslations } from "next-intl";
import { useEffect, useMemo, useRef, useState } from "react";

/** Pause after the last keystroke before the search word reaches the URL and the API. */
const SEARCH_DEBOUNCE_MS = 300;

/**
 * The name/code search box.
 *
 * The input is uncontrolled by the URL while the reader is typing and controlled by it otherwise:
 * a keystroke updates local state immediately, and only a settled word — {@link SEARCH_DEBOUNCE_MS}
 * of quiet — is pushed outward. Without that, "반월특수지역" is eight requests and eight URL
 * writes, seven of which are already stale when they land.
 *
 * `debounce` comes from es-toolkit, the workspace's canonical utility library
 * (`docs/technology-stack.md` §1.1). The callback is read through a ref so the debounced function
 * itself never has to be rebuilt — rebuilding it on every render of the parent is how a debounced
 * call gets dropped.
 */
export function ComplexSearchBar({
  value,
  onSearch,
}: {
  value: string;
  onSearch: (q: string) => void;
}) {
  const t = useTranslations("complexes.search");
  const [draft, setDraft] = useState(value);

  const onSearchRef = useRef(onSearch);
  useEffect(() => {
    onSearchRef.current = onSearch;
  }, [onSearch]);

  const debouncedSearch = useMemo(
    () => debounce((next: string) => onSearchRef.current(next), SEARCH_DEBOUNCE_MS),
    [],
  );
  useEffect(() => () => debouncedSearch.cancel(), [debouncedSearch]);

  // The URL can change without a keystroke — Back, or a shared link. Only adopt it when it says
  // something the draft does not, so a trailing space being trimmed does not fight the typist.
  useEffect(() => {
    setDraft((current) => (current.trim() === value ? current : value));
  }, [value]);

  return (
    <Input
      type="search"
      value={draft}
      onChange={(e) => {
        setDraft(e.target.value);
        debouncedSearch(e.target.value);
      }}
      placeholder={t("placeholder")}
      aria-label={t("placeholder")}
    />
  );
}
