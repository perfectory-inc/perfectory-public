#!/usr/bin/env bash
# Foundation Platform ownership boundary guardrail — ADR 0048.
#
# Foundation Platform owns catalog reference data. Gongzzang may consume its
# published contracts, but must not reintroduce catalog domain ownership,
# persistence writers, or mutation endpoints.
#
# Strategy: whitelist instead of blacklist. Catalog HTTP routes must only use
# `get` / `head` handlers. Anything else (post / put / patch / delete / on /
# any / MethodRouter::new().post / Method::POST / .nest) is flagged.
#
# Checks:
# 1. Foundation-owned catalog domain crates must not exist in this repository
# 2. no direct SQL writes (INSERT / UPDATE / DELETE) to catalog tables anywhere
#    in the workspace (services/, crates/, apps/)
# 3. catalog HTTP routes are read-only (whitelist: get/head only)
# 4. catalog path constants are explicitly surfaced for review

set -euo pipefail

forbidden_catalog_dirs=(
  "crates/industrial-complex-domain"
  "crates/parcel-domain"
  "crates/building-domain"
  "crates/manufacturer-domain"
)

scan_roots=("services" "crates" "apps")

fail=0

report() {
  echo "foundation-ownership-boundary: $1" >&2
  fail=1
}

# ── 1) Foundation-owned catalog crates must stay absent ────────────────────
for dir in "${forbidden_catalog_dirs[@]}"; do
  if [ -e "$dir" ]; then
    report "$dir is Foundation Platform-owned and must not exist in Gongzzang."
  fi
done

# ── 2) direct SQL writes to catalog owner tables ────────────────────────────
export CATALOG_TABLES_RE='(industrial_complex|industrial_complexes|parcel|parcels|building|buildings|manufacturer|manufacturers)'

for root in "${scan_roots[@]}"; do
  [ -d "$root" ] || continue
  while IFS= read -r -d '' file; do
    hits=$(perl -0777 -ne '
      my $tables = $ENV{CATALOG_TABLES_RE};
      while (m{\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:"?\w+"?\.)?"?($tables)"?\b}gis) {
        print "  $1 $2\n";
      }
    ' "$file")
    if [ -n "$hits" ]; then
      report "$file: direct write to Foundation-owned catalog data detected:"
      echo "$hits" >&2
    fi
  done < <(find "$root" -type f -name "*.rs" -print0)
done

# ── 3) HTTP routes / nest blocks against catalog paths ──────────────────────
export CATALOG_PATH_RE='(parcels?|buildings?|industrial[-_]complexes?|manufacturers?)'

for root in "${scan_roots[@]}"; do
  [ -d "$root" ] || continue
  while IFS= read -r -d '' file; do
    violations=$(perl -0777 -ne '
      my $path_re = $ENV{CATALOG_PATH_RE};

      # ── 3a) `.route("path", handler)` whitelist ─────────────────────────
      while (m{\.route\s*\(\s*"([^"]*)"\s*,}sg) {
        my $path = $1;
        # Save outer match offsets BEFORE inner regex (inner clobbers @-/@+
        # globals — Codex stop-time review finding).
        my $tail_start = $+[0];
        my $resume_pos = pos();
        next unless $path =~ m{/$path_re(/|$|\?)};
        my $pos = $tail_start;
        my $depth = 1;
        while ($pos < length($_)) {
          my $ch = substr($_, $pos, 1);
          if ($ch eq "(") { $depth++; }
          elsif ($ch eq ")") {
            $depth--;
            last if $depth == 0;
          }
          $pos++;
        }
        pos($_) = $resume_pos;
        next if $depth != 0;
        my $block = substr($_, $tail_start, $pos - $tail_start);

        # Whitelist: every method-router call inside the block must be get()
        # or head(). post / put / patch / delete / on / any / etc. = mutation.
        # Direct word-boundary match catches all module-qualified forms:
        # `post(`, `routing::post(`, `axum::routing::post(`, etc.
        # `\b` excludes `post_user(` (continues as word char).
        while ($block =~ /\b(post|put|patch|delete|options|trace|connect|any|on|on_method|method_router|fallback)(?:_service)?\s*\(/gi) {
          my $m = $1;
          print "  $path -> handler uses \"$m\" (Foundation catalog consumers allow only get/head)\n";
          last;
        }
        # `MethodFilter::POST` / `Method::POST` constants used with `on()`.
        if ($block =~ /\bMethod(?:Filter)?::(POST|PUT|PATCH|DELETE|TRACE|CONNECT|OPTIONS)\b/) {
          print "  $path -> Method::$1 constant in a Foundation catalog consumer\n";
        }
      }

      # ── 3b) `.nest("path", sub)` / `.route_service("path", svc)` ────────
      # Both register opaque routing surface (sub-router or arbitrary
      # `Service<Request>`) at the catalog path — service can handle any
      # method including mutations. Forbid for catalog paths entirely.
      while (m{\.(nest(?:_service)?|route_service)\s*\(\s*"([^"]*)"\s*,}sg) {
        my $kind = $1;
        my $path = $2;
        my $resume_pos = pos();
        if ($path =~ m{/$path_re(/|$|\?)}) {
          print "  $path -> .$kind(\"$path\", ...) opaque Foundation catalog mutation surface\n";
        }
        pos($_) = $resume_pos;
      }
    ' "$file")
    if [ -n "$violations" ]; then
      report "$file: Foundation catalog mutation route detected:"
      echo "$violations" >&2
    fi
  done < <(find "$root" -type f -name "*.rs" -print0)
done

# ── 4) Path constants pointing at catalog resources ─────────────────────────
# Resolve catalog-path consts to their identifier names, then re-scan for
# `.route(IDENT, ...)` blocks and apply the same whitelist as step 3.
for root in "${scan_roots[@]}"; do
  [ -d "$root" ] || continue
  while IFS= read -r -d '' file; do
    violations=$(perl -0777 -ne '
      my $path_re = $ENV{CATALOG_PATH_RE};

      # Collect catalog-path const/static identifiers in this file.
      my @catalog_idents;
      while (m{\b(?:const|static)\s+(\w+)\s*:\s*&?(?:'"'"'static\s+)?str\s*=\s*"([^"]*)"}sg) {
        my ($name, $val) = ($1, $2);
        next unless $val =~ m{/$path_re(/|$|\?)};
        push @catalog_idents, [$name, $val];
      }
      # For each const, check `.route(IDENT, <handler>)` and `.nest(IDENT, ...)`.
      for my $pair (@catalog_idents) {
        my ($name, $val) = @$pair;
        # route(IDENT, ...) — paren-balance the handler.
        while (m{\.route\s*\(\s*\Q$name\E\s*,}sg) {
          my $tail_start = $+[0];
          my $resume_pos = pos();
          my $pos = $tail_start;
          my $depth = 1;
          while ($pos < length($_)) {
            my $ch = substr($_, $pos, 1);
            if ($ch eq "(") { $depth++; }
            elsif ($ch eq ")") {
              $depth--;
              last if $depth == 0;
            }
            $pos++;
          }
          pos($_) = $resume_pos;
          next if $depth != 0;
          my $block = substr($_, $tail_start, $pos - $tail_start);
          while ($block =~ /\b(post|put|patch|delete|options|trace|connect|any|on|on_method|method_router|fallback)(?:_service)?\s*\(/gi) {
            my $m = $1;
            print "  $name (=\"$val\") .route -> handler uses \"$m\" (Foundation catalog consumers allow only get/head)\n";
            last;
          }
          if ($block =~ /\bMethod(?:Filter)?::(POST|PUT|PATCH|DELETE|TRACE|CONNECT|OPTIONS)\b/) {
            print "  $name (=\"$val\") .route -> Method::$1 constant in a Foundation catalog consumer\n";
          }
        }
        # nest(IDENT, ...) / route_service(IDENT, ...) — opaque routing = forbid.
        while (m{\.(nest(?:_service)?|route_service)\s*\(\s*\Q$name\E\s*,}sg) {
          my $kind = $1;
          print "  $name (=\"$val\") .$kind opaque Foundation catalog mutation surface\n";
        }
      }
    ' "$file")
    if [ -n "$violations" ]; then
      report "$file: Foundation catalog path constant used in mutation context:"
      echo "$violations" >&2
    fi
  done < <(find "$root" -type f -name "*.rs" -print0)
done

if [ "$fail" -ne 0 ]; then
  cat >&2 <<'EOF'

Decision reference:
- docs/adr/0048-horizontal-platform-redefinition.md

EOF
  exit 1
fi
