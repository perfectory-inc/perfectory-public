#!/usr/bin/env python3
"""Render the Korean dataset map and the full API example from graph SSOT."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline_graph import ROOT, GRAPH, EXAMPLE, ENDPOINTS, load, reconcile

OUTPUT = Path('docs/data-pipeline-map.md')
STATUS = {
    'implemented': '실행 경로 있음', 'partial': '일부 연결', 'collected_only': '수집 이후 미연결',
    'contract_only': '계약만 있음', 'schema_defined': '표 정의 있음', 'disabled': '수집 비활성',
    'collection_available': '승인 후 수집 가능', 'planned': '예정',
    'waiting': '실행 증거 대기', 'blocked': '증거 미충족', 'missing': '운영 증거 없음',
    'manual_approval': '승인 증거 필요', 'unknown': '상태 미확인',
}


def md(value: str) -> str:
    return value.replace('|', '\\|').replace('\n', ' ')


def names(nodes: list[dict]) -> str:
    return '<br>'.join(md(n['title']) + (f' (`{n["table_name"]}`)' if 'table_name' in n else '') for n in nodes) or '—'


def render(graph: dict, catalog: dict) -> str:
    nodes = graph['nodes']
    index = {n['id']: n for n in nodes}
    sources = [n for n in nodes if n['type'] == 'source_group']
    endpoints = catalog['endpoints']
    # Operational validation/fanout edges are not data lineage into a product panel.
    data_edges = [e for e in graph['edges'] if e['status'] == 'implemented' and e['relation'] in {'feeds', 'joins', 'serves'}]
    starts = [e for e in data_edges if index[e['from']]['type'] == 'source_group']

    def descendants(start: str) -> list[dict]:
        seen, pending = set(), [start]
        while pending:
            current = pending.pop()
            if current in seen:
                continue
            seen.add(current)
            pending.extend(e['to'] for e in data_edges if e['from'] == current)
        return [n for n in nodes if n['id'] in seen]

    lines = [
        '---', 'status: current', 'owner: foundation-platform', 'doc_type: catalog',
        'last_reviewed: 2026-09-06', '---', '',
        '<!-- GENERATED FILE. Do not edit by hand. -->',
        '<!-- Render with: python3 scripts/catalog/render-pipeline-map.py -->', '',
        '# 데이터가 화면에 도착하기까지', '',
        '원천은 제공기관의 데이터이고, Bronze는 받은 원본, Silver는 이름·형식을 맞춘 표,',
        'Gold는 제공 목적에 맞춘 표입니다. 서빙은 조회·지도 제공용 저장소이며 화면은 이를 읽습니다.',
        '아래의 수집규모는 **카탈로그에 등록된 endpoint 수**입니다. 실제 수집 객체 수·행 수·용량은',
        '이 카탈로그에 없으므로 추정하지 않습니다. 실행 경로가 있다는 표시는 운영 배포·전국 적재 완료를 뜻하지 않습니다.', '',
        f'현재 범위: 원천 **{len(sources)}그룹 / {len(endpoints)} endpoint**, ',
        f'Silver·Gold **{sum(n["type"] in {"silver_table", "gold_table"} for n in nodes)}표**, ',
        f'서빙·운영 원장 **{sum(len(n["tables"]) for n in nodes if n["type"] == "serving_group")}표**.', '',
        f'정본: [파이프라인 그래프](../{GRAPH.as_posix()}) · [원천 카탈로그](../{ENDPOINTS.as_posix()}) · ',
        '[결정 ADR-0086](./adr/0086-the-pipeline-graph-names-every-dataset-once.md).', '',
        '## 원천 가족별 전체 범위', '',
        '| 원천 | 수집규모 | 연결 상태 |', '|---|---:|---|',
    ]
    for n in sources:
        count = sum(e['group'] == n['endpoint_catalog_group'] for e in endpoints)
        lines.append(f'| {md(n["title"])} | {count} endpoint | {STATUS.get(n["status"], n["status"])} — {md(n["description"])} |')
    lines += ['', '## 데이터 가족별 연결 경로', '',
              '수집규모는 각 경로가 선택한 원천의 endpoint 수입니다. 같은 원천이 두 경로에 쓰이면 중복 표시되므로 합산하지 않습니다.', '',
              '| 원천 | 수집규모 | Silver → Gold | 서빙 | 화면 |', '|---|---:|---|---|---|']
    for e in starts:
        source = index[e['from']]
        selected = [x for x in endpoints if x['group'] == source['endpoint_catalog_group'] and x['bronze']['source_slug'] in e['source_slugs']]
        reachable = descendants(e['to'])
        tables = [n for n in reachable if n['type'] in {'silver_table', 'gold_table'}]
        serving = [n for n in reachable if n['type'] == 'serving_group']
        surfaces = [n for n in reachable if n.get('surface_kind') in {'tiles', 'api', 'gateway', 'panel'}]
        lines.append(f'| {md(source["title"])}: {"<br>".join(md(x["display_name_ko"]) for x in selected)} | {len(selected)} endpoint | {names(tables)} | {names(serving)} | {names(surfaces)} |')
        if e.get('description'):
            lines.append(f'| ↳ {md(e["description"])} | | | | |')
    lines += ['', '## 수집 이후 아직 연결되지 않은 데이터', '',
              '등록 원천 중 위 실행 경로가 선택하지 않은 항목을 자동으로 모았습니다. 파일 수집이 가능한 원천도',
              '실제 객체 보유 여부는 수집 원장을 확인해야 합니다. 비활성·승인 대기·예정 원천을 수집 완료로 세지 않습니다.',
              '같은 source slug 안의 제외 형식(D154 도형·D150 DBF)은 위 경로의 설명에 따로 명명했습니다.', '',
              '| 원천 가족 | 이후 연결 없는 endpoint | 수집 설정 |', '|---|---|---|']
    for n in sources:
        connected = {slug for e in starts if e['from'] == n['id'] for slug in e['source_slugs']}
        missing = [x for x in endpoints if x['group'] == n['endpoint_catalog_group'] and x['bronze']['source_slug'] not in connected]
        for x in missing:
            lines.append(f'| {md(n["title"])} | {md(x["display_name_ko"])} (`{x["bronze"]["source_slug"]}`) | `{x["source_acquisition_lane"]}` |')
    lines += ['', '## 정제 표의 미연결 구간', '', '| 표 | 상태 | 이유 |', '|---|---|---|']
    for n in nodes:
        if n['type'] not in {'silver_table', 'gold_table'}:
            continue
        downstream = descendants(n['id'])
        if not any(x['type'] == 'serving_group' for x in downstream):
            state = STATUS.get(n['status'], n['status'])
            reason = n['description'] if n['status'] == 'contract_only' else 'Silver 변환은 있으나 서빙으로 가는 실행 경로는 아직 없다.'
            lines.append(f'| `{n["table_name"]}` | {state} | {md(reason)} |')
    lines += ['', '## 서빙·운영 원장 전체 목록', '',
              '사용자 데이터와 품질·수집·발행 원장을 모두 포함합니다. 각 물리 표는 정확히 한 그룹에만 속합니다.', '',
              '| 책임 | 물리 표 |', '|---|---|']
    for n in nodes:
        if n['type'] == 'serving_group':
            lines.append(f'| {md(n["title"])} | {"<br>".join(f"`{t}`" for t in n["tables"])} |')
    lines += ['', '## 운영·제공 접점', '', '| 접점 | 상태 | 역할 |', '|---|---|---|']
    for n in nodes:
        if n['type'] == 'serving_surface':
            lines.append(f'| {md(n["title"])} (`{n["id"]}`) | {STATUS.get(n["status"], n["status"])} | {md(n["description"])} |')
    lines += ['', '## 실행 근거', '', '| 연결 | 실행 명령·스크립트 |', '|---|---|']
    for e in graph['edges']:
        lines.append(f'| {md(index[e["from"]]["title"])} → {md(index[e["to"]]["title"])} | {"<br>".join(f"`{v}`" for v in e["via"])} |')
    lines += ['', '## 이전 지도에서 바뀐 점', '']
    lines += ['- '+md(note) for note in graph['migration_notes']]
    lines += ['', '## 갱신 방법', '',
              '원천 카탈로그·레이크하우스 계약·마이그레이션을 변경하면 그래프도 함께 갱신합니다.',
              '`pipeline-graph-covers-every-dataset.sh`가 세 집합과 중복·엣지 끝점을 대조하고,',
              '`render-pipeline-map.py --check`가 이 문서와 API 예제가 정본에서 생성된 바이트인지 확인합니다.', '']
    return '\n'.join(line.rstrip() for line in lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--root', type=Path, default=ROOT)
    args = parser.parse_args()
    try:
        graph = load(args.root, GRAPH)
        reconcile(args.root, graph)
        artifacts = {OUTPUT: render(graph, load(args.root, ENDPOINTS)).encode('utf-8'),
                     EXAMPLE: (args.root / GRAPH).read_bytes()}
        for path, content in artifacts.items():
            target = args.root / path
            if args.check:
                if not target.exists() or target.read_bytes() != content:
                    print(f'stale pipeline map artifact: {path}', file=sys.stderr)
                    return 1
            else:
                with target.open('w', encoding='utf-8', newline='') as out:
                    out.write(content.decode('utf-8'))
                print(path.as_posix())
    except (ValueError, KeyError, TypeError, OSError) as exc:
        print(f'pipeline map: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
