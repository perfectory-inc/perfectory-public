#!/usr/bin/env python3
"""17개 시도 GeoJSON 을 병합해 발행 원천을 만든다 (EMD_CD·EMD_NM·SIGUNGU_CD·SIGUNGU_NM)."""
import csv, glob, hashlib, io, json, sys

sgg = {}
doc = json.load(io.open("out/sgg-attrs.geojson", encoding="utf-8"))
for feat in doc["features"]:
    p = feat.get("properties") or {}
    code = (p.get("BJCD") or "")[:5]
    name = p.get("NAME") or ""
    if len(code) == 5 and name:
        sgg.setdefault(code, name)
print(f"sigungu names: {len(sgg)}")

features, bad_code, no_parent = [], 0, 0
for path in sorted(glob.glob("out/30603-*.geojson")):
    doc = json.load(io.open(path, encoding="utf-8"))
    for feat in doc["features"]:
        p = feat.get("properties") or {}
        emd_cd = (p.get("EMD_CD") or "").strip()
        emd_nm = (p.get("EMD_NM") or "").strip()
        parent = (p.get("COL_ADM_SE") or "").strip()
        if len(emd_cd) != 8 or not emd_nm or len(parent) != 5:
            bad_code += 1
            continue
        parent_nm = sgg.get(parent)
        if not parent_nm:
            no_parent += 1
            continue
        feat["properties"] = {
            "EMD_CD": emd_cd, "EMD_NM": emd_nm,
            "SIGUNGU_CD": parent, "SIGUNGU_NM": parent_nm,
        }
        features.append(feat)

out = {"type": "FeatureCollection", "features": features}
data = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode()
io.open("official-administrative-boundary.geojson", "wb").write(data)
digest = hashlib.sha256(data).hexdigest()
print(f"features={len(features)} bad_code={bad_code} no_parent_name={no_parent}")
print(f"sha256={digest}")
print(f"canonical_decimal={int(digest[:15], 16)}")
