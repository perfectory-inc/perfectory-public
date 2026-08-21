-- A boundary PostGIS had to repair is published as repaired, carrying what the repair measured.
--
-- Root ADR-0047 records the decision and the alternatives it rejects.
--
-- Measured on 2026-08-22 over the 1,343 exported Silver boundaries: two of them are invalid in the
-- source coordinates, before any code in this repository touches them.
--
--     141060  용인첨단시스템반도체클러스터국가산업단지   Self-intersection
--     247920  김천1일반산업단지(4단계)                  Self-intersection
--
-- Neither is a crossing. The ring revisits a place it has already been — 0.0008 m apart on 141060,
-- 0.000000 m apart on 247920 — so the loop closes against itself at a point. There are zero pairs
-- of segments that cross.
--
-- `complex_boundary_publication_geometry_check` refuses both, and one publish is one transaction,
-- so **two rows stop the other 1,341**. The choice was never "publish 1,343 or publish 1,341"; it
-- was "publish 1,343 or publish none".
--
-- `ST_MakeValid` cuts a ring at the place it touches itself and returns the parts. On these two it
-- creates no coordinate: measured before and after, the area is identical to the last printed digit
-- and `ST_HausdorffDistance` is 0.000000 m. Reproduced here at the same magnitude — a 1,501-vertex
-- ring of 7,258,233.40 m² with the same defect — the relative area change is 1.4e-15 and the
-- Hausdorff distance is 0. Several parts are not exceptional in this source either: 88 of the 1,343
-- boundaries already arrive as more than one part, and 26 have holes.
--
-- So the load repairs rather than drops. The rest of this file is what stops "repair" from becoming
-- "substitute a different shape", because `ST_MakeValid` is not a promise: on other inputs it
-- returns a `GEOMETRYCOLLECTION`, or a set of parts whose total area is nothing like the input's.
--
-- Four gates have to hold before a repaired geometry is stored, and all four are the table's own —
-- not a publisher's opinion, so a second writer inherits them:
--
--   1. **it is still polygonal** — `geom` is declared `geometry(MultiPolygon, 4326)`, and a
--      `GEOMETRYCOLLECTION` is refused by that type. `ST_IsValid` does *not* refuse one: a
--      collection of a polygon and a dangling line is a perfectly valid collection, measured here.
--      Nothing in the publisher calls `ST_CollectionExtract` or `ST_MakeValid(..., 'method=structure')`,
--      both of which would make the type gate pass by discarding the parts that do not fit — which
--      is silently publishing a different shape, the exact failure these gates exist for.
--   2. **it is valid** — `complex_boundary_publication_geometry_check`, unchanged. This migration
--      does not relax it; the repair happens before the row is offered to it.
--   3. **its area survived** — `repair_area_change_ratio`, bounded below.
--   4. **its vertices did not move** — `repair_hausdorff_distance_m`, bounded below.
--
-- Both measurements are taken in the source CRS, in metres, because that is where the defect is and
-- where the repair happens: the two boundaries are invalid in EPSG:5186 before reprojection, and a
-- tolerance stated in EPSG:4326 degrees would mean a different distance at every latitude.
--
-- The thresholds are stated **here and only here.** The publisher measures and records; this
-- constraint decides. A number kept in both places is one release away from being two numbers.
--
--   * `repair_area_change_ratio <= 1e-9`. Six orders of magnitude above the 1.4e-15 the reproduction
--     measured, and on the larger of the two boundaries this repairs (7,280,685.27 m²) it admits an
--     absolute change of 0.0073 m² — far below anything closing a 0.8 mm self-touch could account
--     for.
--   * `repair_hausdorff_distance_m <= 1e-6`. The source states its coordinates as metres to six
--     decimal places, so one micrometre is one unit of the source's own coordinate resolution: a
--     repair inside it cannot have moved a point to any place the source was able to express.
--
-- `NaN` fails both bounds rather than passing them: `NaN <= x` is false in PostgreSQL for every `x`,
-- while `NaN >= 0` is true, which is why the upper bound and not the lower one is what refuses it.
--
-- Rollback: `ALTER TABLE ... DROP COLUMN` / `DROP CONSTRAINT` as a new forward migration (root
-- ADR-0001 §7). Dropping them discards the record that two boundaries were repaired, so the
-- rollback is only honest together with re-publishing the load that wrote them.

ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    -- `DEFAULT false` fills the rows a deployment already published, and the statement below takes
    -- the default away again. It is true of those rows — before this migration no repair could
    -- happen, so every existing row is an unrepaired one — and it must not stay available to the
    -- next writer, because a repaired geometry inserted without naming this column would then be
    -- recorded as untouched by omission.
    ADD COLUMN geometry_repaired boolean NOT NULL DEFAULT false,
    -- `ST_HausdorffDistance(source, repaired)` in the source CRS. PostGIS computes the *discrete*
    -- Hausdorff distance, vertex to geometry, which is the question being asked: did the repair put
    -- a vertex anywhere the original outline does not already pass through.
    ADD COLUMN repair_hausdorff_distance_m double precision,
    -- `abs(area_after - area_before) / area_before`, in the source CRS. A ratio and not a difference
    -- because the two boundaries this repairs already differ six-fold in area (7,280,685.27 m² and
    -- 1,219,008.65 m²) and the source holds 1,341 others, so one absolute tolerance would be slack
    -- on the large ones and impossible on the small ones. A repair of a ring whose signed
    -- area is zero — the two lobes of a figure-of-eight cancelling — has no ratio to state and is
    -- recorded as infinity, which fails the bound below rather than dividing by zero.
    ADD COLUMN repair_area_change_ratio double precision;

ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    ALTER COLUMN geometry_repaired DROP DEFAULT;

ALTER TABLE serving_postgis.industrial_complex_boundary_publication
    -- The flag and the evidence are one fact stated three ways, so the three have to agree. A row
    -- claiming a repair with nothing measured, or measurements under a row claiming none, would
    -- leave a reader unable to say which half to believe.
    ADD CONSTRAINT complex_boundary_publication_repair_evidence_check
        CHECK (geometry_repaired = (repair_hausdorff_distance_m IS NOT NULL)
           AND geometry_repaired = (repair_area_change_ratio IS NOT NULL)),
    ADD CONSTRAINT complex_boundary_publication_repair_tolerance_check
        CHECK (
            (repair_hausdorff_distance_m IS NULL
                OR (repair_hausdorff_distance_m >= 0
                    AND repair_hausdorff_distance_m <= 0.000001))
            AND (repair_area_change_ratio IS NULL
                OR (repair_area_change_ratio >= 0
                    AND repair_area_change_ratio <= 0.000000001))
        );

COMMENT ON COLUMN serving_postgis.industrial_complex_boundary_publication.geometry_repaired IS
    'Whether geom is the reprojection of ST_MakeValid(source) rather than of the source geometry itself. Set only when the source geometry was invalid in its own CRS; the two measurement columns say by how little the repair changed it.';
COMMENT ON COLUMN serving_postgis.industrial_complex_boundary_publication.repair_hausdorff_distance_m IS
    'ST_HausdorffDistance between the source geometry and its repair, in source-CRS metres. NULL unless geometry_repaired.';
COMMENT ON COLUMN serving_postgis.industrial_complex_boundary_publication.repair_area_change_ratio IS
    'Relative change in ST_Area between the source geometry and its repair, in the source CRS; infinity when the source area is zero. NULL unless geometry_repaired.';
