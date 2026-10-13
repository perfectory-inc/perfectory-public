use std::io::Cursor;

use super::super::pmtiles_feature_ids::tests::{archive, gzip};
use super::*;

/// The bounding box of an MVT command stream, in tile units.
fn geometry_bounds(packed: &[u8]) -> anyhow::Result<Option<(i64, i64, i64, i64)>> {
    Ok(geometry_points(packed)?
        .into_iter()
        .fold(None, |bounds, (x, y)| {
            Some(bounds.map_or((x, y, x, y), |(a, b, c, d)| {
                (a.min(x), b.min(y), c.max(x), d.max(y))
            }))
        }))
}

fn varints(words: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    for word in words {
        let mut value = *word;
        loop {
            let byte = u8::try_from(value & 0x7f).unwrap_or(0);
            value >>= 7;
            if value == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
    }
    out
}

const fn zz(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// A closed square with its lower corner at `(x, y)`, as a packed MVT command stream.
fn square(x: i64, y: i64, side: i64) -> Vec<u8> {
    varints(&[
        9,
        zz(x),
        zz(y),
        26,
        zz(side),
        0,
        0,
        zz(side),
        zz(-side),
        0,
        15,
    ])
}

/// Polygon rings given in absolute tile units, as a packed MVT command stream.
fn polygon(rings: &[&[(i64, i64)]]) -> Vec<u8> {
    let (mut x, mut y) = (0, 0);
    let mut words = Vec::new();
    for ring in rings {
        for (index, (px, py)) in ring.iter().enumerate() {
            if index == 0 {
                words.push(9);
            } else if index == 1 {
                words.push(2 | (u64::try_from(ring.len() - 1).unwrap_or(0) << 3));
            }
            words.push(zz(px - x));
            words.push(zz(py - y));
            (x, y) = (*px, *py);
        }
        words.push(15);
    }
    varints(&words)
}

/// A `width` by `height` rectangle with its lower corner at `(x, y)`.
fn rect(x: i64, y: i64, width: i64, height: i64) -> Vec<u8> {
    polygon(&[&[
        (x, y),
        (x + width, y),
        (x + width, y + height),
        (x, y + height),
    ]])
}

/// One synthetic feature: id, other properties, geometry.
struct Synthetic<'a> {
    id: &'a str,
    kind: TileValue,
    geometry: Vec<u8>,
}

fn text(value: &str) -> TileValue {
    TileValue {
        string_value: Some(value.to_owned()),
        ..TileValue::default()
    }
}

fn inside(id: &str) -> Synthetic<'_> {
    Synthetic {
        id,
        kind: text("land"),
        geometry: square(100, 100, 50),
    }
}

fn mvt(features: &[Synthetic<'_>]) -> Vec<u8> {
    let mut values = Vec::new();
    let mut encoded = Vec::new();
    for feature in features {
        values.push(text(feature.id));
        values.push(feature.kind.clone());
        let at = u32::try_from(values.len()).unwrap_or(0);
        encoded.push(Feature {
            tags: vec![0, at - 2, 1, at - 1],
            geometry: feature.geometry.clone(),
        });
    }
    gzip(
        &Tile {
            layers: vec![Layer {
                name: "parcels".to_owned(),
                features: encoded,
                keys: vec!["pnu".to_owned(), "kind".to_owned()],
                values,
                extent: Some(4096),
            }],
        }
        .encode_to_vec(),
    )
}

fn contract() -> SampleContract {
    SampleContract {
        zooms: vec![1, 2],
        densest_tiles: 2,
        random_tiles: 4,
        random_seed: 7,
        corner_inset_percent: 10,
        example_ids: 20,
        tiny_max_area_tile_units: 64.0,
        additions_max_ratio: 0.001,
        region_id_prefix_chars: BTreeMap::from([("parcels".to_owned(), 7)]),
    }
}

/// An active and a new archive that share z1 tile 1 and z2 tile 6 and differ only in z2 tile 5.
fn pair(new_tile_5: &[Synthetic<'_>]) -> (Vec<u8>, Vec<u8>, SampleCollector) {
    let active = archive(
        1,
        2,
        &[
            (1, mvt(&[inside("9999911111"), inside("9999922222")])),
            (5, mvt(&[inside("9999911111")])),
            (6, mvt(&[inside("9999922222")])),
        ],
    );
    let new = archive(
        1,
        2,
        &[
            (1, mvt(&[inside("9999911111"), inside("9999922222")])),
            (5, mvt(new_tile_5)),
            (6, mvt(&[inside("9999922222")])),
        ],
    );
    let mut collector = SampleCollector::new(&contract(), "parcels");
    for (tile, id) in [(5, "9999911111"), (6, "9999922222")] {
        if let Err(error) = collector.observe(tile, id) {
            panic!("observe: {error}");
        }
    }
    (active, new, collector)
}

fn gate(active: Vec<u8>, new: Vec<u8>, collector: &SampleCollector) -> anyhow::Result<Value> {
    let outcome = first_release_gate(
        Cursor::new(active),
        Cursor::new(new),
        &contract(),
        ("parcels", "pnu"),
        (1, 2),
        collector,
    )?;
    verdict(&outcome, "synthetic.json")
}

fn run(new_tile_5: &[Synthetic<'_>]) -> anyhow::Result<Value> {
    let (active, new, collector) = pair(new_tile_5);
    gate(active, new, &collector)
}

fn refusal(new_tile_5: &[Synthetic<'_>]) -> String {
    run(new_tile_5)
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default()
}

fn compare() -> Compare<'static> {
    Compare {
        layer: "parcels",
        id_property: "pnu",
        max_zoom: 2,
        tiny_max_area_tile_units: 64.0,
        region_chars: Some(7),
        limit: 20,
    }
}

/// The active archive's maxzoom tiles answer nothing: every inside extra that is not tiny is an
/// addition.
fn no_active_maxzoom(_: u64) -> anyhow::Result<BTreeSet<String>> {
    Ok(BTreeSet::new())
}

#[test]
fn identical_archives_pass_and_every_sampled_region_reports_its_tiles() -> anyhow::Result<()> {
    let evidence = run(&[inside("9999911111")])?;
    assert_eq!(evidence["missing"]["count"], 0);
    assert_eq!(evidence["tiles"], evidence["tiles_passed"]);
    assert!(evidence["tiles"].as_u64().unwrap_or(0) >= 3, "{evidence}");
    assert!(
        evidence["active_ids"].as_u64().unwrap_or(0) >= 3,
        "{evidence}"
    );
    for region in ["9999911", "9999922"] {
        assert!(
            evidence["regions"][region]["sampled"].as_u64().unwrap_or(0) > 0,
            "{evidence}"
        );
    }
    Ok(())
}

#[test]
fn an_active_id_missing_from_the_new_tile_is_refused() {
    let refusal = refusal(&[]);
    assert!(
        refusal.contains("\"missing\":{\"count\":1,\"ids\":[\"9999911111\"]"),
        "{refusal}"
    );
    assert!(
        refusal.contains("whole evidence synthetic.json"),
        "{refusal}"
    );
}

#[test]
fn an_addition_above_its_share_of_the_sampled_active_ids_is_refused() {
    // Not tiny, and the active maxzoom tile under it lacks it too: an addition. One in a handful of
    // sampled active ids is far above the contract's 0.1% (ADR-0135 §2).
    let refusal = refusal(&[inside("9999911111"), inside("9999933333")]);
    assert!(
        refusal.contains("\"addition\":{\"count\":1,\"ids\":[\"9999933333\"]"),
        "{refusal}"
    );
}

#[test]
fn an_inside_extra_is_classified_tiny_addition_or_unexplained() -> anyhow::Result<()> {
    let tiny = Synthetic {
        id: "9999933333",
        kind: text("land"),
        geometry: rect(100, 100, 1, 60),
    };
    let active = decompressed(&mvt(&[inside("9999911111")]))?;
    let new = decompressed(&mvt(&[inside("9999911111"), tiny, inside("9999944444")]))?;
    let mut outcome = Equivalence::default();
    let passed = compare_tile(
        Some(&active),
        Some(&new),
        &compare(),
        5,
        &mut no_active_maxzoom,
        &mut outcome,
    )?;
    assert!(
        passed,
        "tiny extras and additions do not fail a tile; only their totals are judged"
    );
    let inside_extras = &outcome.extra_inside;
    assert_eq!(inside_extras.tiny.examples, ["9999933333@2/0/0"]);
    assert_eq!(inside_extras.addition.examples, ["9999944444@2/0/0"]);
    assert_eq!(inside_extras.addition.by_zoom.get(&2), Some(&1));
    assert_eq!(inside_extras.addition.by_region.get("9999944"), Some(&1));
    assert_eq!(inside_extras.unexplained.count, 0);

    // At z1 the active tile lacks 9999944444, but the active z2 tile under it has it: the old
    // oven dropped it at the lower zoom for no reason the gate knows.
    let mut outcome = Equivalence::default();
    let mut asked = Vec::new();
    let passed = compare_tile(
        Some(&active),
        Some(&new),
        &compare(),
        1,
        &mut |under| {
            asked.push(under);
            Ok(BTreeSet::from(["9999944444".to_owned()]))
        },
        &mut outcome,
    )?;
    assert!(!passed, "an unexplained extra fails its tile");
    assert_eq!(outcome.extra_inside.unexplained.count, 1);
    assert_eq!(
        asked,
        [tile_id(2, 0, 0)],
        "its position at z1 is z2 tile 0/0"
    );
    assert!(outcome.refusal()["extra_inside"]["unexplained"]["ids"]
        .as_array()
        .is_some_and(|ids| ids.len() == 1));
    Ok(())
}

#[test]
fn the_maxzoom_tile_under_a_feature_is_found_from_a_vertex_inside_the_tile() -> anyhow::Result<()> {
    // A z1 tile 1/1/0 point at (3000, 100) lies in z2 tile 3/0 (right half, top half).
    let under = maxzoom_tile_under(
        &[(-50, 100), (3000, 100)],
        (0, 100, 3000, 100),
        (1, 1, 0),
        4096,
        2,
    )?;
    assert_eq!(under, tile_id(2, 3, 0));
    // No vertex inside: the centre of the clipped part decides.
    let crossing = maxzoom_tile_under(
        &[(-50, 100), (5000, 100)],
        (0, 100, 4096, 100),
        (1, 0, 0),
        4096,
        2,
    )?;
    assert_eq!(crossing, tile_id(2, 1, 0));
    Ok(())
}

#[test]
fn an_extra_id_only_in_the_buffer_passes_and_is_counted() -> anyhow::Result<()> {
    let neighbours = [
        Synthetic {
            id: "9999933333",
            kind: text("land"),
            geometry: square(-120, 100, 100),
        },
        Synthetic {
            id: "9999944444",
            kind: text("land"),
            geometry: square(4096, 4100, 64),
        },
    ];
    let evidence = run(&[
        inside("9999911111"),
        neighbours[0].clone(),
        neighbours[1].clone(),
    ])?;
    assert_eq!(evidence["extra_in_buffer"], 2);
    assert_eq!(evidence["extra_inside"]["addition"]["count"], 0);
    // One unit inside the tile is inside: 121 square units is not tiny, so it is an addition, above
    // its share of these few sampled ids.
    let poking_in = Synthetic {
        id: "9999933333",
        kind: text("land"),
        geometry: square(-120, 100, 121),
    };
    assert!(run(&[inside("9999911111"), poking_in]).is_err());
    Ok(())
}

#[test]
fn a_changed_property_is_refused() {
    let changed = Synthetic {
        id: "9999911111",
        kind: text("water"),
        geometry: square(100, 100, 50),
    };
    let refusal = refusal(&[changed]);
    assert!(
        refusal.contains("\"changed\":{\"count\":1,\"ids\":[\"9999911111\"]"),
        "{refusal}"
    );
}

#[test]
fn a_value_whose_type_changed_but_reads_the_same_is_counted_not_refused() -> anyhow::Result<()> {
    let (active, new) = (
        Synthetic {
            id: "9999911111",
            kind: TileValue {
                int_value: Some(7),
                ..TileValue::default()
            },
            geometry: square(100, 100, 50),
        },
        Synthetic {
            id: "9999911111",
            kind: text("7"),
            geometry: square(100, 100, 50),
        },
    );
    let mut outcome = Equivalence::default();
    let passed = compare_tile(
        Some(&decompressed(&mvt(&[active]))?),
        Some(&decompressed(&mvt(&[new]))?),
        &compare(),
        5,
        &mut no_active_maxzoom,
        &mut outcome,
    )?;
    assert!(passed);
    assert_eq!(outcome.type_changed.count, 1);
    assert_eq!(outcome.type_changed.examples, ["9999911111@2/0/0"]);
    Ok(())
}

fn decompressed(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(bytes).read_to_end(&mut out)?;
    Ok(out)
}

impl Clone for Synthetic<'_> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            kind: self.kind.clone(),
            geometry: self.geometry.clone(),
        }
    }
}

#[test]
fn a_tile_the_new_archive_lacks_loses_every_active_id() -> anyhow::Result<()> {
    let mut outcome = Equivalence::default();
    let active = decompressed(&mvt(&[inside("9999911111"), inside("9999922222")]))?;
    let one_example = Compare {
        limit: 1,
        ..compare()
    };
    assert!(!compare_tile(
        Some(&active),
        None,
        &one_example,
        5,
        &mut no_active_maxzoom,
        &mut outcome
    )?);
    assert_eq!(outcome.missing.count, 2);
    assert_eq!(
        outcome.missing.examples.len(),
        1,
        "examples stop at the limit"
    );
    assert!(compare_tile(
        None,
        None,
        &one_example,
        5,
        &mut no_active_maxzoom,
        &mut outcome
    )?);
    Ok(())
}

#[test]
fn geometry_bounds_follow_the_cursor_through_every_command() -> anyhow::Result<()> {
    assert_eq!(
        geometry_bounds(&square(-5, 10, 20))?,
        Some((-5, 10, 15, 30))
    );
    assert_eq!(geometry_bounds(&[])?, None);
    assert!(
        geometry_bounds(&varints(&[9, 2])).is_err(),
        "a MoveTo without dy"
    );
    assert!(
        geometry_bounds(&varints(&[3])).is_err(),
        "command 3 does not exist"
    );
    Ok(())
}

#[test]
fn densest_tiles_are_the_longest_maxzoom_entries() -> anyhow::Result<()> {
    let many: Vec<Synthetic<'_>> = ["9999900001", "9999900002", "9999900003"]
        .into_iter()
        .map(inside)
        .collect();
    let bytes = archive(
        1,
        2,
        &[
            (1, mvt(&many)),
            (5, mvt(&many[..1])),
            (6, mvt(&many)),
            (7, mvt(&many[..2])),
        ],
    );
    let mut archive = Archive::open(Cursor::new(bytes))?;
    let mut densest = densest_tiles(&mut archive, 2, 2)?;
    densest.sort_unstable();
    assert_eq!(densest, [6, 7], "z1 tile 1 is not maxzoom");
    assert!(densest_tiles(&mut archive, 2, 0)?.is_empty());
    Ok(())
}

#[test]
fn samples_cover_region_centres_and_corners_and_the_ancestors_of_every_tile() -> anyhow::Result<()>
{
    let regions = BTreeMap::from([(
        "11".to_owned(),
        Extent {
            min: (0, 0),
            max: (3, 3),
        },
    )]);
    let samples = sample_tiles(&contract(), (1, 2), &regions, &[tile_id(2, 3, 0)], &[])?;
    // Centre (1,1) and corners inset to (0,0)..(3,3) at z2, and their z1 ancestors.
    for (zoom, x, y) in [(2, 1, 1), (2, 0, 0), (2, 3, 3), (1, 0, 0), (1, 1, 1)] {
        assert!(
            samples
                .get(&tile_id(zoom, x, y))
                .is_some_and(|why| why.contains("region:11")),
            "{zoom}/{x}/{y}"
        );
    }
    assert!(samples
        .get(&tile_id(1, 1, 0))
        .is_some_and(|why| why.contains("densest")));
    assert!(sample_tiles(
        &contract(),
        (1, 2),
        &BTreeMap::new(),
        &[tile_id(1, 0, 0)],
        &[]
    )
    .is_err());
    let narrow = sample_tiles(&contract(), (2, 2), &regions, &[], &[])?;
    assert!(
        narrow
            .keys()
            .all(|tile| tile_zxy(*tile).is_ok_and(|(zoom, _, _)| zoom == 2)),
        "zooms the layer does not serve are not sampled"
    );
    Ok(())
}

#[test]
fn the_collector_records_region_extents_and_a_seeded_reservoir() -> anyhow::Result<()> {
    let observe = |seed: u64| -> anyhow::Result<SampleCollector> {
        let mut contract = contract();
        contract.random_seed = seed;
        contract.random_tiles = 2;
        contract.region_id_prefix_chars = BTreeMap::from([("parcels".to_owned(), 2)]);
        let mut collector = SampleCollector::new(&contract, "parcels");
        for (x, y) in [(0, 0), (3, 1), (2, 2), (1, 3), (0, 3)] {
            collector.observe(tile_id(2, x, y), "11-synthetic")?;
            collector.observe(tile_id(2, x, y), "22-synthetic")?;
        }
        Ok(collector)
    };
    let collector = observe(7)?;
    assert_eq!(
        collector.regions.get("11"),
        Some(&Extent {
            min: (0, 0),
            max: (3, 3)
        })
    );
    assert_eq!(collector.regions.len(), 2);
    assert_eq!(collector.reservoir.len(), 2);
    assert_eq!(
        collector.reservoir,
        observe(7)?.reservoir,
        "the seed fixes the set"
    );
    let unit_without_regions = SampleCollector::new(&contract(), "complex");
    assert!(unit_without_regions.prefix_chars.is_none());
    Ok(())
}

#[test]
fn a_region_no_sample_reached_reports_zero() -> anyhow::Result<()> {
    let (active, new, _) = pair(&[inside("9999911111")]);
    let outcome = compare_archives(
        &mut Archive::open(Cursor::new(active))?,
        &mut Archive::open(Cursor::new(new))?,
        &BTreeMap::new(),
        &compare(),
        ["unsampled".to_owned()],
    )?;
    assert_eq!(outcome.regions["unsampled"].sampled, 0);
    Ok(())
}

#[test]
fn the_contract_parses_and_refuses_what_it_cannot_sample() -> anyhow::Result<()> {
    let contract = SampleContract::parse(SAMPLE_CONTRACT_JSON)?;
    assert_eq!(contract.zooms, [14, 15, 16]);
    assert!(contract.example_ids <= 20);
    let mut value: Value = serde_json::from_str(SAMPLE_CONTRACT_JSON)?;
    for (field, bad) in [
        ("zooms", serde_json::json!([])),
        ("zooms", serde_json::json!([16, 14])),
        ("corner_inset_percent", serde_json::json!(50)),
        ("example_ids", serde_json::json!(0)),
    ] {
        let saved = value[field].clone();
        value[field] = bad;
        assert!(
            SampleContract::parse(&value.to_string()).is_err(),
            "{field}"
        );
        value[field] = saved;
    }
    Ok(())
}

/// One extra beside `9999911111` in z2 tile 5, judged with the active maxzoom tile under it
/// already carrying the extra — so unless it is tiny it is unexplained.
fn judge_extra(geometry: Vec<u8>) -> anyhow::Result<(bool, Equivalence)> {
    let extra = Synthetic {
        id: "9999933333",
        kind: text("land"),
        geometry,
    };
    let active = decompressed(&mvt(&[inside("9999911111")]))?;
    let new = decompressed(&mvt(&[inside("9999911111"), extra]))?;
    let mut outcome = Equivalence {
        additions_max_ratio: 0.001,
        tiny_max_area_tile_units: 64.0,
        ..Equivalence::default()
    };
    let passed = compare_tile(
        Some(&active),
        Some(&new),
        &compare(),
        5,
        &mut |_| Ok(BTreeSet::from(["9999933333".to_owned()])),
        &mut outcome,
    )?;
    Ok((passed, outcome))
}

#[test]
fn a_small_span_with_an_area_above_the_tiny_bound_is_unexplained_and_refused() -> anyhow::Result<()>
{
    // 10 x 10: a span of 10 units, but 100 square units of area (ADR-0135 §2 decides by area).
    let (passed, outcome) = judge_extra(rect(100, 100, 10, 10))?;
    assert!(!passed);
    assert_eq!(outcome.extra_inside.tiny.count, 0);
    assert_eq!(
        outcome.extra_inside.unexplained.examples,
        ["9999933333@2/0/0"]
    );
    assert!(!outcome.passed());
    let refusal = verdict(&outcome, "synthetic.json")
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        refusal.contains("\"unexplained\":{\"count\":1,\"ids\":[\"9999933333\"]"),
        "{refusal}"
    );
    Ok(())
}

#[test]
fn a_thin_sliver_with_a_small_area_is_tiny_allowed_and_counted() -> anyhow::Result<()> {
    // 1 x 64: a span of 64 units, but exactly the 64 square units the contract allows.
    let (passed, outcome) = judge_extra(rect(100, 100, 1, 64))?;
    assert!(passed);
    assert!(outcome.passed());
    let evidence = verdict(&outcome, "synthetic.json")?;
    let tiny = &evidence["extra_inside"]["tiny"];
    assert_eq!(tiny["count"], 1);
    assert_eq!(tiny["by_zoom"]["2"], 1);
    assert_eq!(tiny["by_region"]["9999933"], 1);
    assert_eq!(tiny["examples"][0], "9999933333@2/0/0");
    assert_eq!(evidence["tiny_max_area_tile_units"], 64.0);
    // One more square unit and it is no longer tiny.
    let (passed, outcome) = judge_extra(rect(100, 100, 1, 65))?;
    assert!(!passed);
    assert_eq!(outcome.extra_inside.unexplained.count, 1);
    Ok(())
}

#[test]
fn additions_are_allowed_up_to_their_share_and_refused_just_above_it() -> anyhow::Result<()> {
    let with_additions = |additions: u64, active_ids: u64| {
        let mut outcome = Equivalence {
            active_ids,
            additions_max_ratio: 0.001,
            ..Equivalence::default()
        };
        outcome.extra_inside.addition.count = additions;
        outcome
    };
    let at_the_ratio = with_additions(1, 1_000);
    assert!(at_the_ratio.passed());
    let evidence = verdict(&at_the_ratio, "synthetic.json")?;
    assert_eq!(evidence["extra_inside"]["addition"]["count"], 1);
    assert_eq!(evidence["additions_max_ratio"], 0.001);
    assert!(!with_additions(2, 1_000).passed(), "0.2% is above 0.1%");
    assert!(!with_additions(1, 999).passed(), "just above the ratio");
    assert!(with_additions(0, 0).passed());
    assert!(
        !with_additions(1, 0).passed(),
        "an addition against no sampled id"
    );
    let refusal = verdict(&with_additions(2, 1_000), "synthetic.json")
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(refusal.contains("\"ratio\":0.002"), "{refusal}");
    Ok(())
}

#[test]
fn the_area_inside_is_clipped_to_the_tile_and_holes_subtract() -> anyhow::Result<()> {
    let area = |geometry: Vec<u8>| -> anyhow::Result<f64> {
        Ok(area_inside(&geometry_rings(&geometry)?, 4096))
    };
    assert!(
        (area(rect(-50, 0, 100, 100))? - 5_000.0).abs() < 1e-9,
        "half is outside"
    );
    assert!(
        (area(rect(4000, 4000, 200, 200))? - 9_216.0).abs() < 1e-9,
        "a corner"
    );
    assert!(area(rect(5000, 0, 10, 10))?.abs() < 1e-9, "wholly outside");
    let holed = polygon(&[
        &[(0, 0), (100, 0), (100, 100), (0, 100)],
        &[(25, 25), (25, 75), (75, 75), (75, 25)],
    ]);
    assert!((area(holed)? - 7_500.0).abs() < 1e-9);
    Ok(())
}
