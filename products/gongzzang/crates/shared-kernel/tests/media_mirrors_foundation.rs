//! 공짱의 `media` 는 foundation 이 쓰고 서빙하는 것의 사본이다(영역 간 Cargo path 의존 금지,
//! AGENTS.md). 2026-11-06 까지 매물 사진은 형식(`PhotoContentType`)과 키를 따로 정의해 공매·경매
//! 사진과 같은 파이프라인에 들지 못했다(root ADR-0427 "남은 중복"). 이 시험이 사본을 원본에 묶는다:
//! foundation `media.rs` 의 사진 형식(`sniff` 가 내는 것)·확장자·media type 과 `parcel_media.renditions`
//! 의 사다리. 고시 첨부의 문서 형식(`sniff_document` 만 내는 HWP·HWPX·ZIP·bin, root ADR-0484)은
//! 공짱이 받거나 내지 않으므로 사본에 없다.
//! 파일은 컴파일 때가 아니라 시험 때 읽는다(이미지 빌드 문맥에 다른 영역 파일이 없어도 된다).

#![allow(clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use shared_kernel::media::{MediaFormat, RENDITION_FORMATS, RENDITION_VERSION, RENDITION_WIDTHS};

fn foundation(path: &str) -> String {
    // cargo runs a test with the package root as its working directory.
    let file = PathBuf::from("../../../../platforms/foundation-platform").join(path);
    std::fs::read_to_string(&file).unwrap_or_else(|error| panic!("{}: {error}", file.display()))
}

#[test]
fn the_formats_are_the_foundation_ones() {
    let source = foundation("crates/foundation-shared-kernel/src/media.rs");
    // The photo formats: the ones foundation's `sniff` returns, up to the next function.
    let sniff = source
        .split_once("pub fn sniff(")
        .and_then(|(_, rest)| rest.split_once("pub fn "))
        .map(|(body, _)| body)
        .expect("foundation MediaFormat::sniff");
    let photo = regex::Regex::new(r"Some\(Self::(\w+)\)").expect("regex");
    // A photo foundation never serves (`is_served`: BMP, TIFF, root ADR-0503) stands in the served
    // store as its PNG display master, so gongzzang never meets it and has no copy of it.
    let unserved_body = source
        .split_once("pub const fn is_served(")
        .and_then(|(_, rest)| rest.split_once("pub "))
        .map(|(body, _)| body)
        .expect("foundation MediaFormat::is_served");
    let unserved: Vec<String> = regex::Regex::new(r"Self::(\w+)")
        .expect("regex")
        .captures_iter(unserved_body)
        .map(|capture| capture[1].to_owned())
        .collect();
    let photos: Vec<String> = photo
        .captures_iter(sniff)
        .map(|capture| capture[1].to_owned())
        .filter(|variant| !unserved.contains(variant))
        .collect();
    let arm = regex::Regex::new(r#"Self::(\w+) => "([^"]+)","#).expect("regex");
    let mut arms: Vec<(String, Vec<String>)> = Vec::new();
    for capture in arm.captures_iter(&source) {
        let variant = capture[1].to_owned();
        if !photos.contains(&variant) {
            continue;
        }
        let value = capture[2].to_owned();
        match arms.iter_mut().find(|(name, _)| *name == variant) {
            Some((_, values)) => values.push(value),
            None => arms.push((variant, vec![value])),
        }
    }
    assert_eq!(arms.len(), MediaFormat::ALL.len(), "{arms:?}");
    for (variant, values) in arms {
        let [extension, content_type] = values.as_slice() else {
            panic!("{variant}: expected an extension and a media type, got {values:?}");
        };
        let ours = MediaFormat::from_extension(extension)
            .unwrap_or_else(|| panic!("{variant}: no gongzzang format for .{extension}"));
        assert_eq!(ours.content_type(), content_type, "{variant}");
        assert_eq!(format!("{ours:?}"), variant);
    }
}

#[test]
fn the_ladder_is_the_contract_one() {
    let contract: serde_json::Value =
        serde_json::from_str(&foundation("config/r2-connections.contract.json")).expect("json");
    let ladder = &contract["parcel_media"]["renditions"];
    assert_eq!(ladder["version"], RENDITION_VERSION);
    assert_eq!(ladder["widths"], serde_json::json!(RENDITION_WIDTHS));
    let formats: Vec<&str> = ladder["formats"]
        .as_array()
        .expect("formats")
        .iter()
        .filter_map(|entry| entry["format"].as_str())
        .collect();
    let ours: Vec<&str> = RENDITION_FORMATS.iter().map(|f| f.extension()).collect();
    assert_eq!(formats, ours);
}
