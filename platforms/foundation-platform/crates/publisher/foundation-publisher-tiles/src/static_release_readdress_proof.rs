//! Bounded representative MVT proof for a copy and its original release.
use anyhow::{ensure, Context as _};
use reqwest::Client;
use std::io::Read as _;

const MAX_TILE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RepresentativeTile {
    z: u32,
    x: u32,
    y: u32,
}

impl RepresentativeTile {
    pub(super) fn parse(raw: &str) -> anyhow::Result<Self> {
        let parts = raw.split('/').collect::<Vec<_>>();
        ensure!(parts.len() == 3, "representative tile must be z/x/y");
        let parse = |value: &str| -> anyhow::Result<u32> {
            ensure!(
                !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
                "tile coordinates must be unsigned decimal integers"
            );
            Ok(value.parse()?)
        };
        let tile = Self {
            z: parse(parts[0])?,
            x: parse(parts[1])?,
            y: parse(parts[2])?,
        };
        ensure!(tile.z <= 30, "representative tile zoom must be in 0..=30");
        let dimension = 1_u32 << tile.z;
        ensure!(
            tile.x < dimension && tile.y < dimension,
            "representative tile coordinates exceed the zoom range"
        );
        Ok(tile)
    }

    pub(super) fn path(&self) -> String {
        format!("{}/{}/{}", self.z, self.x, self.y)
    }
}

pub(super) async fn prove_tiles(
    http: &Client,
    bases: [&str; 3],
    input_source: &str,
    output_source: &str,
    tile: &RepresentativeTile,
) -> anyhow::Result<Vec<u8>> {
    let input = fetch_tile(http, bases[0], input_source, tile)
        .await
        .context("S1 representative tile is unreadable")?;
    let verified = fetch_tile(http, bases[1], output_source, tile)
        .await
        .context("S2 verify representative tile is unreadable")?;
    let public = fetch_tile(http, bases[2], output_source, tile)
        .await
        .context("S2 public representative tile is unreadable")?;
    ensure!(
        input == verified,
        "S2 verify representative tile differs from S1"
    );
    ensure!(
        input == public,
        "S2 public representative tile differs from S1"
    );
    Ok(input)
}

async fn fetch_tile(
    http: &Client,
    base: &str,
    source: &str,
    tile: &RepresentativeTile,
) -> anyhow::Result<Vec<u8>> {
    let mut response = http
        .get(format!("{base}/{source}/{}", tile.path()))
        .header("Accept-Encoding", "identity")
        .send()
        .await?;
    ensure!(
        response.status() == reqwest::StatusCode::OK,
        "representative tile returned {}",
        response.status()
    );
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .context("representative tile response has no Content-Type")?
        .to_str()
        .context("representative tile Content-Type is invalid")?;
    let media_type = content_type.split(';').next().unwrap_or_default().trim();
    ensure!(
        [
            "application/vnd.mapbox-vector-tile",
            "application/x-protobuf",
            "application/protobuf",
            "application/octet-stream",
        ]
        .iter()
        .any(|allowed| media_type.eq_ignore_ascii_case(allowed)),
        "representative response does not have a tile Content-Type"
    );
    let encoding = response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .map(|value| value.to_str().map(str::to_owned))
        .transpose()?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_TILE_BYTES,
            "representative tile exceeds the byte limit"
        );
        body.extend_from_slice(&chunk);
    }
    decode_tile(&body, encoding.as_deref())
}

fn decode_tile(body: &[u8], encoding: Option<&str>) -> anyhow::Result<Vec<u8>> {
    let encoding = encoding.unwrap_or("identity").trim();
    ensure!(
        encoding.eq_ignore_ascii_case("identity") || encoding.eq_ignore_ascii_case("gzip"),
        "unsupported representative tile content encoding"
    );
    let mut decoded = Vec::new();
    if encoding.eq_ignore_ascii_case("gzip") || body.starts_with(&[0x1f, 0x8b]) {
        flate2::read::GzDecoder::new(body)
            .take((MAX_TILE_BYTES + 1) as u64)
            .read_to_end(&mut decoded)
            .context("invalid gzip representative tile")?;
    } else {
        decoded.extend_from_slice(body);
    }
    ensure!(!decoded.is_empty(), "decoded representative tile is empty");
    ensure!(
        decoded.len() <= MAX_TILE_BYTES,
        "decoded representative tile exceeds the byte limit"
    );
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn tile_coordinates_are_bounded_before_any_http_request() {
        assert_eq!(RepresentativeTile::parse("0/0/0").unwrap().path(), "0/0/0");
        for invalid in [
            "",
            "0/1/0",
            "0/0/1",
            "31/0/0",
            "32/0/0",
            "1/-1/0",
            "1/+1/0",
            "1/0",
            "1/0/0/0",
            "1/0/0?x=1",
        ] {
            assert!(RepresentativeTile::parse(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn decodes_gzip_with_or_without_header_and_rejects_empty_or_corrupt_tiles() {
        let bytes = b"\x1a\x03\x0a\x01x";
        assert_eq!(decode_tile(bytes, None).unwrap(), bytes);
        for encoding in [None, Some("gzip"), Some("identity")] {
            assert_eq!(decode_tile(&gzip(bytes), encoding).unwrap(), bytes);
        }
        assert!(decode_tile(&[], None).is_err());
        assert!(decode_tile(&gzip(&[]), Some("gzip")).is_err());
        assert!(decode_tile(b"not gzip", Some("gzip")).is_err());
        assert!(decode_tile(bytes, Some("br")).is_err());
    }

    async fn routes(
        source: ResponseTemplate,
        verify: ResponseTemplate,
        public: ResponseTemplate,
    ) -> MockServer {
        let server = MockServer::start().await;
        for (route, response) in [
            ("/source/s1/0/0/0", source),
            ("/verify/s2/0/0/0", verify),
            ("/public/s2/0/0/0", public),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(response.insert_header("Content-Type", "application/octet-stream"))
                .mount(&server)
                .await;
        }
        server
    }

    async fn proof(server: &MockServer) -> anyhow::Result<Vec<u8>> {
        let source = format!("{}/source", server.uri());
        let verify = format!("{}/verify", server.uri());
        let public = format!("{}/public", server.uri());
        prove_tiles(
            &Client::new(),
            [&source, &verify, &public],
            "s1",
            "s2",
            &RepresentativeTile::parse("0/0/0")?,
        )
        .await
    }

    #[tokio::test]
    async fn both_s2_routes_must_decode_to_the_nonempty_s1_tile() {
        let bytes = b"\x1a\x03\x0a\x01x";
        let server = routes(
            ResponseTemplate::new(200).set_body_bytes(bytes),
            ResponseTemplate::new(200)
                .set_body_bytes(gzip(bytes))
                .insert_header("Content-Encoding", "gzip"),
            ResponseTemplate::new(200).set_body_bytes(bytes),
        )
        .await;
        assert_eq!(proof(&server).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn refuses_mismatch_empty_and_unreadable_routes_before_registration() {
        for failing_route in 0..3 {
            for bad in [
                ResponseTemplate::new(404),
                ResponseTemplate::new(200),
                ResponseTemplate::new(200).set_body_bytes(b"different"),
            ] {
                let response = ResponseTemplate::new(200).set_body_bytes(b"\x1a\x03\x0a\x01x");
                let mut replies = [response.clone(), response.clone(), response];
                replies[failing_route] = bad;
                let server =
                    routes(replies[0].clone(), replies[1].clone(), replies[2].clone()).await;
                assert!(
                    proof(&server).await.is_err(),
                    "route {failing_route} must be proved"
                );
            }
        }
    }

    #[tokio::test]
    async fn identical_html_json_or_untyped_responses_are_not_tile_evidence() {
        for content_type in [Some("text/html"), Some("application/json"), None] {
            let server = MockServer::start().await;
            let mut response = ResponseTemplate::new(200).set_body_bytes(b"same proxy response");
            if let Some(content_type) = content_type {
                response = response.insert_header("Content-Type", content_type);
            }
            Mock::given(method("GET"))
                .respond_with(response)
                .mount(&server)
                .await;
            assert!(
                proof(&server).await.is_err(),
                "{content_type:?} must not be tile evidence"
            );
        }
    }

    #[tokio::test]
    async fn tile_mime_types_allow_case_and_parameters() {
        for content_type in [
            "application/vnd.mapbox-vector-tile",
            "application/x-protobuf",
            "application/protobuf",
            "application/octet-stream",
            "Application/X-Protobuf; charset=binary",
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_raw(b"\x1a\x03\x0a\x01x", content_type),
                )
                .mount(&server)
                .await;
            assert!(proof(&server).await.is_ok(), "{content_type}");
        }
    }
}
