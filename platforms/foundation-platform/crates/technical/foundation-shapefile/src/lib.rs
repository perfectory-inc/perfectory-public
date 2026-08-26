//! Streaming access to zipped shapefile datasets owned by Foundation Platform.

use std::{
    collections::BTreeMap,
    fmt,
    io::{Cursor, Read, Seek},
};

use anyhow::{bail, Context};
use encoding_rs::Encoding;
use geo_types::MultiPolygon;
use proj4rs::{proj::Proj, transform::transform};
use serde_json::{json, Value as JsonValue};
use shapefile::{
    dbase::{self, encoding::EncodingRs, FieldValue},
    Shape, ShapeReader,
};
use zip::ZipArchive;

const EPSG_4326_PROJ: &str = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs";
const SUPPORTED_CENTRAL_MERIDIANS: [f64; 4] = [125.0, 127.0, 129.0, 131.0];
const REQUIRED_MEMBER_EXTENSIONS: [&str; 5] = ["shp", "shx", "dbf", "prj", "cpg"];

type InMemoryShapefileReader = shapefile::Reader<Cursor<Vec<u8>>, Cursor<Vec<u8>>>;

/// Metadata established before any feature is emitted from a zipped shapefile dataset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShapefileMetadata {
    /// Basename shared by the shapefile members.
    pub dataset_name: String,
    /// Exact nonblank label read from the `.cpg` member.
    pub cpg_label: String,
    /// Projected CRS name read from the `.prj` WKT root.
    pub source_crs_name: String,
    /// Shape count declared by the `.shx` index.
    pub shape_count: u64,
    /// Record count declared by the `.dbf` header.
    pub dbf_record_count: u64,
    /// Sum of uncompressed bytes held for `.shp`, `.shx`, and `.dbf` seekable readers.
    pub seekable_member_bytes: u64,
}

/// One shapefile geometry and its matching dBase attribute record.
#[derive(Clone, Debug, PartialEq)]
pub struct ShapefileFeature {
    record: dbase::Record,
    geometry: JsonValue,
}

impl ShapefileFeature {
    /// Reads a nullable dBase character field without guessing its type.
    ///
    /// Blank and null cells return `None`; an absent column or a non-character column is a schema
    /// error.
    ///
    /// # Errors
    /// Returns an error when the field is absent or not a character field.
    pub fn optional_text(&self, field_name: &str) -> anyhow::Result<Option<&str>> {
        match self.record.get(field_name) {
            Some(FieldValue::Character(Some(value))) => {
                let trimmed = value.trim();
                Ok((!trimmed.is_empty()).then_some(trimmed))
            }
            Some(FieldValue::Character(None)) => Ok(None),
            None => bail!("shapefile field {field_name} is absent from the DBF schema"),
            Some(_) => bail!("shapefile field {field_name} is not character data"),
        }
    }

    /// Reads a required nonblank dBase character field without guessing its type.
    ///
    /// # Errors
    /// Returns an error when the field is absent, null, blank, or not a character field.
    pub fn required_text(&self, field_name: &str) -> anyhow::Result<&str> {
        self.optional_text(field_name)?
            .with_context(|| format!("required shapefile field {field_name} is null or blank"))
    }

    /// Returns the EPSG:4326 polygonal `GeoJSON` geometry for this feature.
    #[must_use]
    pub const fn geometry(&self) -> &JsonValue {
        &self.geometry
    }
}

/// A direct-from-ZIP, feature-at-a-time shapefile reader.
pub struct ZipShapefileReader {
    reader: InMemoryShapefileReader,
    projection: SourceProjection,
    metadata: ShapefileMetadata,
}

impl fmt::Debug for ZipShapefileReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ZipShapefileReader")
            .field("metadata", &self.metadata)
            .finish_non_exhaustive()
    }
}

impl ZipShapefileReader {
    /// Opens exactly one complete shapefile dataset from a ZIP source.
    ///
    /// Compressed members are never extracted to disk. The three readers that require seeking
    /// (`.shp`, `.shx`, and `.dbf`) are decompressed into bounded per-member buffers; features are
    /// subsequently decoded and emitted one at a time.
    ///
    /// # Errors
    /// Returns an error for an invalid ZIP, missing/ambiguous members, unknown CPG, unsupported
    /// PRJ, invalid shapefile headers, or mismatched shape/DBF counts.
    pub fn new<R>(source: R) -> anyhow::Result<Self>
    where
        R: Read + Seek,
    {
        let mut archive = ZipArchive::new(source).context("failed to open shapefile ZIP")?;
        let members = discover_members(&mut archive)?;
        let shp = read_member(&mut archive, members.index("shp")?)?;
        let shx = read_member(&mut archive, members.index("shx")?)?;
        let dbf = read_member(&mut archive, members.index("dbf")?)?;
        let prj = read_text_member(&mut archive, members.index("prj")?, "PRJ")?;
        let cpg_label = read_text_member(&mut archive, members.index("cpg")?, "CPG")?;
        let encoding = encoding_from_cpg(&cpg_label)?;
        let projection = SourceProjection::from_prj(&prj)?;

        let shape_reader = ShapeReader::with_shx(Cursor::new(shp), Cursor::new(shx))
            .context("failed to open SHP/SHX members")?;
        let shape_count = u64::try_from(
            shape_reader
                .shape_count()
                .context("failed to count SHX shapes")?,
        )
        .context("shape count overflow")?;
        let dbase_reader = dbase::ReaderBuilder::new()
            .with_encoding(EncodingRs::from(encoding))
            .build(Cursor::new(dbf))
            .context("failed to open DBF member with CPG encoding")?;
        let dbf_record_count = u64::from(dbase_reader.header().num_records);
        if shape_count != dbf_record_count {
            bail!(
                "shapefile shape/DBF count mismatch: shapes={shape_count}, dbf_records={dbf_record_count}"
            );
        }
        let seekable_member_bytes = members.seekable_member_bytes;
        let metadata = ShapefileMetadata {
            dataset_name: members.dataset_name,
            cpg_label,
            source_crs_name: projection.source_name().to_owned(),
            shape_count,
            dbf_record_count,
            seekable_member_bytes,
        };

        Ok(Self {
            reader: shapefile::Reader::new(shape_reader, dbase_reader),
            projection,
            metadata,
        })
    }

    /// Returns immutable source metadata established when the archive was opened.
    #[must_use]
    pub const fn metadata(&self) -> &ShapefileMetadata {
        &self.metadata
    }

    /// Visits each matching shape/record pair without collecting the feature set.
    ///
    /// # Errors
    /// Returns an error when a source row cannot be read or transformed, when the visitor fails,
    /// or when iteration does not produce the counts declared by SHX/DBF metadata.
    pub fn for_each_feature<F>(&mut self, mut visitor: F) -> anyhow::Result<u64>
    where
        F: FnMut(ShapefileFeature) -> anyhow::Result<()>,
    {
        let projection = &self.projection;
        let mut count = 0_u64;
        for result in self.reader.iter_shapes_and_records() {
            let (shape, record) = result.with_context(|| {
                format!("failed to read shapefile feature at zero-based index {count}")
            })?;
            let geometry = shape_to_epsg4326_geojson(projection, shape).with_context(|| {
                format!("failed to transform shapefile feature at zero-based index {count}")
            })?;
            visitor(ShapefileFeature { record, geometry }).with_context(|| {
                format!("shapefile feature visitor failed at zero-based index {count}")
            })?;
            count += 1;
        }
        if count != self.metadata.shape_count || count != self.metadata.dbf_record_count {
            bail!(
                "shapefile iteration count mismatch: visited={count}, shapes={}, dbf_records={}",
                self.metadata.shape_count,
                self.metadata.dbf_record_count
            );
        }
        Ok(count)
    }
}

#[derive(Debug)]
struct ArchiveMembers {
    dataset_name: String,
    indexes: BTreeMap<String, usize>,
    seekable_member_bytes: u64,
}

impl ArchiveMembers {
    fn index(&self, extension: &str) -> anyhow::Result<usize> {
        self.indexes
            .get(extension)
            .copied()
            .with_context(|| format!("shapefile ZIP is missing .{extension} member"))
    }
}

fn discover_members<R>(archive: &mut ZipArchive<R>) -> anyhow::Result<ArchiveMembers>
where
    R: Read + Seek,
{
    let mut datasets = BTreeMap::<String, BTreeMap<String, (usize, u64)>>::new();
    for index in 0..archive.len() {
        let entry = archive
            .by_index(index)
            .with_context(|| format!("failed to inspect ZIP member {index}"))?;
        if entry.is_dir() {
            continue;
        }
        let Some((base, extension)) = entry.name().rsplit_once('.') else {
            continue;
        };
        let extension = extension.to_ascii_lowercase();
        if !REQUIRED_MEMBER_EXTENSIONS.contains(&extension.as_str()) {
            continue;
        }
        let entries = datasets.entry(base.to_owned()).or_default();
        if entries
            .insert(extension.clone(), (index, entry.size()))
            .is_some()
        {
            bail!("shapefile ZIP contains duplicate .{extension} member for {base}");
        }
    }
    let mut complete = datasets
        .into_iter()
        .filter(|(_, members)| {
            REQUIRED_MEMBER_EXTENSIONS
                .iter()
                .all(|extension| members.contains_key(*extension))
        })
        .collect::<Vec<_>>();
    if complete.len() != 1 {
        bail!(
            "shapefile ZIP must contain exactly one complete .shp/.shx/.dbf/.prj/.cpg dataset; found {}",
            complete.len()
        );
    }
    let (base, raw_indexes) = complete.pop().context("complete dataset disappeared")?;
    let dataset_name = base
        .rsplit(['/', '\\'])
        .next()
        .filter(|name| !name.is_empty())
        .context("shapefile dataset basename is empty")?
        .to_owned();
    let seekable_member_bytes = ["shp", "shx", "dbf"]
        .into_iter()
        .map(|extension| raw_indexes[extension].1)
        .sum();
    let indexes = raw_indexes
        .into_iter()
        .map(|(extension, (index, _))| (extension, index))
        .collect();
    Ok(ArchiveMembers {
        dataset_name,
        indexes,
        seekable_member_bytes,
    })
}

fn read_member<R>(archive: &mut ZipArchive<R>, index: usize) -> anyhow::Result<Vec<u8>>
where
    R: Read + Seek,
{
    let mut entry = archive
        .by_index(index)
        .with_context(|| format!("failed to open ZIP member {index}"))?;
    let capacity =
        usize::try_from(entry.size()).context("ZIP member size exceeds address space")?;
    let mut bytes = Vec::with_capacity(capacity);
    entry
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read ZIP member {}", entry.name()))?;
    if bytes.len() != capacity {
        bail!(
            "ZIP member {} length mismatch: declared={capacity}, read={}",
            entry.name(),
            bytes.len()
        );
    }
    Ok(bytes)
}

fn read_text_member<R>(
    archive: &mut ZipArchive<R>,
    index: usize,
    label: &str,
) -> anyhow::Result<String>
where
    R: Read + Seek,
{
    let bytes = read_member(archive, index)?;
    let value = std::str::from_utf8(&bytes)
        .with_context(|| format!("{label} member must be UTF-8/ASCII text"))?
        .trim_start_matches('\u{feff}')
        .trim();
    if value.is_empty() {
        bail!("{label} member must not be blank");
    }
    Ok(value.to_owned())
}

fn encoding_from_cpg(label: &str) -> anyhow::Result<&'static Encoding> {
    let normalized = label.trim();
    let encoding = match normalized.to_ascii_lowercase().as_str() {
        "949" | "cp949" | "ms949" => Some(encoding_rs::EUC_KR),
        _ => Encoding::for_label(normalized.as_bytes()),
    };
    encoding.with_context(|| format!("unsupported CPG encoding label: {normalized}"))
}

fn shape_to_epsg4326_geojson(
    projection: &SourceProjection,
    shape: Shape,
) -> anyhow::Result<JsonValue> {
    let polygons: MultiPolygon<f64> = match shape {
        Shape::Polygon(polygon) => polygon
            .try_into()
            .context("failed to convert Polygon rings")?,
        Shape::PolygonM(polygon) => polygon
            .try_into()
            .context("failed to convert PolygonM rings")?,
        Shape::PolygonZ(polygon) => polygon
            .try_into()
            .context("failed to convert PolygonZ rings")?,
        _ => bail!("shapefile feature geometry must be Polygon, PolygonM, or PolygonZ"),
    };
    let coordinates = polygons
        .0
        .into_iter()
        .map(|polygon| {
            let (exterior, interiors) = polygon.into_inner();
            std::iter::once(exterior)
                .chain(interiors)
                .map(|ring| {
                    ring.0
                        .into_iter()
                        .map(|coordinate| {
                            projection
                                .to_epsg4326(coordinate.x, coordinate.y)
                                .map(|(longitude, latitude)| vec![longitude, latitude])
                        })
                        .collect::<anyhow::Result<Vec<_>>>()
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(json!({
        "type": "MultiPolygon",
        "coordinates": coordinates
    }))
}

/// A validated Korea 2000 2010 belt projection read from a shapefile `.prj` member.
pub struct SourceProjection {
    source_name: String,
    source: Proj,
    target: Proj,
}

impl fmt::Debug for SourceProjection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceProjection")
            .field("source_name", &self.source_name)
            .finish_non_exhaustive()
    }
}

impl SourceProjection {
    /// Parses and validates a shapefile WKT projection definition.
    ///
    /// # Errors
    /// Returns an error when WKT parsing fails or the CRS is not one of the GRS80 Korea 2000
    /// 2010 Transverse Mercator belts used by `VWorld` spatial-file exports.
    pub fn from_prj(wkt: &str) -> anyhow::Result<Self> {
        let source_name =
            projected_crs_name(wkt).context("PRJ is not a supported Korea 2000 2010 belt")?;
        let proj_string = proj4wkt::wkt_to_projstring(wkt)
            .context("failed to parse shapefile PRJ as WKT1/WKT2")?;
        validate_supported_korea_belt(&proj_string)?;
        let source = Proj::from_proj_string(&proj_string)
            .context("failed to initialize source projection from PRJ")?;
        let target = Proj::from_proj_string(EPSG_4326_PROJ)
            .context("failed to initialize EPSG:4326 target projection")?;
        Ok(Self {
            source_name,
            source,
            target,
        })
    }

    /// Returns the CRS name declared at the root of the `.prj` WKT.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// Reprojects one source easting/northing coordinate into EPSG:4326 longitude/latitude.
    ///
    /// # Errors
    /// Returns an error when projection fails or produces non-finite coordinates.
    pub fn to_epsg4326(&self, easting: f64, northing: f64) -> anyhow::Result<(f64, f64)> {
        let mut coordinate = (easting, northing, 0.0_f64);
        transform(&self.source, &self.target, &mut coordinate)
            .context("coordinate transformation to EPSG:4326 failed")?;
        let longitude = coordinate.0.to_degrees();
        let latitude = coordinate.1.to_degrees();
        if !longitude.is_finite() || !latitude.is_finite() {
            bail!("coordinate transformation to EPSG:4326 produced non-finite output");
        }
        Ok((longitude, latitude))
    }
}

fn projected_crs_name(wkt: &str) -> anyhow::Result<String> {
    let trimmed = wkt.trim_start_matches('\u{feff}').trim();
    let prefix = ["PROJCS[\"", "PROJCRS[\""]
        .into_iter()
        .find(|prefix| trimmed.starts_with(prefix))
        .context("PRJ must declare a projected CRS")?;
    let rest = &trimmed[prefix.len()..];
    let end = rest
        .find('"')
        .context("PRJ projected CRS name is not closed")?;
    let name = &rest[..end];
    if name.is_empty() {
        bail!("PRJ projected CRS name must not be empty");
    }
    Ok(name.to_owned())
}

fn validate_supported_korea_belt(proj_string: &str) -> anyhow::Result<()> {
    let params = proj_parameters(proj_string);
    let supported = params.get("proj").is_some_and(|value| *value == "tmerc")
        && numeric_param_is(&params, "lat_0", 38.0)
        && (numeric_param_is(&params, "k", 1.0) || numeric_param_is(&params, "k_0", 1.0))
        && numeric_param_is(&params, "x_0", 200_000.0)
        && numeric_param_is(&params, "y_0", 600_000.0)
        && numeric_param_is(&params, "a", 6_378_137.0)
        && numeric_param_is(&params, "rf", 298.257_222_101)
        && numeric_param_in(&params, "lon_0", &SUPPORTED_CENTRAL_MERIDIANS)
        && params
            .get("towgs84")
            .is_some_and(|value| *value == "0,0,0,0,0,0,0");
    if !supported {
        bail!(
            "PRJ is not a supported Korea 2000 2010 belt (GRS80 tmerc, lon_0 125/127/129/131); parsed definition: {proj_string}"
        );
    }
    Ok(())
}

fn proj_parameters(proj_string: &str) -> BTreeMap<&str, &str> {
    proj_string
        .split_ascii_whitespace()
        .filter_map(|part| part.strip_prefix('+')?.split_once('='))
        .collect()
}

fn numeric_param_is(params: &BTreeMap<&str, &str>, name: &str, expected: f64) -> bool {
    params
        .get(name)
        .and_then(|value| value.parse::<f64>().ok())
        .is_some_and(|actual| (actual - expected).abs() < 1e-9)
}

fn numeric_param_in(params: &BTreeMap<&str, &str>, name: &str, expected: &[f64]) -> bool {
    params
        .get(name)
        .and_then(|value| value.parse::<f64>().ok())
        .is_some_and(|actual| {
            expected
                .iter()
                .any(|candidate| (actual - candidate).abs() < 1e-9)
        })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write as _};

    use super::{SourceProjection, ZipShapefileReader};
    use anyhow::bail;
    use shapefile::{
        dbase::{self, encoding::EncodingRs, FieldName, FieldValue},
        Point, Polygon, PolygonRing, ShapeWriter, Writer,
    };
    use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

    const KOREA_CENTRAL_BELT_2010: &str = concat!(
        r#"PROJCS["Korea_2000_Korea_Central_Belt_2010","#,
        r#"GEOGCS["GCS_Korea_2000",DATUM["D_Korea_2000","#,
        r#"SPHEROID["GRS_1980",6378137.0,298.257222101]],"#,
        r#"PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],"#,
        r#"PROJECTION["Transverse_Mercator"],"#,
        r#"PARAMETER["False_Easting",200000.0],"#,
        r#"PARAMETER["False_Northing",600000.0],"#,
        r#"PARAMETER["Central_Meridian",127.0],"#,
        r#"PARAMETER["Scale_Factor",1.0],"#,
        r#"PARAMETER["Latitude_Of_Origin",38.0],UNIT["Meter",1.0]]"#,
    );

    #[test]
    fn central_belt_false_origin_maps_to_declared_longitude_and_latitude() -> anyhow::Result<()> {
        let projection = SourceProjection::from_prj(KOREA_CENTRAL_BELT_2010)?;

        let (longitude, latitude) = projection.to_epsg4326(200_000.0, 600_000.0)?;

        assert!((longitude - 127.0).abs() < 1e-8, "longitude={longitude}");
        assert!((latitude - 38.0).abs() < 1e-8, "latitude={latitude}");
        Ok(())
    }

    #[test]
    fn every_korea_2000_2010_belt_origin_is_accepted() -> anyhow::Result<()> {
        for central_meridian in [125.0_f64, 127.0, 129.0, 131.0] {
            let prj = KOREA_CENTRAL_BELT_2010.replace(
                r#"PARAMETER["Central_Meridian",127.0]"#,
                &format!(r#"PARAMETER["Central_Meridian",{central_meridian:.1}]"#),
            );
            let projection = SourceProjection::from_prj(&prj)?;
            let (longitude, latitude) = projection.to_epsg4326(200_000.0, 600_000.0)?;

            assert!(
                (longitude - central_meridian).abs() < 1e-8,
                "central_meridian={central_meridian}, longitude={longitude}"
            );
            assert!((latitude - 38.0).abs() < 1e-8, "latitude={latitude}");
        }
        Ok(())
    }

    #[test]
    fn pyproj_golden_coordinates_guard_projection_regressions() -> anyhow::Result<()> {
        let projection = SourceProjection::from_prj(KOREA_CENTRAL_BELT_2010)?;
        let cases = [
            (
                392_496.179_031_357_87,
                410_159.621_975_078_36,
                129.142_145_037_217_6,
                36.270_233_874_260_01,
            ),
            (
                419_772.708_181_663_9,
                544_153.508_251_463_4,
                129.484_227_180_631_05,
                37.470_716_064_375_72,
            ),
            (
                406_134.443_606_510_9,
                477_156.565_113_270_9,
                129.311_726_799_184_44,
                36.870_672_528_988_02,
            ),
        ];
        for (easting, northing, expected_longitude, expected_latitude) in cases {
            let (longitude, latitude) = projection.to_epsg4326(easting, northing)?;

            assert!((longitude - expected_longitude).abs() < 8e-9);
            assert!((latitude - expected_latitude).abs() < 8e-9);
        }
        Ok(())
    }

    #[test]
    fn geographic_or_unrecognized_prj_is_rejected() -> anyhow::Result<()> {
        let Err(error) = SourceProjection::from_prj(
            r#"GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]]]"#,
        ) else {
            bail!("a non-belt CRS must fail closed");
        };

        assert!(error.to_string().contains("supported Korea 2000 2010 belt"));
        Ok(())
    }

    #[test]
    fn zipped_dataset_uses_cpg_and_visits_shape_record_pairs_incrementally() -> anyhow::Result<()> {
        let bytes = fixture_zip("EUC-KR", KOREA_CENTRAL_BELT_2010)?;
        let mut reader = ZipShapefileReader::new(Cursor::new(bytes))?;
        assert_eq!(reader.metadata().dataset_name, "parcel");
        assert_eq!(reader.metadata().cpg_label, "EUC-KR");
        assert_eq!(
            reader.metadata().source_crs_name,
            "Korea_2000_Korea_Central_Belt_2010"
        );

        let mut seen = Vec::new();
        let feature_count = reader.for_each_feature(|feature| {
            seen.push((
                feature.required_text("PNU")?.to_owned(),
                feature.required_text("JIBUN")?.to_owned(),
                feature.geometry().clone(),
            ));
            Ok(())
        })?;

        assert_eq!(feature_count, 2);
        assert_eq!(seen[0].0, "4777038029105800001");
        assert_eq!(seen[0].1, "산 580-1");
        assert_eq!(seen[0].2["type"], "MultiPolygon");
        let first = &seen[0].2["coordinates"][0][0][0];
        assert!((first[0].as_f64().unwrap_or_default() - 127.0).abs() < 1e-8);
        assert!((first[1].as_f64().unwrap_or_default() - 38.0).abs() < 1e-8);
        assert_eq!(seen[1].0, "4777038029105810000");
        Ok(())
    }

    #[test]
    fn unknown_cpg_fails_instead_of_guessing() -> anyhow::Result<()> {
        let bytes = fixture_zip("NOT-A-REAL-CODEPAGE", KOREA_CENTRAL_BELT_2010)?;
        let Err(error) = ZipShapefileReader::new(Cursor::new(bytes)) else {
            bail!("unknown CPG must fail closed");
        };

        assert!(error.to_string().contains("unsupported CPG encoding label"));
        Ok(())
    }

    fn fixture_zip(cpg: &str, prj: &str) -> anyhow::Result<Vec<u8>> {
        let mut shp = Cursor::new(Vec::new());
        let mut shx = Cursor::new(Vec::new());
        let mut dbf = Cursor::new(Vec::new());
        {
            let shape_writer = ShapeWriter::with_shx(&mut shp, &mut shx);
            let dbase_writer =
                dbase::TableWriterBuilder::with_encoding(EncodingRs::from(encoding_rs::EUC_KR))
                    .add_character_field(
                        FieldName::try_from("PNU").map_err(|error| anyhow::anyhow!(error))?,
                        19,
                    )
                    .add_character_field(
                        FieldName::try_from("JIBUN").map_err(|error| anyhow::anyhow!(error))?,
                        100,
                    )
                    .build_with_dest(&mut dbf);
            let mut writer = Writer::new(shape_writer, dbase_writer);

            let first_shape = square(200_000.0, 600_000.0);
            let first_record = record("4777038029105800001", "산 580-1");
            writer.write_shape_and_record(&first_shape, &first_record)?;
            let second_shape = square(200_100.0, 600_100.0);
            let second_record = record("4777038029105810000", "581");
            writer.write_shape_and_record(&second_shape, &second_record)?;
        }

        let mut zip = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        for (name, bytes) in [
            ("parcel.shp", shp.into_inner()),
            ("parcel.shx", shx.into_inner()),
            ("parcel.dbf", dbf.into_inner()),
            ("parcel.prj", prj.as_bytes().to_vec()),
            ("parcel.cpg", cpg.as_bytes().to_vec()),
        ] {
            zip.start_file(name, options)?;
            zip.write_all(&bytes)?;
        }
        Ok(zip.finish()?.into_inner())
    }

    fn square(min_x: f64, min_y: f64) -> Polygon {
        Polygon::new(PolygonRing::Outer(vec![
            Point::new(min_x, min_y),
            Point::new(min_x, min_y + 10.0),
            Point::new(min_x + 10.0, min_y + 10.0),
            Point::new(min_x + 10.0, min_y),
            Point::new(min_x, min_y),
        ]))
    }

    fn record(pnu: &str, jibun: &str) -> dbase::Record {
        let mut record = dbase::Record::default();
        record.insert(
            "PNU".to_owned(),
            FieldValue::Character(Some(pnu.to_owned())),
        );
        record.insert(
            "JIBUN".to_owned(),
            FieldValue::Character(Some(jibun.to_owned())),
        );
        record
    }
}
