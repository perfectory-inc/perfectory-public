//! JSONL coordinate projection probe used by the independent pyproj oracle.

use std::{
    env, fs,
    io::{self, BufRead as _, Write as _},
};

use anyhow::{bail, Context};
use foundation_shapefile::SourceProjection;
use serde_json::Value as JsonValue;

fn main() -> anyhow::Result<()> {
    let mut args = env::args_os();
    let _program = args.next();
    let prj_path = args
        .next()
        .context("usage: project_coordinates <source.prj>")?;
    if args.next().is_some() {
        bail!("usage: project_coordinates <source.prj>");
    }
    let prj = fs::read_to_string(&prj_path)
        .with_context(|| format!("failed to read PRJ {}", prj_path.to_string_lossy()))?;
    let projection = SourceProjection::from_prj(&prj)?;
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for (index, line) in stdin.lock().lines().enumerate() {
        let line_number = index + 1;
        let line = line.with_context(|| format!("failed to read input line {line_number}"))?;
        let coordinate: JsonValue = serde_json::from_str(&line)
            .with_context(|| format!("input line {line_number} is not JSON"))?;
        let pair = coordinate
            .as_array()
            .filter(|values| values.len() == 2)
            .with_context(|| format!("input line {line_number} must be [easting,northing]"))?;
        let easting = pair[0]
            .as_f64()
            .with_context(|| format!("input line {line_number} easting must be numeric"))?;
        let northing = pair[1]
            .as_f64()
            .with_context(|| format!("input line {line_number} northing must be numeric"))?;
        let (longitude, latitude) = projection.to_epsg4326(easting, northing)?;
        let coordinate: [f64; 2] = (longitude, latitude).into();
        serde_json::to_writer(&mut stdout, &coordinate)
            .context("failed to serialize projected coordinate")?;
        stdout.write_all(b"\n")?;
    }
    stdout.flush()?;
    Ok(())
}
