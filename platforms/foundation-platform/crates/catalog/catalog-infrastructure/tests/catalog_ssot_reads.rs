//! `PostgreSQL` read tests for Catalog SSOT subresources.

#![allow(clippy::expect_used, clippy::print_stderr, clippy::unwrap_used)]

use catalog_application::ports::{CatalogRepository, CatalogUnitOfWork};
use catalog_domain::{IndustrialComplex, IndustrialComplexKind};
use catalog_infrastructure::{PgCatalogRepository, PgCatalogUnitOfWork};
use chrono::Utc;
use foundation_shared_kernel::ids::{ComplexId, ParcelId};
use foundation_shared_kernel::pnu::Pnu;
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    match PgPool::connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skipping - could not connect to DATABASE_URL: {e}");
            None
        }
    }
}

#[tokio::test]
#[ignore = "requires local docker stack"]
async fn repository_reads_industrial_complex_ssot_subresources() {
    let Some(pool) = pool().await else {
        return;
    };
    let repo = PgCatalogRepository::new(pool.clone());
    let uow = PgCatalogUnitOfWork::new(pool.clone());
    let fixture = SsotFixture::new();

    uow.create_complex(&fixture.complex)
        .await
        .expect("create complex");
    fixture.insert(&pool).await;

    assert_core_subresource_reads(&repo, &fixture).await;
    assert_document_subresource_reads(&repo, &fixture).await;
    assert_industry_reads(&repo, &fixture).await;
    assert_canonical_complex_reads(&repo, &fixture).await;

    fixture.cleanup(&pool).await;
}

async fn assert_core_subresource_reads(repo: &PgCatalogRepository, fixture: &SsotFixture) {
    let parcels = repo
        .list_parcels_by_complex(fixture.complex.id)
        .await
        .expect("list parcels");
    assert_eq!(parcels.len(), 1);
    assert_eq!(parcels[0].id, fixture.parcel_id);

    let buildings = repo
        .list_buildings_by_complex(fixture.complex.id)
        .await
        .expect("list buildings");
    assert_eq!(buildings.len(), 1);
    assert_eq!(buildings[0].parcel_id, fixture.parcel_id);
    assert_eq!(buildings[0].purpose_code, "02000");

    let pnu = Pnu::parse(fixture.pnu.clone()).expect("fixture pnu");
    let buildings_by_pnu = repo
        .list_buildings_by_pnu(&pnu)
        .await
        .expect("list buildings by pnu");
    assert_eq!(buildings_by_pnu.len(), 1);
    assert_eq!(buildings_by_pnu[0].parcel_id, fixture.parcel_id);
    assert_eq!(buildings_by_pnu[0].purpose_code, "02000");

    let manufacturers = repo
        .list_manufacturers_by_complex(fixture.complex.id)
        .await
        .expect("list manufacturers");
    assert_eq!(manufacturers.len(), 1);
    assert_eq!(manufacturers[0].primary_parcel_id, fixture.parcel_id);
    assert_eq!(manufacturers[0].name, "Fixture Manufacturing");
}

async fn assert_document_subresource_reads(repo: &PgCatalogRepository, fixture: &SsotFixture) {
    let notices = repo
        .list_complex_notices(fixture.complex.id)
        .await
        .expect("list notices");
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].title, "Supply notice");

    let notice_assets = repo
        .list_notice_file_assets(notices[0].id)
        .await
        .expect("notice attachments");
    assert_eq!(notice_assets.len(), 1);
    assert_eq!(
        notice_assets[0].object_key.as_str(),
        fixture.notice_object_key
    );

    let attachments = repo
        .list_complex_attachments(fixture.complex.id)
        .await
        .expect("complex attachments");
    assert_eq!(attachments.len(), 1);
    assert_eq!(attachments[0].object_key.as_str(), fixture.image_object_key);

    let blueprints = repo
        .list_complex_blueprints(fixture.complex.id)
        .await
        .expect("list blueprints");
    assert_eq!(blueprints.len(), 1);
    assert_eq!(blueprints[0].coordinate_system, "EPSG:5186");

    let layers = repo
        .list_complex_spatial_layers(fixture.complex.id)
        .await
        .expect("list spatial layers");
    assert_eq!(layers.len(), 1);
    assert_eq!(
        layers[0]
            .geometry_object_key
            .as_ref()
            .expect("geometry object key")
            .as_str(),
        fixture.geometry_object_key
    );

    let digital_twins = repo
        .list_complex_digital_twin_assets(fixture.complex.id)
        .await
        .expect("list digital twins");
    assert_eq!(digital_twins.len(), 1);
    assert_eq!(
        digital_twins[0].file_asset_id.as_uuid(),
        fixture.digital_twin_file_asset_id
    );
}

async fn assert_industry_reads(repo: &PgCatalogRepository, fixture: &SsotFixture) {
    let industry_groups = repo
        .list_industry_groups(Some(fixture.complex.id))
        .await
        .expect("list industry groups");
    assert_eq!(industry_groups.len(), 1);
    assert_eq!(industry_groups[0].name, "Advanced manufacturing");

    let members = repo
        .list_industry_group_members_for_complex(fixture.complex.id)
        .await
        .expect("list industry members");
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].industry_code, "29299");

    let assignments = repo
        .list_parcel_industry_assignments(fixture.parcel_id)
        .await
        .expect("list parcel industry assignments");
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].industry_group_id, fixture.industry_group_id);
}

async fn assert_canonical_complex_reads(repo: &PgCatalogRepository, fixture: &SsotFixture) {
    let complexes = repo
        .list_complexes()
        .await
        .expect("list canonical industrial complexes");
    let exported_complex = complexes
        .iter()
        .find(|complex| complex.id == fixture.complex.id)
        .expect("fixture complex must be exportable");
    assert_eq!(
        exported_complex.official_complex_code,
        fixture.complex.official_complex_code
    );
}

struct SsotFixture {
    complex: IndustrialComplex,
    parcel_id: ParcelId,
    pnu: String,
    building_id: Uuid,
    manufacturer_id: Uuid,
    source_record_id: Uuid,
    image_file_asset_id: Uuid,
    notice_file_asset_id: Uuid,
    blueprint_file_asset_id: Uuid,
    digital_twin_file_asset_id: Uuid,
    notice_id: Uuid,
    blueprint_id: Uuid,
    spatial_layer_id: Uuid,
    digital_twin_asset_id: Uuid,
    industry_group_id: foundation_shared_kernel::ids::IndustryGroupId,
    parcel_assignment_id: Uuid,
    image_object_key: &'static str,
    notice_object_key: &'static str,
    geometry_object_key: &'static str,
}

impl SsotFixture {
    fn new() -> Self {
        let now = Utc::now();
        let complex_id = ComplexId::new(Uuid::now_v7());
        let suffix = Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .filter(char::is_ascii_digit)
            .take(10)
            .collect::<String>();
        let primary_bjdong_code = format!("{suffix:0<10}")[..10].to_owned();

        let pnu = format!("{primary_bjdong_code}100010000");

        Self {
            complex: IndustrialComplex {
                id: complex_id,
                official_complex_code: format!("IC-{}", Uuid::new_v4().simple()),
                name: "SSOT read fixture".to_owned(),
                kind: IndustrialComplexKind::General,
                primary_bjdong_code,
                area_m2: 100_000,
                created_at: now,
                updated_at: now,
                archived_at: None,
                version: 1,
            },
            parcel_id: ParcelId::new(Uuid::now_v7()),
            pnu,
            building_id: Uuid::now_v7(),
            manufacturer_id: Uuid::now_v7(),
            source_record_id: Uuid::now_v7(),
            image_file_asset_id: Uuid::now_v7(),
            notice_file_asset_id: Uuid::now_v7(),
            blueprint_file_asset_id: Uuid::now_v7(),
            digital_twin_file_asset_id: Uuid::now_v7(),
            notice_id: Uuid::now_v7(),
            blueprint_id: Uuid::now_v7(),
            spatial_layer_id: Uuid::now_v7(),
            digital_twin_asset_id: Uuid::now_v7(),
            industry_group_id: foundation_shared_kernel::ids::IndustryGroupId::new(Uuid::now_v7()),
            parcel_assignment_id: Uuid::now_v7(),
            image_object_key: "complexes/ssot/official-image.jpg",
            notice_object_key: "complexes/ssot/notices/supply.pdf",
            geometry_object_key: "complexes/ssot/layers/parcel-boundary.geojson",
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn insert(&self, pool: &PgPool) {
        sqlx::query(
            "INSERT INTO catalog.parcel
             (id, complex_id, pnu, kind, area_m2, version)
             VALUES ($1, $2, $3, 'factory', 1200, 1)",
        )
        .bind(self.parcel_id.as_uuid())
        .bind(self.complex.id.as_uuid())
        .bind(&self.pnu)
        .execute(pool)
        .await
        .expect("insert parcel");

        sqlx::query(
            "INSERT INTO catalog.source_record
             (id, source, source_url, external_id, checksum_sha256)
             VALUES ($1, 'fixture', 'https://example.test/source', $2, $3)",
        )
        .bind(self.source_record_id)
        .bind(self.complex.id.to_string())
        .bind("0".repeat(64))
        .execute(pool)
        .await
        .expect("insert source record");

        sqlx::query(
            "INSERT INTO catalog.building
             (id, parcel_id, purpose_code, structure_code, floor_area_m2, stories, built_year)
             VALUES ($1, $2, '02000', '11', 1234.5, 5, 2020)",
        )
        .bind(self.building_id)
        .bind(self.parcel_id.as_uuid())
        .execute(pool)
        .await
        .expect("insert building");

        sqlx::query(
            "INSERT INTO catalog.manufacturer
             (id, primary_parcel_id, name, ksic_code, business_registration_number)
             VALUES ($1, $2, 'Fixture Manufacturing', '29299', '12345678901')",
        )
        .bind(self.manufacturer_id)
        .bind(self.parcel_id.as_uuid())
        .execute(pool)
        .await
        .expect("insert manufacturer");

        self.insert_file_asset(
            pool,
            self.image_file_asset_id,
            self.image_object_key,
            "image/jpeg",
        )
        .await;
        self.insert_file_asset(
            pool,
            self.notice_file_asset_id,
            self.notice_object_key,
            "application/pdf",
        )
        .await;
        self.insert_file_asset(
            pool,
            self.blueprint_file_asset_id,
            "complexes/ssot/blueprints/master.pdf",
            "application/pdf",
        )
        .await;
        self.insert_file_asset(
            pool,
            self.digital_twin_file_asset_id,
            "complexes/ssot/3d/tileset.json",
            "application/json",
        )
        .await;

        sqlx::query(
            "INSERT INTO catalog.complex_attachment
             (complex_id, file_asset_id, asset_kind, display_order)
             VALUES ($1, $2, 'official_image', 1)",
        )
        .bind(self.complex.id.as_uuid())
        .bind(self.image_file_asset_id)
        .execute(pool)
        .await
        .expect("insert complex attachment");

        sqlx::query(
            "INSERT INTO catalog.complex_notice
             (id, complex_id, notice_type, title, summary, source_record_id, version)
             VALUES ($1, $2, 'sale', 'Supply notice', 'fixture notice', $3, 1)",
        )
        .bind(self.notice_id)
        .bind(self.complex.id.as_uuid())
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert notice");

        sqlx::query(
            "INSERT INTO catalog.notice_attachment
             (notice_id, file_asset_id, display_order)
             VALUES ($1, $2, 1)",
        )
        .bind(self.notice_id)
        .bind(self.notice_file_asset_id)
        .execute(pool)
        .await
        .expect("insert notice attachment");

        sqlx::query(
            "INSERT INTO catalog.blueprint
             (id, complex_id, file_asset_id, blueprint_kind, coordinate_system, scale, source_record_id, version)
             VALUES ($1, $2, $3, 'master_plan', 'EPSG:5186', '1:5000', $4, 1)",
        )
        .bind(self.blueprint_id)
        .bind(self.complex.id.as_uuid())
        .bind(self.blueprint_file_asset_id)
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert blueprint");

        sqlx::query(
            "INSERT INTO catalog.spatial_layer
             (id, complex_id, parcel_id, blueprint_id, layer_kind, geometry_object_key, source_record_id, version)
             VALUES ($1, $2, $3, $4, 'parcel_boundary', $5, $6, 1)",
        )
        .bind(self.spatial_layer_id)
        .bind(self.complex.id.as_uuid())
        .bind(self.parcel_id.as_uuid())
        .bind(self.blueprint_id)
        .bind(self.geometry_object_key)
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert spatial layer");

        sqlx::query(
            "INSERT INTO catalog.digital_twin_asset
             (id, complex_id, parcel_id, file_asset_id, asset_kind, coordinate_transform, source_record_id, version)
             VALUES ($1, $2, $3, $4, 'tileset_3d', '{\"epsg\":5186}'::jsonb, $5, 1)",
        )
        .bind(self.digital_twin_asset_id)
        .bind(self.complex.id.as_uuid())
        .bind(self.parcel_id.as_uuid())
        .bind(self.digital_twin_file_asset_id)
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert digital twin");

        sqlx::query(
            "INSERT INTO catalog.industry_group
             (id, complex_id, name, description, version)
             VALUES ($1, $2, 'Advanced manufacturing', 'fixture group', 1)",
        )
        .bind(self.industry_group_id.as_uuid())
        .bind(self.complex.id.as_uuid())
        .execute(pool)
        .await
        .expect("insert industry group");

        sqlx::query(
            "INSERT INTO catalog.industry_group_member
             (industry_group_id, industry_code, industry_code_system)
             VALUES ($1, '29299', 'ksic')",
        )
        .bind(self.industry_group_id.as_uuid())
        .execute(pool)
        .await
        .expect("insert industry member");

        sqlx::query(
            "INSERT INTO catalog.parcel_industry_assignment
             (id, parcel_id, industry_group_id, assignment_kind, source_record_id, version)
             VALUES ($1, $2, $3, 'allowed', $4, 1)",
        )
        .bind(self.parcel_assignment_id)
        .bind(self.parcel_id.as_uuid())
        .bind(self.industry_group_id.as_uuid())
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert parcel assignment");
    }

    async fn insert_file_asset(
        &self,
        pool: &PgPool,
        file_asset_id: Uuid,
        object_key: &str,
        mime_type: &str,
    ) {
        sqlx::query(
            "INSERT INTO catalog.file_asset
             (id, object_key, mime_type, size_bytes, source_record_id, visibility, version)
             VALUES ($1, $2, $3, 10, $4, 'internal', 1)",
        )
        .bind(file_asset_id)
        .bind(object_key)
        .bind(mime_type)
        .bind(self.source_record_id)
        .execute(pool)
        .await
        .expect("insert file asset");
    }

    async fn cleanup(&self, pool: &PgPool) {
        sqlx::query("DELETE FROM catalog.parcel_industry_assignment WHERE id = $1")
            .bind(self.parcel_assignment_id)
            .execute(pool)
            .await
            .expect("cleanup parcel assignment");
        sqlx::query("DELETE FROM catalog.industry_group WHERE id = $1")
            .bind(self.industry_group_id.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup industry group");
        sqlx::query("DELETE FROM catalog.digital_twin_asset WHERE id = $1")
            .bind(self.digital_twin_asset_id)
            .execute(pool)
            .await
            .expect("cleanup digital twin");
        sqlx::query("DELETE FROM catalog.spatial_layer WHERE id = $1")
            .bind(self.spatial_layer_id)
            .execute(pool)
            .await
            .expect("cleanup spatial layer");
        sqlx::query("DELETE FROM catalog.blueprint WHERE id = $1")
            .bind(self.blueprint_id)
            .execute(pool)
            .await
            .expect("cleanup blueprint");
        sqlx::query("DELETE FROM catalog.complex_notice WHERE id = $1")
            .bind(self.notice_id)
            .execute(pool)
            .await
            .expect("cleanup notice");
        sqlx::query("DELETE FROM catalog.complex_attachment WHERE complex_id = $1")
            .bind(self.complex.id.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup complex attachment");
        sqlx::query("DELETE FROM catalog.file_asset WHERE source_record_id = $1")
            .bind(self.source_record_id)
            .execute(pool)
            .await
            .expect("cleanup file assets");
        sqlx::query("DELETE FROM catalog.source_record WHERE id = $1")
            .bind(self.source_record_id)
            .execute(pool)
            .await
            .expect("cleanup source record");
        sqlx::query("DELETE FROM catalog.manufacturer WHERE id = $1")
            .bind(self.manufacturer_id)
            .execute(pool)
            .await
            .expect("cleanup manufacturer");
        sqlx::query("DELETE FROM catalog.building WHERE id = $1")
            .bind(self.building_id)
            .execute(pool)
            .await
            .expect("cleanup building");
        sqlx::query("DELETE FROM catalog.parcel WHERE id = $1")
            .bind(self.parcel_id.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup parcel");
        sqlx::query("DELETE FROM catalog.industrial_complex WHERE id = $1")
            .bind(self.complex.id.as_uuid())
            .execute(pool)
            .await
            .expect("cleanup complex");
    }
}
