export const PNU_PATTERN = /^\d{19}$/;
export const LISTING_ID_PATTERN = /^lst_[0-9A-HJKMNP-TV-Z]{26}$/;
/**
 * The lakehouse identity of an industrial complex, as a lowercase hyphenated UUIDv5.
 *
 * Pinned to version 5 on purpose. A complex carries two identifiers that are not computable from
 * each other: `catalog.industrial_complex.id`, minted with `Uuid::now_v7()`, and the lakehouse
 * `complex_id`, derived as a UUIDv5 in the Bronze-to-Silver job. The second one is what the
 * `complex` vector tile publishes as its feature id (`feature_id_property: "complex_id"`) and what
 * the panel opens on, so a v7 value arriving here means the two id spaces were mixed — which this
 * pattern turns into a rejected panel entry instead of a silent 404.
 *
 * The same invariant is enforced server-side by
 * `shared_kernel::lakehouse_complex_id::LakehouseComplexId` and, in the database, by the
 * `industrial_complex_lakehouse_complex_id_is_uuid_v5` and
 * `complex_boundary_publication_complex_id_is_uuid_v5` CHECK constraints.
 */
export const LAKEHOUSE_COMPLEX_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
