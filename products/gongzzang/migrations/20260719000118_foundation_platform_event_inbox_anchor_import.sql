-- Durable inbound Foundation Platform event inbox and anchor importer contract.
--
-- `parcel_marker_anchor` remains a Gongzzang-local read model copied from
-- Foundation Platform. The inbox records Foundation Platform webhook events by event id so
-- replays are idempotent and import failures are inspectable.

alter table parcel_marker_anchor
    alter column algorithm_version type varchar(128);

create table foundation_platform_event_inbox (
    event_id uuid primary key,
    event_type varchar(128) not null,
    scope varchar(32) not null,
    effect varchar(64) not null,
    status varchar(32) not null,
    payload jsonb not null,
    anchor_snapshot_id varchar(128),
    source_geometry_version varchar(128),
    received_at timestamptz not null default now(),
    processed_at timestamptz,
    failed_at timestamptz,
    failure_reason text,
    constraint foundation_platform_event_inbox_scope_chk
        check (scope = 'catalog'),
    constraint foundation_platform_event_inbox_status_chk
        check (status in ('accepted', 'pending_import', 'processing', 'processed', 'failed')),
    constraint foundation_platform_event_inbox_effect_chk
        check (effect in ('invalidate_catalog_cache', 'enqueue_anchor_projection_import')),
    constraint foundation_platform_event_inbox_anchor_payload_chk
        check (
            event_type <> 'catalog.parcel_marker_anchor.snapshot.published.v1'
            or (
                anchor_snapshot_id is not null
                and source_geometry_version is not null
                and effect = 'enqueue_anchor_projection_import'
            )
        )
);

create index foundation_platform_event_inbox_pending_idx
    on foundation_platform_event_inbox(event_type, received_at)
    where status = 'pending_import';

create index foundation_platform_event_inbox_anchor_snapshot_idx
    on foundation_platform_event_inbox(anchor_snapshot_id)
    where anchor_snapshot_id is not null;
