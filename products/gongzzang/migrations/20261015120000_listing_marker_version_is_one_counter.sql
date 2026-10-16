-- The listing marker projection version is one counter that only goes up (root ADR-0257).
--
-- It was max(projection_version) over per-listing rows: a new listing (row version 1) or
-- withdrawing the highest-version listing did not raise it, so the map and the
-- `base_version` delta/tombstone queries missed those changes.
--
-- Every projection change now takes `update listing_marker_state set version = version + 1
-- returning version` in its own transaction and stamps that value on the projection, delta
-- and tombstone rows. The row lock is held until commit, so versions commit in order: a
-- reader that sees version N also sees every change at or below N.

create table listing_marker_state (
    singleton boolean primary key default true,
    version bigint not null,
    constraint listing_marker_state_singleton_chk check (singleton),
    constraint listing_marker_state_version_chk check (version >= 0)
);

-- Start above every version already handed to a client, so an old `base_version` still
-- sees every change made after this migration.
insert into listing_marker_state (version)
select greatest(
    0,
    (select max(projection_version) from listing_marker_projection),
    (select max(projection_version) from listing_marker_delta_log),
    (select max(projection_version) from listing_marker_tombstone_log)
);

grant select, update on listing_marker_state to gongzzang_app_writer;
grant select on listing_marker_state to gongzzang_app_reader;
