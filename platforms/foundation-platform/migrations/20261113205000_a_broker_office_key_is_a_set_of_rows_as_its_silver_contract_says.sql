-- A 부동산중개업 office key holds a set of current rows, as silver.real_estate_broker_office's
-- contract says (`keep_ties`, root ADR-0550): the provider's file repeats 10 (시군구, 등록번호)
-- pairs within one 시군구 (2026-11-11 release, measured 2026-11-13). Three of them are two offices
-- under different names, so one current row per pair could not hold the file, and every roster load
-- from 2026-11-12 failed on `real_estate_broker_office_current_key`. A current row is unique by its
-- key and the columns the roster loads; two Silver rows that load the same are one roster row (the
-- loader drops the copy).

DROP INDEX catalog.real_estate_broker_office_current_key;

CREATE UNIQUE INDEX real_estate_broker_office_current_row
    ON catalog.real_estate_broker_office (
        sigungu_code,
        registration_no,
        md5(coalesce(office_name, E'\N') || E'\t' || coalesce(status_code, E'\N') || E'\t'
            || coalesce(status_name, E'\N') || E'\t' || coalesce(data_reference_date, E'\N'))
    )
    WHERE retired_by_load_id IS NULL;

CREATE INDEX real_estate_broker_office_current_key
    ON catalog.real_estate_broker_office (sigungu_code, registration_no) WHERE retired_by_load_id IS NULL;
