-- One 시도's run of gate.sql, added to the edition's totals: the counts, the first SAMPLE items of each
-- reason (the first SAMPLE of every run hold the edition's first SAMPLE), and the ids held, which the
-- module groups once over every run (an id two parcels share may span two 시도).
INSERT INTO gate_acc_counts
SELECT (SELECT count(*) FROM gate_scan), (SELECT count(*) FROM gate_well), (SELECT count(*) FROM gate_malformed),
       (SELECT count(*) FROM gate_stray), (SELECT count(*) FROM gate_moved), (SELECT count(*) FROM gate_no_boundary),
       (SELECT count(*) FROM gate_no_current);

INSERT INTO gate_acc_malformed SELECT pnu FROM gate_malformed ORDER BY pnu LIMIT {SAMPLE};
INSERT INTO gate_acc_stray SELECT n FROM gate_stray ORDER BY n LIMIT {SAMPLE};
INSERT INTO gate_acc_no_boundary SELECT n FROM gate_no_boundary ORDER BY n LIMIT {SAMPLE};
INSERT INTO gate_acc_no_current SELECT n FROM gate_no_current ORDER BY n LIMIT {SAMPLE};
INSERT INTO gate_acc_ids SELECT parcel_id FROM gate_held WHERE parcel_id IS NOT NULL;
