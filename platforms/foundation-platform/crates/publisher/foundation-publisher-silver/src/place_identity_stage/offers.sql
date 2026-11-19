-- What each row's address offers the address joins, and every PNU the run asks the served parcels
-- about (root ADR-0288 §1, ADR-0482 §2). It was `address_pnu.candidates` (Python's regexes over the
-- text) and `place_identity_resolver.candidate_pnus`; reference.sql runs first.
--
-- Input: place_rows(_row BIGINT, pnu VARCHAR, address VARCHAR): a format-valid source PNU or null,
-- and the address text.
-- Output: place_offers(_row, jibun_pnu, reason, road_sido, road_sigungu, road_name, road_main_no,
-- road_sub_no), place_asked(pnu), place_roads(road_name).

-- Python's \s, \S and \d in a text pattern: Unicode whitespace (str.isspace) and decimal digits.
CREATE OR REPLACE MACRO place_s() AS '[' || py_ws() || ']';
CREATE OR REPLACE MACRO place_ns() AS '[^' || py_ws() || ']';
-- int() of a run of Python digits (the full-width ones are the ones a Korean text holds).
CREATE OR REPLACE MACRO place_int(t) AS TRY_CAST(translate(t, '０１２３４５６７８９', '0123456789') AS BIGINT);

-- The length of the text before the first match of `re` (its group 1 is what comes before), or the
-- whole text's when it does not match.
CREATE OR REPLACE MACRO place_cut(a, re) AS
  CASE WHEN regexp_matches(a, re) THEN length(regexp_extract(a, re, 1)) ELSE length(a) END;

-- address_words, step one: re.split(r"[(,]|\s외\s|(?<=\d)외|외(?=\d)", address, maxsplit=1)[0], the
-- text before the leftmost of the four.
CREATE OR REPLACE MACRO place_head(a) AS
  left(a, least(place_cut(a, '(?s)^(.*?)[(,]'),
                place_cut(a, '(?s)^(.*?)' || place_s() || '외' || place_s()),
                place_cut(a, '(?s)^(.*?\p{Nd})외'),
                place_cut(a, '(?s)^(.*?)외\p{Nd}')));

-- Step two: re.sub(r"(?<=\s)산(?=\d)", "산 ", head).
CREATE OR REPLACE MACRO place_mountain_split(h) AS
  regexp_replace(h, '(' || place_s() || ')산(\p{Nd})', '\1산 \2', 'g');

-- Step three, per word: re.sub(r"(?<=\S시)(?=\S+구(?:\s|$))", " ", …): a word that ends in 구 is cut
-- after every 시 that has a letter before it and at least two after it (천안시서북구 -> 천안시 서북구).
CREATE OR REPLACE MACRO place_city_cuts(w) AS
  CASE WHEN suffix(w, '구') THEN
    list_filter(range(2, length(w) - 1), lambda i: substr(w, i, 1) = '시')
  ELSE []::BIGINT[] END;
CREATE OR REPLACE MACRO place_city_pieces(w, cuts) AS
  list_transform(range(1, len(cuts) + 2), lambda k:
    substr(w, ([0] || cuts || [length(w)])[k] + 1, ([0] || cuts || [length(w)])[k + 1] - ([0] || cuts || [length(w)])[k]));

-- address_words: the words before any parenthesis, comma or 외, with 산 split from a glued lot and a
-- 시 split from its 구.
CREATE OR REPLACE MACRO place_address_words(a) AS
  flatten(list_transform(place_words(place_mountain_split(place_head(a))),
                         lambda w: place_city_pieces(w, place_city_cuts(w))));

-- The register's road address: 시도, 시군구 words, 도로명, 건물번호, then anything. Spark read it with Java's
-- regex (\s is [ \t\n\x0B\f\r], `.` stops at a line end) and cast '' to null.
CREATE OR REPLACE MACRO place_register_road() AS
  '^[^ \t\n\x0B\f\r]+[ \t\n\x0B\f\r]+([^\n\r\x{85}\x{2028}\x{2029}]+?)[ \t\n\x0B\f\r]+([^ \t\n\x0B\f\r]+(?:로|길))[ \t\n\x0B\f\r]+([0-9]+)(?:-([0-9]+))?';

CREATE OR REPLACE MACRO place_lot_re() AS '^(산)?(\p{Nd}+)(?:-(\p{Nd}+))?(?:번지)?$';
CREATE OR REPLACE MACRO place_lot_word(w, i) AS
  CASE WHEN w[i] = '산' AND i + 1 <= len(w) THEN w[i + 1] ELSE w[i] END;

CREATE OR REPLACE TEMP TABLE place_words_of AS
SELECT _row, pnu, coalesce(address, '') AS address, place_address_words(coalesce(address, '')) AS words FROM place_rows;

-- sido_code: the 시도 a first word starts with; the longest prefix wins (two prefixes of one length
-- that both start a word are the same prefix).
CREATE OR REPLACE TEMP TABLE place_sido_of AS
SELECT w._row, arg_max(p.sido, length(p.prefix)) AS sido
FROM place_words_of w JOIN place_sido_prefixes p ON len(w.words) > 0 AND starts_with(w.words[1], p.prefix)
GROUP BY w._row;

CREATE OR REPLACE TEMP TABLE place_parsed AS
SELECT _row, pnu, address, words, sido,
       -- jibun_key: the first word from the third on that reads as a lot.
       CASE WHEN len(words) >= 3 AND sido IS NOT NULL THEN
         list_filter(range(3, len(words) + 1), lambda i: regexp_matches(place_lot_word(words, i), place_lot_re()))[1]
       END AS lot_at,
       -- road_key: the first word from the third on that is a road with a building number after it.
       CASE WHEN len(words) >= 4 AND sido IS NOT NULL THEN
         list_filter(range(3, len(words)), lambda i:
           length(words[i]) >= 2 AND (suffix(words[i], '로') OR suffix(words[i], '길'))
           AND regexp_matches(words[i + 1], '^(\p{Nd}+)(?:-(\p{Nd}+))?$'))[1]
       END AS road_at
FROM place_words_of LEFT JOIN place_sido_of USING (_row);

CREATE OR REPLACE TEMP TABLE place_keys AS
SELECT _row, pnu, address, sido,
       -- A number after a road name is a building number, not a lot: the place before it must end
       -- in 읍·면·동·리·가.
       CASE WHEN lot_at IS NOT NULL AND regexp_matches(words[lot_at - 1], '(읍|면|동|리|가)$') THEN true
            WHEN lot_at IS NOT NULL THEN false END AS jibun_read,
       CASE WHEN lot_at IS NOT NULL THEN array_to_string(words[2:lot_at - 1], ' ') END AS jibun_place,
       CASE WHEN lot_at IS NOT NULL THEN
         words[lot_at] = '산' OR regexp_extract(place_lot_word(words, lot_at), place_lot_re(), 1) <> '' END AS mountain,
       CASE WHEN lot_at IS NOT NULL THEN place_int(regexp_extract(place_lot_word(words, lot_at), place_lot_re(), 2)) END AS bonbun,
       CASE WHEN lot_at IS NOT NULL THEN coalesce(place_int(nullif(regexp_extract(place_lot_word(words, lot_at), place_lot_re(), 3), '')), 0) END AS bubun,
       CASE WHEN road_at IS NOT NULL THEN
         array_to_string(list_filter(words[2:road_at - 1], lambda w: suffix(w, '시') OR suffix(w, '군') OR suffix(w, '구')), ' ') END AS road_sigungu,
       CASE WHEN road_at IS NOT NULL THEN words[road_at] END AS road_name,
       CASE WHEN road_at IS NOT NULL THEN place_int(regexp_extract(words[road_at + 1], '^(\p{Nd}+)(?:-(\p{Nd}+))?$', 1)) END AS road_main_no,
       CASE WHEN road_at IS NOT NULL THEN coalesce(place_int(nullif(regexp_extract(words[road_at + 1], '^(\p{Nd}+)(?:-(\p{Nd}+))?$', 2), '')), 0) END AS road_sub_no
FROM place_parsed;

-- candidates: a jibun PNU when the address reads as a lot (its 법정동 by name), else a road key, else why
-- neither. A lot over 9999 names no PNU and no reason, as jibun_pnu returned None.
CREATE OR REPLACE TEMP TABLE place_offers AS
SELECT k._row,
       CASE WHEN k.jibun_read AND d.code IS NOT NULL AND k.bonbun <= 9999 AND k.bubun <= 9999
            THEN d.code || CASE WHEN k.mountain THEN '2' ELSE '1' END || lpad(CAST(k.bonbun AS VARCHAR), 4, '0')
                 || lpad(CAST(k.bubun AS VARCHAR), 4, '0') END AS jibun_pnu,
       CASE WHEN trim(k.address, py_ws()) = '' THEN 'no_address'
            WHEN k.jibun_read AND d.code IS NULL THEN 'no_legal_dong'
            WHEN k.jibun_read THEN NULL
            WHEN k.road_name IS NOT NULL AND k.road_sigungu <> '' THEN NULL
            ELSE 'unparsed' END AS reason,
       CASE WHEN trim(k.address, py_ws()) <> '' AND k.jibun_read IS NOT true AND k.road_sigungu <> '' THEN k.sido END AS road_sido,
       CASE WHEN trim(k.address, py_ws()) <> '' AND k.jibun_read IS NOT true AND k.road_sigungu <> '' THEN k.road_sigungu END AS road_sigungu,
       CASE WHEN trim(k.address, py_ws()) <> '' AND k.jibun_read IS NOT true AND k.road_sigungu <> '' THEN k.road_name END AS road_name,
       CASE WHEN trim(k.address, py_ws()) <> '' AND k.jibun_read IS NOT true AND k.road_sigungu <> '' THEN k.road_main_no END AS road_main_no,
       CASE WHEN trim(k.address, py_ws()) <> '' AND k.jibun_read IS NOT true AND k.road_sigungu <> '' THEN k.road_sub_no END AS road_sub_no
FROM place_keys k LEFT JOIN place_dongs d ON d.sido = k.sido AND d.place = k.jibun_place AND k.jibun_read;

-- candidate_pnus: each code carried to its current legal-dong code, and its earlier forms; a code that
-- splits asks about nothing. Asked for the source PNUs and the jibun PNUs alike.
CREATE OR REPLACE TEMP TABLE place_asked AS
WITH asked AS (
  SELECT pnu FROM place_rows WHERE pnu IS NOT NULL
  UNION SELECT jibun_pnu FROM place_offers WHERE jibun_pnu IS NOT NULL
), open AS (
  SELECT pnu FROM asked WHERE left(pnu, 10) NOT IN (SELECT code FROM place_ambiguous)
)
SELECT coalesce(r.current_code, left(o.pnu, 10)) || substr(o.pnu, 11) AS pnu
FROM open o LEFT JOIN place_remap r ON r.old_code = left(o.pnu, 10)
UNION
SELECT f.form10 || substr(o.pnu, 11) FROM open o JOIN place_forms f ON f.code10 = left(o.pnu, 10);

CREATE OR REPLACE TEMP TABLE place_roads AS
SELECT DISTINCT road_name FROM place_offers WHERE road_name IS NOT NULL;
