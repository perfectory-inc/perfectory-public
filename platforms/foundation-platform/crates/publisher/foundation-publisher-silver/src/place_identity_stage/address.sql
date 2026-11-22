-- The macros that read a Korean address (root ADR-0288 §1): `offers.sql` reads each row's address
-- with them, and a source that parses a lot itself (the court auction search rows) uses the same
-- pattern. Needs `python_values.sql` (py_ws).

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
