-- 검색 신호를 칼럼별로 분리한다.
--
-- 이전 정의는 형태소 토큰과 원문을 `search_text` 한 칸에 섞어 하나의 tsvector로 만들었다.
-- 그러면 신호가 서로를 가린다: 제목이 정확히 맞은 문서가 긴 본문에 희석되고, 원문 토큰이
-- 맞은 것과 형태소가 맞은 것을 구분할 수 없다. 순위를 매기는 자가 하나뿐이라 그 자가
-- 틀리면 대안이 없다.
--
-- 조사한 프로덕션 사례들은 신호마다 별도의 순위 목록을 만들고 질의 시점에 합친다
-- (docs/reference/knowledge-search-industry-cases.md). 이 마이그레이션은 그 분리를 위한
-- 자리를 만든다. 합치는 규칙은 knowledge-domain의 `reciprocal_rank_fusion`이 소유한다.
--
-- 세 신호:
--   search_text_morph  형태소 경계로 쪼갠 제목+본문  — 조사가 붙은 명사를 잡는다
--   search_text_raw    원문 그대로의 제목+본문        — 사전에 없는 단어를 잡는다
--   heading_path       제목 경로 (기존 칼럼)          — 제목 일치는 강한 신호다
--
-- `body`는 계속 원문 그대로다. 화면에 보여 줄 것은 형태소가 아니다.
-- 이 표에는 아직 운영 데이터가 없으므로 이전 칼럼을 남기지 않는다. 같은 뜻을 담은 칸이
-- 둘이면 어느 것이 맞는지 판정할 SSOT가 사라진다.

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN IF NOT EXISTS search_text_morph TEXT NOT NULL DEFAULT '';

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN IF NOT EXISTS search_text_raw TEXT NOT NULL DEFAULT '';

DROP INDEX IF EXISTS idx_ip_knowledge_chunk_search;
ALTER TABLE ip_knowledge_chunk DROP COLUMN IF EXISTS search_vector;
ALTER TABLE ip_knowledge_chunk DROP COLUMN IF EXISTS search_text;

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN search_vector_morph tsvector
    GENERATED ALWAYS AS (to_tsvector('simple', search_text_morph)) STORED;

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN search_vector_raw tsvector
    GENERATED ALWAYS AS (to_tsvector('simple', search_text_raw)) STORED;

-- 제목은 짧아 별도 칼럼 없이 생성한다. 형태소 분석을 거치지 않은 원문 제목이며,
-- 제목에는 조사가 붙는 경우가 드물어 원문만으로 충분하다.
ALTER TABLE ip_knowledge_chunk
    ADD COLUMN search_vector_heading tsvector
    GENERATED ALWAYS AS (to_tsvector('simple', heading_path)) STORED;

CREATE INDEX IF NOT EXISTS idx_ip_knowledge_chunk_search_morph
    ON ip_knowledge_chunk USING GIN (search_vector_morph);

CREATE INDEX IF NOT EXISTS idx_ip_knowledge_chunk_search_raw
    ON ip_knowledge_chunk USING GIN (search_vector_raw);

CREATE INDEX IF NOT EXISTS idx_ip_knowledge_chunk_search_heading
    ON ip_knowledge_chunk USING GIN (search_vector_heading);
