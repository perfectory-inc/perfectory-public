-- 한국어 형태소 경계로 미리 쪼갠 색인 입력을 별도 칼럼에 둔다.
--
-- 이전 정의는 `to_tsvector('simple', heading_path || ' ' || body)`였다. `simple`은 공백으로만
-- 자르므로 조사가 명사에 붙은 채 한 토큰이 되고("건폐율을"), 질의 "건폐율"이 아무것도
-- 맞히지 못했다. 고시문은 거의 모든 명사가 조사를 달고 나오므로 그 상태로는 검색이
-- 성립하지 않는다.
--
-- 형태소 분석은 Rust 쪽(lindera + mecab-ko-dic)이 색인 전에 수행하고 그 결과를
-- `search_text`에 넣는다. Postgres는 공백으로 자르기만 한다. 이렇게 하면 커스텀
-- Postgres 확장을 깔지 않고도 Elasticsearch Nori와 같은 사전을 쓴다.
--
-- `body`는 원문 그대로 남긴다 — 검색 결과로 보여 줄 것은 형태소가 아니라 원문이다.

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN IF NOT EXISTS search_text TEXT NOT NULL DEFAULT '';

-- 생성 칼럼의 식은 바꿀 수 없으므로 지우고 다시 만든다. 인덱스도 함께 사라지므로
-- 다시 만든다. 이 표에는 아직 운영 데이터가 없다.
ALTER TABLE ip_knowledge_chunk DROP COLUMN IF EXISTS search_vector;

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN search_vector tsvector
    GENERATED ALWAYS AS (to_tsvector('simple', search_text)) STORED;

CREATE INDEX IF NOT EXISTS idx_ip_knowledge_chunk_search
    ON ip_knowledge_chunk USING GIN (search_vector);
