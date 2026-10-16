-- 조각의 제목 경로를 목록으로도 보관한다.
--
-- 지금까지는 제목들을 ` > `로 이은 문자열(`heading_path`)만 저장했고 검색 결과도 그 문자열을
-- 돌려줬다. 소비자가 제목 하나하나를 쓰려면 구분자로 되쪼개야 했고, 제목 안에 ` > `가 있으면
-- 틀리게 쪼개진다. 이제 원문 목록(`headings`)을 저장하고 결과는 그것을 읽는다.
--
-- `heading_path`는 남는다: 제목 검색 신호(`search_vector_heading`)의 입력이다. 같은 쓰기에서
-- `headings`로부터 만들어지고 결과로 읽히지 않는다 — 검색용 파생 칸이지 두 번째 정본이 아니다.
--
-- 기존 행은 이은 문자열을 한 번 쪼개 채운다. 다음 재색인이 원문 목록으로 덮어쓴다.

ALTER TABLE ip_knowledge_chunk
    ADD COLUMN IF NOT EXISTS headings TEXT[];

UPDATE ip_knowledge_chunk
SET headings = CASE
    WHEN heading_path = '' THEN ARRAY[]::TEXT[]
    ELSE string_to_array(heading_path, ' > ')
END
WHERE headings IS NULL;

ALTER TABLE ip_knowledge_chunk
    ALTER COLUMN headings SET NOT NULL;
