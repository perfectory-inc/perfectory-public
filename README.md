# perfectory

산업용 부동산 사업 모노레포. 수평 플랫폼 3개 위에 제품이 올라간다.

| 영역 | 경로 | 역할 |
|---|---|---|
| Gongzzang | `products/gongzzang` | B2C 산업용 부동산 정보 서비스 (Rust API + Next.js) |
| Foundation Platform | `platforms/foundation-platform` | 산업단지·필지·건물 카탈로그 SSOT, 레이크하우스, 지도 타일 |
| Identity Platform | `platforms/identity-platform` | 직원/서비스 인증·인가, 정책 결정 API |
| Intelligence Platform | `platforms/intelligence-platform` | LLM 정규화 제안 엔진 (Foundation에 proposal-only) |

- 규칙·컨벤션: [AGENTS.md](./AGENTS.md) → [docs/adr/0001](./docs/adr/0001-monorepo-governance-and-conventions.md)
- 각 영역 시작점: 영역 디렉토리의 `README.md` / `AGENTS.md`
- CI: 루트 `.github/workflows/` (PR은 전체 필수 게이트, `main` push는 영역별 path filter)
- Rust 검증(로컬): `bash scripts/verify/cargo-verify.sh <area-dir>` (Docker 필요)

## License

이 저장소는 공개 열람 가능한 **독점 소프트웨어**이며 오픈소스가 아닙니다.
GitHub 서비스 안의 열람·fork에 필요한 권리는 GitHub 이용약관을 따르며, 그 범위를
넘는 사용·수정·배포 권리는 별도 서면 계약 없이는 부여되지 않습니다.
전체 조건은 [LICENSE](./LICENSE), 제3자 고지는
[THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md)를 확인하세요.
