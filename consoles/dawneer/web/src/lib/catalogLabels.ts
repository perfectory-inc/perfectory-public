// Korean words for the Catalog API's codes and numbers. The codes are the contract; these are
// only how the screen says them.

// The source's own words (`lrstt_ty`), one to one.
export const COMPLEX_KIND_LABEL: Record<string, string> = {
  national: "국가",
  general: "일반",
  urban_high_tech: "도시첨단",
  agricultural: "농공",
};

// Said in the source's own words (VWorld 산업단지 프로필 `make_sttus_nm`): `operating` is what the
// source calls 조성완료 and `planned` covers its 준비중 and 보상중. "운영 중" would claim factories
// are running, which the source does not say.
export const COMPLEX_STATUS_LABEL: Record<string, string> = {
  planned: "준비·보상 중",
  developing: "조성 중",
  operating: "조성 완료",
  changed: "변경",
  abolished: "해제",
  unknown: "알 수 없음",
};

// The source's own words (`lttot_sttus_nm`), one to one.
export const LOT_SALES_LABEL: Record<string, string> = {
  planned: "분양계획",
  in_progress: "분양중",
  completed: "분양완료",
};

export const TILE_UNIT_LABEL: Record<string, string> = {
  admin: "행정경계",
  complex: "산업단지",
  parcel: "필지",
};

/** Two-digit province codes (행정표준코드), for the complex filter. */
export const SIDO_LABEL: Record<string, string> = {
  "11": "서울",
  "26": "부산",
  "27": "대구",
  "28": "인천",
  "29": "광주",
  "30": "대전",
  "31": "울산",
  "36": "세종",
  "41": "경기",
  "43": "충북",
  "44": "충남",
  "46": "전남",
  "47": "경북",
  "48": "경남",
  "50": "제주",
  "51": "강원",
  "52": "전북",
};

/**
 * The word to show for a coded value: the source's own word when the platform carries it
 * (`*_raw`, root ADR-0117 §5), otherwise this screen's table, otherwise the code itself. The
 * tables are a fallback for rows loaded before the words were kept, not a second source of truth.
 */
export function sourceWord(raw: string | null | undefined, table: Record<string, string>, code: string | null | undefined): string {
  if (raw != null && raw.trim() !== "") return raw;
  return label(table, code);
}

/** A code's Korean word, or the code itself when the screen does not know it yet. */
export function label(table: Record<string, string>, code: string | null | undefined): string {
  if (code == null) return "—";
  return table[code] ?? code;
}

/** Bytes as the largest unit that keeps the number at or above one. */
export function formatBytes(bytes: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? value : value.toFixed(1)} ${units[unit]}`;
}

/** Square meters, with the 평 figure people in this trade think in. */
export function formatArea(squareMeters: number): string {
  const pyeong = Math.round(squareMeters / 3.305785);
  return `${squareMeters.toLocaleString("ko-KR")}㎡ (${pyeong.toLocaleString("ko-KR")}평)`;
}

/** How long ago `iso` was, in the largest whole unit. */
export function ago(iso: string, now: Date = new Date()): string {
  const seconds = Math.max(0, Math.round((now.getTime() - new Date(iso).getTime()) / 1000));
  if (seconds < 60) return "방금";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}분 전`;
  const hours = Math.floor(minutes / 60);
  if (hours < 48) return `${hours}시간 전`;
  return `${Math.floor(hours / 24)}일 전`;
}
