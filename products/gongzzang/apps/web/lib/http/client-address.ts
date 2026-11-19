/**
 * 요청을 보낸 주소 — 속도 제한 열쇠. 호출자가 고를 수 있는 값은 쓰지 않는다.
 *
 * - Cloudflare Workers 런타임(`navigator.userAgent === "Cloudflare-Workers"`, Workers 문서
 *   「Web standards — navigator.userAgent」): 모든 요청이 Cloudflare 앞문을 지나고, Cloudflare 는
 *   접속한 주소를 `CF-Connecting-IP` 로 준다. Cloudflare 문서 「HTTP headers」는 클라이언트 주소로
 *   `X-Forwarded-For` 대신 이것을 보라고 한다(XFF 는 호출자가 보낸 값 뒤에 덧붙이기만 한다).
 * - 그 밖(로컬, 다른 앞문): `X-Forwarded-For` 의 **마지막** 항목 — 바로 앞 프록시가 덧붙인 것이다.
 *   첫 항목은 호출자가 보낸 글자 그대로라 요청마다 새 열쇠를 고를 수 있었다(결함 W-2).
 *   앞문이 두 겹이면(CDN 뒤 부하분산기) 마지막 항목은 CDN 의 주소라 여러 사람이 한 열쇠를 나눈다:
 *   우회가 아니라 더 일찍 막히는 쪽으로 틀린다.
 */
export function clientAddress(headers: Headers): string {
  if (runsOnCloudflareWorkers()) {
    const connecting = headers.get("cf-connecting-ip")?.trim();
    if (connecting) return connecting;
  }
  const hops = headers.get("x-forwarded-for")?.split(",") ?? [];
  const nearest = hops[hops.length - 1]?.trim();
  return nearest || "unknown";
}

function runsOnCloudflareWorkers(): boolean {
  return typeof navigator !== "undefined" && navigator.userAgent === "Cloudflare-Workers";
}
