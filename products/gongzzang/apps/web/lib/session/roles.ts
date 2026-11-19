import type { SessionData } from "./store";

/** 관문 규칙 하나: `required` 가운데 하나라도 세션에 있으면 통과. 빈 `required` 는 역할을 묻지 않는다. */
export function hasAnyRole(
  session: Pick<SessionData, "roles">,
  required: readonly string[],
): boolean {
  return required.length === 0 || required.some((role) => session.roles.includes(role));
}
