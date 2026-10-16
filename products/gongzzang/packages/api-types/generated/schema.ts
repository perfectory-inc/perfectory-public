export type paths = Record<string, never>;
export type webhooks = Record<string, never>;
export interface components {
    schemas: {
        /**
         * @description Card list sort order. The `snake_case` names are the `sort` query values.
         * @enum {string}
         */
        CardSearchSort: "created_at_desc" | "price_asc" | "price_desc" | "area_asc" | "area_desc";
        /**
         * @description 연락처 공개 범위 (3값).
         * @enum {string}
         */
        ContactVisibility: "public" | "login_required" | "verified_only";
        /**
         * @description 매물 상태 (6값).
         * @enum {string}
         */
        ListingStatus: "draft" | "pending_review" | "active" | "sold" | "expired" | "rejected";
        /**
         * @description 매물 유형 (6값).
         * @enum {string}
         */
        ListingType: "factory" | "warehouse" | "office" | "knowledge_industry_center" | "industrial_land" | "logistics_center";
        /**
         * @description 거래 유형 (3값).
         * @enum {string}
         */
        TransactionType: "sale" | "monthly_rent" | "jeonse";
    };
    responses: never;
    parameters: never;
    requestBodies: never;
    headers: never;
    pathItems: never;
}
export type $defs = Record<string, never>;
type FlattenedDeepRequired<T> = {
    [K in keyof T]-?: FlattenedDeepRequired<T[K] extends unknown[] | undefined | null ? Extract<T[K], unknown[]>[number] : T[K]>;
};
type ReadonlyArray<T> = [
    Exclude<T, undefined>
] extends [
    unknown[]
] ? Readonly<Exclude<T, undefined>> : Readonly<Exclude<T, undefined>[]>;
export const cardSearchSortValues: ReadonlyArray<FlattenedDeepRequired<components>["schemas"]["CardSearchSort"]> = ["created_at_desc", "price_asc", "price_desc", "area_asc", "area_desc"];
export const contactVisibilityValues: ReadonlyArray<FlattenedDeepRequired<components>["schemas"]["ContactVisibility"]> = ["public", "login_required", "verified_only"];
export const listingStatusValues: ReadonlyArray<FlattenedDeepRequired<components>["schemas"]["ListingStatus"]> = ["draft", "pending_review", "active", "sold", "expired", "rejected"];
export const listingTypeValues: ReadonlyArray<FlattenedDeepRequired<components>["schemas"]["ListingType"]> = ["factory", "warehouse", "office", "knowledge_industry_center", "industrial_land", "logistics_center"];
export const transactionTypeValues: ReadonlyArray<FlattenedDeepRequired<components>["schemas"]["TransactionType"]> = ["sale", "monthly_rent", "jeonse"];
export type operations = Record<string, never>;
