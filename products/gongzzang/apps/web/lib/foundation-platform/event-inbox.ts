import { getRedis } from "@/lib/session/redis";

const INBOX_KEY_PREFIX = "foundation-platform:event-inbox:";
const INBOX_TTL_SECONDS = 60 * 60 * 24 * 30;

export type FoundationPlatformEventInboxStatus = "processing" | "accepted" | "dead_letter";

export interface FoundationPlatformEventInboxRecord {
  event_id: string;
  event_type: string;
  scope: string;
  status: FoundationPlatformEventInboxStatus;
  first_seen_at: string;
  updated_at: string;
  effect?: string;
  reason?: string;
}

export type FoundationPlatformEventInboxReservation =
  | { status: "started"; record: FoundationPlatformEventInboxRecord }
  | { status: "duplicate"; record: FoundationPlatformEventInboxRecord };

export async function reserveFoundationPlatformEvent(input: {
  event_id: string;
  event_type: string;
  scope: string;
}): Promise<FoundationPlatformEventInboxReservation> {
  const now = new Date().toISOString();
  const record: FoundationPlatformEventInboxRecord = {
    event_id: input.event_id,
    event_type: input.event_type,
    scope: input.scope,
    status: "processing",
    first_seen_at: now,
    updated_at: now,
  };
  const key = inboxKey(input.event_id);
  const created = await getRedis().set(key, JSON.stringify(record), "EX", INBOX_TTL_SECONDS, "NX");
  if (created === "OK") {
    return { status: "started", record };
  }

  const existing = await getFoundationPlatformEventInboxRecord(input.event_id);
  if (existing) {
    return { status: "duplicate", record: existing };
  }

  return { status: "started", record };
}

export async function recordFoundationPlatformEventAccepted(input: {
  event_id: string;
  event_type: string;
  scope: string;
  effect: string;
}): Promise<FoundationPlatformEventInboxRecord> {
  return writeFoundationPlatformEventRecord({
    event_id: input.event_id,
    event_type: input.event_type,
    scope: input.scope,
    status: "accepted",
    effect: input.effect,
  });
}

export async function recordFoundationPlatformEventDeadLetter(input: {
  event_id: string;
  event_type: string;
  scope: string;
  reason: string;
}): Promise<FoundationPlatformEventInboxRecord> {
  return writeFoundationPlatformEventRecord({
    event_id: input.event_id,
    event_type: input.event_type,
    scope: input.scope,
    status: "dead_letter",
    reason: input.reason,
  });
}

export async function releaseFoundationPlatformEventReservation(eventId: string): Promise<void> {
  const existing = await getFoundationPlatformEventInboxRecord(eventId);
  if (existing?.status === "processing") {
    await getRedis().del(inboxKey(eventId));
  }
}

export async function getFoundationPlatformEventInboxRecord(
  eventId: string,
): Promise<FoundationPlatformEventInboxRecord | undefined> {
  const raw = await getRedis().get(inboxKey(eventId));
  if (!raw) {
    return undefined;
  }
  return JSON.parse(raw) as FoundationPlatformEventInboxRecord;
}

async function writeFoundationPlatformEventRecord(
  input: Omit<FoundationPlatformEventInboxRecord, "first_seen_at" | "updated_at">,
): Promise<FoundationPlatformEventInboxRecord> {
  const existing = await getFoundationPlatformEventInboxRecord(input.event_id);
  const now = new Date().toISOString();
  const record: FoundationPlatformEventInboxRecord = {
    ...input,
    first_seen_at: existing?.first_seen_at ?? now,
    updated_at: now,
  };
  await getRedis().set(inboxKey(input.event_id), JSON.stringify(record), "EX", INBOX_TTL_SECONDS);
  return record;
}

function inboxKey(eventId: string): string {
  return `${INBOX_KEY_PREFIX}${eventId}`;
}
