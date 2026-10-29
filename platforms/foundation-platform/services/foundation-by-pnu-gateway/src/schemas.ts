import registry from "../../../contracts/schema-registry/registry.json";

/// The schema registry (root ADR-0336), bundled when the Worker is built. A lane whose subject it
/// lists serves documents of the subject's majors only: the manifest's record of the document
/// schema (`section_packs.document_schema_version`, and a patch's own) must be one of the major's
/// strings, `<subject>.v<major>` or a legacy alias, whichever revision it is. The Worker reads no
/// field of a document; it is the deployed reader the publisher asks before a new major goes out,
/// at the capabilities path (can-i-deploy).
interface Revision {
  major: number;
  schema_version: string;
}

export interface LaneSubject {
  subject: string;
  majors: readonly number[];
  /// Every `schema_version` a document of one of the majors carries.
  strings: ReadonlySet<string>;
}

/// The registered subject `lane` owns, `null` when the registry lists none.
export function subjectOfLane(lane: string): LaneSubject | null {
  const entry = registry.subjects.find((subject) => subject.owner.lane === lane);
  if (entry === undefined) return null;
  const revisions = entry.revisions as readonly Revision[];
  const majors = [...new Set(revisions.map((revision) => revision.major))];
  const strings = new Set<string>();
  for (const major of majors) {
    strings.add(`${entry.subject}.v${major}`);
    for (const revision of revisions) if (revision.major === major) strings.add(revision.schema_version);
  }
  return { subject: entry.subject, majors, strings };
}

/// Whether a `section_packs` block records only documents of the lane's subject's majors; a lane
/// the registry does not list is not held to it.
export function recordsReadableDocuments(lane: string, block: Record<string, unknown>): boolean {
  const subject = subjectOfLane(lane);
  if (subject === null) return true;
  const recorded = [block.document_schema_version];
  for (const patch of Array.isArray(block.patches) ? (block.patches as unknown[]) : []) {
    const own = (patch as { document_schema_version?: unknown } | null)?.document_schema_version;
    if (own !== undefined) recorded.push(own);
  }
  return recorded.every((version) => typeof version === "string" && subject.strings.has(version));
}

/// The capabilities answer's `document_schemas`: each registered subject a lane serves, with the
/// majors this build reads.
export function documentSchemas(lane: string): Record<string, readonly number[]> {
  const subject = subjectOfLane(lane);
  return subject === null ? {} : { [subject.subject]: subject.majors };
}
