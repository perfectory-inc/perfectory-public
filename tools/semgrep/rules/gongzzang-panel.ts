// Fixture for tools/semgrep/rules/gongzzang-panel.yml (`opengrep scan --test`). Not compiled.
// ruleid: gongzzang-panel-framework-does-not-import-kinds
import "@/components/panels/parcel/register";
// ruleid: gongzzang-panel-framework-does-not-import-kinds
import ParcelPanel from "@/components/panels/parcel/panel";
// ruleid: gongzzang-panel-framework-does-not-import-kinds
import { registerParcel } from "@/components/panels/parcel/register";
// ruleid: gongzzang-panel-framework-does-not-import-kinds
import * as kinds from "@/components/panels/index";
// ruleid: gongzzang-panel-framework-does-not-import-kinds
export { listingKind } from "@/components/panels/listing/kind";
// ok: gongzzang-panel-framework-does-not-import-kinds
import { parsePanelPath } from "@/lib/panel/codec";
// ok: gongzzang-panel-framework-does-not-import-kinds
import { PanelCard } from "@/components/panel-card";

export async function loadKind() {
  // ruleid: gongzzang-panel-framework-does-not-import-kinds
  return import("@/components/panels/parcel/register");
}

// ruleid: gongzzang-panel-url-grammar-stays-in-the-codec
export const parts = (s: string) => s.split(">");
// ruleid: gongzzang-panel-url-grammar-stays-in-the-codec
export const quoted = (s: string) => s.split('>');
// ok: gongzzang-panel-url-grammar-stays-in-the-codec
export const words = (s: string) => s.split(" ");
// ok: gongzzang-panel-url-grammar-stays-in-the-codec
export const note = "split('>') belongs to the codec";

// ruleid: gongzzang-panel-stack-is-not-held-in-a-store
export const panelStack = [];
export const useStore = create(() => ({
  // ruleid: gongzzang-panel-stack-is-not-held-in-a-store
  panelStack: [],
  // ok: gongzzang-panel-stack-is-not-held-in-a-store
  selected: null,
}));
// ruleid: gongzzang-panel-stack-is-not-held-in-a-store
export const top = (state: State) => state.panelStack.at(-1);
