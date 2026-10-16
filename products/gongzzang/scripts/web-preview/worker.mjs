// Built by `opennextjs-cloudflare build` next to this file (see deploy.sh).
import nextWorker from "./.open-next/worker.js";
import { relayRequest } from "./edge-relay.mjs";

export default {
  async fetch(request, env, ctx) {
    const relayed = relayRequest(request);
    if (relayed instanceof Request) return fetch(relayed);
    if (relayed) return relayed;
    return nextWorker.fetch(request, env, ctx);
  },
};
