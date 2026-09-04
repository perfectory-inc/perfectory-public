function principalKind(ctx, api) { var user = ctx.v1.getUser(); var kind = (user !== undefined && user.machine !== undefined) ? "service" : "staff"; api.v1.claims.setClaim("principal_kind", kind); }
