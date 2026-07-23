type Environment = Readonly<Record<string, string | undefined>>;

export type ZitadelE2eCredentials = Readonly<{
  username: string;
  password: string;
}>;

function requireValue(env: Environment, name: string): string {
  const value = env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required when ZITADEL_E2E_REAL=true`);
  }
  return value;
}

export function requireZitadelE2eCredentials(env: Environment): ZitadelE2eCredentials {
  return {
    username: requireValue(env, "ZITADEL_E2E_USERNAME"),
    password: requireValue(env, "ZITADEL_E2E_PASSWORD"),
  };
}
