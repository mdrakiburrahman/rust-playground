export interface AzureAccount {
  readonly tenantId: string;
  readonly subscriptionId?: string;
  readonly subscriptionName?: string;
  readonly state?: string;
}

interface AzureAccountJson {
  readonly tenantId?: unknown;
  readonly id?: unknown;
  readonly name?: unknown;
  readonly state?: unknown;
}

export function verifyAzureAccount(
  json: string,
  expectedTenant: string,
): AzureAccount {
  let value: AzureAccountJson;
  try {
    value = JSON.parse(json) as AzureAccountJson;
  } catch {
    throw new Error("Azure CLI account output was not valid JSON.");
  }

  if (typeof value.tenantId !== "string" || value.tenantId.length === 0) {
    throw new Error("Azure CLI account output did not include tenantId.");
  }

  if (value.tenantId.toLowerCase() !== expectedTenant.toLowerCase()) {
    throw new Error(
      `Azure CLI is signed in to tenant ${value.tenantId}, not the requested tenant ${expectedTenant}.`,
    );
  }

  return {
    tenantId: value.tenantId,
    subscriptionId: typeof value.id === "string" ? value.id : undefined,
    subscriptionName: typeof value.name === "string" ? value.name : undefined,
    state: typeof value.state === "string" ? value.state : undefined,
  };
}
