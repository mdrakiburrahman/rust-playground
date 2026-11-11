import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Config, SCHEMA_EVOLUTION_GROUP } from './bootstrap.js';

interface ArmResource {
  type: string;
  name: string;
  scope?: string;
  dependsOn?: unknown[];
  condition?: string;
  properties?: Record<string, unknown>;
}
interface ArmTemplate {
  resources?: ArmResource[];
  variables?: Record<string, unknown>;
  outputs?: Record<string, { value?: unknown }>;
}

const [templatePath] = process.argv.slice(2);
assert.ok(templatePath, 'compiled sandbox template path is required');
const template = JSON.parse(fs.readFileSync(templatePath, 'utf8')) as ArmTemplate;
const resources = template.resources ?? [];
const storageReads = resources.find(resource =>
  resource.type === 'Microsoft.Insights/diagnosticSettings' && resource.name === 'arrowcol-storage-reads');
assert.ok(storageReads, 'compiled template must contain the StorageRead diagnostic setting');
const storageModule = resources.find(resource =>
  resource.type === 'Microsoft.Resources/deployments' && resource.name === 'readLogStorage');
assert.ok(storageModule, 'compiled template must isolate optional Storage resources in a module');
assert.equal(storageModule.condition, "[not(empty(parameters('readLogStorageAccountName')))]");

const subscription = '00000000-0000-4000-8000-000000000001';
const isolated = new Config({ AZURE_SUBSCRIPTION_ID: subscription, AZURE_RESOURCE_GROUP: SCHEMA_EVOLUTION_GROUP }, 'schema-evolution');
const stamped = new Config({ AZURE_SUBSCRIPTION_ID: subscription, AZURE_STORAGE_READ_LOG_DESTINATION: 'storage' });
assert.equal(isolated.readLogAccount, '');
assert.equal(stamped.readLogAccount, 'arrowreadlogs1');

const moduleParameters = storageModule.properties?.parameters as Record<string, { value?: unknown }>;
assert.equal(moduleParameters.readLogStorageAccountName?.value, "[parameters('readLogStorageAccountName')]");

const dependencies = (storageReads.dependsOn ?? []).filter((value): value is string => typeof value === 'string');
const moduleDependency = dependencies.find(value => value.includes("Microsoft.Resources/deployments"));
assert.ok(moduleDependency, 'StorageRead diagnostics must wait for the optional Storage module');
const moduleDependencyParts = /^\[resourceId\('([^']+)', '([^']+)'\)\]$/.exec(moduleDependency);
assert.ok(moduleDependencyParts, `unexpected compiled module dependency: ${moduleDependency}`);
assert.equal(moduleDependencyParts[1], 'Microsoft.Resources/deployments');
assert.equal(moduleDependencyParts[2], 'readLogStorage');
assert.ok(dependencies.every(value => !value.includes("parameters('readLogStorageAccountName')")),
  'parent dependencies must not construct an optional Storage ID from an empty name');
const evaluateModuleDependency = (config: Config) =>
  `${config.base}/providers/${moduleDependencyParts[1]}/${moduleDependencyParts[2]}`;
assert.equal(evaluateModuleDependency(isolated), `${isolated.base}/providers/Microsoft.Resources/deployments/readLogStorage`);
assert.equal(evaluateModuleDependency(stamped), `${stamped.base}/providers/Microsoft.Resources/deployments/readLogStorage`);
assert.ok(!evaluateModuleDependency(isolated).endsWith('/providers/Microsoft.Storage/'));
assert.equal(Boolean(isolated.readLogAccount), false, 'isolated configuration must disable the module condition');
assert.equal(Boolean(stamped.readLogAccount), true, 'Storage logging must enable the module condition');

const moduleProperties = storageModule.properties as Record<string, unknown>;
const nestedTemplate = moduleProperties.template as ArmTemplate;
const nestedResources = nestedTemplate.resources ?? [];
const nestedStorage = nestedResources.find(resource => resource.type === 'Microsoft.Storage/storageAccounts');
assert.ok(nestedStorage, 'enabled module must contain the read-log Storage account');
assert.equal(nestedStorage.name, "[parameters('readLogStorageAccountName')]");

const storageAccountId = String(storageReads.properties?.storageAccountId);
const storageAccountIdParts = /^\[if\(empty\(parameters\('([^']+)'\)\), null\(\), reference\(resourceId\('Microsoft\.Resources\/deployments', '([^']+)'\), '[^']+'\)\.outputs\.storageAccountId\.value\)\]$/.exec(storageAccountId);
assert.ok(storageAccountIdParts, `unexpected compiled StorageRead account expression: ${storageAccountId}`);
assert.equal(storageAccountIdParts[1], 'readLogStorageAccountName');
assert.equal(storageAccountIdParts[2], 'readLogStorage');
const nestedOutput = nestedTemplate.outputs?.storageAccountId?.value;
const nestedOutputParts = /^\[resourceId\('([^']+)', parameters\('([^']+)'\)\)\]$/.exec(String(nestedOutput));
assert.ok(nestedOutputParts, `unexpected compiled module Storage output: ${nestedOutput}`);
assert.equal(nestedOutputParts[2], 'readLogStorageAccountName');
const evaluateStorageAccountId = (config: Config): string | null => config.readLogAccount
  ? `${config.base}/providers/${nestedOutputParts[1]}/${config.readLogAccount}` : null;
assert.equal(evaluateStorageAccountId(isolated), null, 'isolated mode must not evaluate an empty Storage name');
assert.equal(evaluateStorageAccountId(stamped),
  `${stamped.base}/providers/Microsoft.Storage/storageAccounts/${stamped.readLogAccount}`);
const aksModule = resources.find(resource => resource.type === 'Microsoft.Resources/deployments' &&
  String(resource.name).includes('arrowcol-aks-'));
assert.ok(aksModule, 'compiled sandbox must contain the AKS module');
const aksTemplate = aksModule.properties?.template as ArmTemplate;
const userAssignedIdentities = resources.filter(resource =>
  resource.type === 'Microsoft.ManagedIdentity/userAssignedIdentities');
assert.equal(userAssignedIdentities.length, 1,
  'compiled sandbox must contain exactly one user-assigned identity');
assert.equal(userAssignedIdentities[0]?.name, "[parameters('identityName')]",
  'the single UAMI must use the configured identity name');
const ficContributorRoleId = "[subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7e559ce2-48d7-4b27-9128-fa1b247f1308')]";
const runFederationRole = (aksTemplate.resources ?? []).find(resource =>
  resource.type === 'Microsoft.Authorization/roleAssignments' &&
  (String(resource.name).includes('run-federation-contributor') ||
    resource.properties?.roleDefinitionId === ficContributorRoleId ||
    Object.entries(aksTemplate.variables ?? {}).some(([name, value]) =>
      value === ficContributorRoleId && resource.properties?.roleDefinitionId === `[variables('${name}')]`)));
assert.equal(runFederationRole, undefined, 'runtime UAMI must not manage federated credentials on itself');
assert.equal(stamped.identity, 'arrow-collector',
  'stamped configuration must keep the single arrow-collector UAMI');
console.log('Compiled Azure template Storage module and single-identity, no-self-FIC-role evaluation passed.');
