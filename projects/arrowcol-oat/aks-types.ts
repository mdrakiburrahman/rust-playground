export type AksNetworkMode = 'auto' | 'azure' | 'cilium';

export interface LiveStamp {
  stamp: string;
  subscription: string;
  resourceGroup: string;
  clusterName: string;
}

export interface DestroyOptions {
  subscription?: string;
  resourceGroup?: string;
  confirm?: string;
  receiptPath?: string;
  timeoutMinutes?: number;
  stampConfig?: string;
}

export interface DestroyResult {
  resourceGroup: string;
  existed: boolean;
  deleted: boolean;
  dryRun: boolean;
  managedResourceGroups: string[];
  receiptPath?: string;
}

export interface DeployOptions {
  subscription?: string;
  stampConfig?: string;
  stamp?: string;
  network?: AksNetworkMode;
  whatIf?: boolean;
}

export interface DeployAttempt {
  network: AksNetworkMode;
  outcome: 'succeeded' | 'cluster-create-failed' | 'other-failure';
  error?: string;
}

export interface DeployResult {
  stamp: string;
  network: AksNetworkMode;
  attempts: DeployAttempt[];
  receiptPath: string;
  setup?: SetupResult;
}

export interface SetupOptions {
  stampConfig?: string;
  stamp?: string;
  skipImages?: boolean;
  skipPreflight?: boolean;
  tag?: string;
}

export interface SetupResult {
  stamp: string;
  clusterName: string;
  images: {
    sqlserver: string;
    catalog: string;
    router: string;
    dataflow: string;
    client: string;
  };
  aksConfigPath: string;
  runtimeConfigPath: string;
  bootstrapEnvPath: string;
  preflightReady: boolean;
}
