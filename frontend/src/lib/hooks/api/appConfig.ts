import {
  appConfigApiClient as appConfig,
  internalApiClient as internalApi,
} from 'lib/api';
import {
  useMutation,
  useQueryClient,
  useSuspenseQuery,
} from '@tanstack/react-query';
import {
  ApplicationConfig,
  ApplicationConfigPropertiesKafkaClusters,
  ApplicationInfo,
} from 'generated-sources';
import { QUERY_REFETCH_OFF_OPTIONS } from 'lib/constants';
import { nextDuplicateName } from 'lib/duplicateClusterName';

export function useAuthSettings() {
  return useSuspenseQuery({
    queryKey: ['app', 'authSettings'],
    queryFn: () => appConfig.getAuthenticationSettings(),
    ...QUERY_REFETCH_OFF_OPTIONS,
  });
}

export function useAuthenticate() {
  return useMutation({
    mutationFn: (params: { username: string; password: string }) =>
      internalApi.authenticateRaw(params, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      }),
  });
}

export function useAppInfo() {
  return useSuspenseQuery({
    queryKey: ['app', 'info'],
    queryFn: async () => {
      const data = await appConfig.getApplicationInfoRaw();

      let response: ApplicationInfo = {};
      try {
        response = await data.value();
      } catch {
        response = {};
      }

      const url = new URL(data.raw.url);
      return {
        redirect: url.pathname.includes('auth'),
        response,
      };
    },
  });
}

export function useAppConfig() {
  return useSuspenseQuery({
    queryKey: ['app', 'config'],
    queryFn: () => appConfig.getCurrentConfig(),
  });
}

function aggregateClusters(
  cluster: ApplicationConfigPropertiesKafkaClusters,
  existingConfig: ApplicationConfig,
  initialName?: string,
  deleteCluster?: boolean
): ApplicationConfigPropertiesKafkaClusters[] {
  const existingClusters = existingConfig.properties?.kafka?.clusters || [];

  if (!initialName) {
    return [...existingClusters, cluster];
  }

  if (!deleteCluster) {
    return existingClusters.map((c) => (c.name === initialName ? cluster : c));
  }

  return existingClusters.filter((c) => c.name !== initialName);
}

export function useUpdateAppConfig({
  initialName,
  deleteCluster,
}: {
  initialName?: string;
  deleteCluster?: boolean;
}) {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (cluster: ApplicationConfigPropertiesKafkaClusters) => {
      const existingConfig = await appConfig.getCurrentConfig();

      const clusters = aggregateClusters(
        cluster,
        existingConfig,
        initialName,
        deleteCluster
      );

      const config = {
        ...existingConfig,
        properties: {
          ...existingConfig.properties,
          kafka: { clusters },
        },
      };
      return appConfig.restartWithConfig({ restartRequest: { config } });
    },
    onSuccess: () => client.invalidateQueries({ queryKey: ['app', 'config'] }),
  });
}

/**
 * Clones one configured cluster: the running config is re-read so the copy
 * carries every stored field (credentials included), and its name is derived
 * from the freshest cluster list so concurrent edits cannot produce duplicates.
 * Resolves with the name the copy was actually stored under.
 */
export function useDuplicateAppConfigCluster() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (sourceName: string) => {
      const existingConfig = await appConfig.getCurrentConfig();
      const existingClusters = existingConfig.properties?.kafka?.clusters || [];
      const source = existingClusters.find(({ name }) => name === sourceName);
      if (!source) {
        throw new Error(`Cluster ${sourceName} is no longer configured`);
      }

      const name = nextDuplicateName(
        sourceName,
        existingClusters
          .map((c) => c.name)
          .filter((n): n is string => n !== undefined)
      );
      const clusters = aggregateClusters({ ...source, name }, existingConfig);
      const config = {
        ...existingConfig,
        properties: {
          ...existingConfig.properties,
          kafka: { clusters },
        },
      };
      await appConfig.restartWithConfig({ restartRequest: { config } });
      return name;
    },
    onSuccess: () => {
      client.invalidateQueries({ queryKey: ['app', 'config'] });
      client.invalidateQueries({ queryKey: ['clusters'] });
    },
  });
}

/**
 * Removes one configured cluster. The running config is re-read first so the
 * write keeps every unmodelled sibling property, and a cluster that is already
 * gone fails loudly instead of silently rewriting the file.
 */
export function useDeleteAppConfigCluster() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (clusterName: string) => {
      const existingConfig = await appConfig.getCurrentConfig();
      const existingClusters = existingConfig.properties?.kafka?.clusters || [];
      if (!existingClusters.some(({ name }) => name === clusterName)) {
        throw new Error(`Cluster ${clusterName} is no longer configured`);
      }

      const clusters = aggregateClusters(
        { name: clusterName },
        existingConfig,
        clusterName,
        true
      );
      const config = {
        ...existingConfig,
        properties: {
          ...existingConfig.properties,
          kafka: { clusters },
        },
      };
      await appConfig.restartWithConfig({ restartRequest: { config } });
    },
    onSuccess: () => {
      client.invalidateQueries({ queryKey: ['app', 'config'] });
      client.invalidateQueries({ queryKey: ['clusters'] });
    },
  });
}

export function useAppConfigFilesUpload() {
  return useMutation({
    mutationFn: (payload: FormData) =>
      fetch('/api/config/relatedfiles', {
        method: 'POST',
        body: payload,
      }).then((res) => res.json()),
  });
}

export function useValidateAppConfig() {
  return useMutation({
    mutationFn: (config: ApplicationConfigPropertiesKafkaClusters) =>
      appConfig.validateConfig({
        applicationConfig: { properties: { kafka: { clusters: [config] } } },
      }),
  });
}
