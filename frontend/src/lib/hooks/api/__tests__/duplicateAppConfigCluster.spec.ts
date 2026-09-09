import { act, renderHook } from '@testing-library/react';
import fetchMock from 'fetch-mock';
import { TestQueryClientProvider } from 'lib/testHelpers';
import {
  useDeleteAppConfigCluster,
  useDuplicateAppConfigCluster,
} from 'lib/hooks/api/appConfig';
import { ApplicationConfigPropertiesKafkaClusters } from 'generated-sources';

const configPath = '/api/config';
const source: ApplicationConfigPropertiesKafkaClusters = {
  name: 'prod',
  bootstrapServers: 'prod.example.com:9092',
  readOnly: true,
  properties: { 'security.protocol': 'SASL_SSL' },
  schemaRegistry: 'https://sr.example.com',
  schemaRegistryAuth: { username: 'sr-user', password: 'sr-secret' },
};
const other: ApplicationConfigPropertiesKafkaClusters = {
  name: 'stage',
  bootstrapServers: 'stage.example.com:9092',
};

const requestBody = (path: string) => {
  const body = fetchMock.lastCall(path)?.[1]?.body;
  if (typeof body !== 'string') throw new Error(`missing body for ${path}`);
  return JSON.parse(body);
};

const renderDuplicate = () =>
  renderHook(() => useDuplicateAppConfigCluster(), {
    wrapper: TestQueryClientProvider,
  });

describe('useDuplicateAppConfigCluster', () => {
  beforeEach(() => fetchMock.restore());

  it('appends a full copy of the cluster named with the -copy suffix', async () => {
    fetchMock.getOnce(configPath, {
      properties: { kafka: { clusters: [other, source] } },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderDuplicate();

    let created = '';
    await act(async () => {
      created = await result.current.mutateAsync('prod');
    });

    expect(created).toEqual('prod-copy');
    expect(requestBody(configPath)).toEqual({
      config: {
        properties: {
          kafka: {
            clusters: [other, source, { ...source, name: 'prod-copy' }],
          },
        },
      },
    });
  });

  it('preserves unmodelled sibling properties of the config tree', async () => {
    fetchMock.getOnce(configPath, {
      properties: { kafka: { clusters: [source] }, auth: { type: 'DISABLED' } },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderDuplicate();

    await act(async () => {
      await result.current.mutateAsync('prod');
    });

    expect(requestBody(configPath).config.properties.auth).toEqual({
      type: 'DISABLED',
    });
  });

  it('increments the suffix when the -copy name is already taken', async () => {
    fetchMock.getOnce(configPath, {
      properties: {
        kafka: { clusters: [source, { ...source, name: 'prod-copy' }] },
      },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderDuplicate();

    let created = '';
    await act(async () => {
      created = await result.current.mutateAsync('prod');
    });

    expect(created).toEqual('prod-copy-2');
    expect(
      requestBody(configPath).config.properties.kafka.clusters
    ).toHaveLength(3);
  });

  it('fails without writing anything when the source cluster is gone', async () => {
    fetchMock.getOnce(configPath, {
      properties: { kafka: { clusters: [other] } },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderDuplicate();

    await act(async () => {
      await expect(result.current.mutateAsync('prod')).rejects.toThrow();
    });

    expect(fetchMock.calls(configPath, 'PUT')).toHaveLength(0);
  });
});

describe('useDeleteAppConfigCluster', () => {
  beforeEach(() => fetchMock.restore());

  it('writes the config back without the removed cluster', async () => {
    fetchMock.getOnce(configPath, {
      properties: {
        kafka: { clusters: [other, source] },
        auth: { type: 'DISABLED' },
      },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderHook(() => useDeleteAppConfigCluster(), {
      wrapper: TestQueryClientProvider,
    });

    await act(async () => {
      await result.current.mutateAsync('prod');
    });

    const body = requestBody(configPath);
    expect(body.config.properties.kafka.clusters).toEqual([other]);
    expect(body.config.properties.auth).toEqual({ type: 'DISABLED' });
  });

  it('fails without writing anything when the cluster is already gone', async () => {
    fetchMock.getOnce(configPath, {
      properties: { kafka: { clusters: [other] } },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderHook(() => useDeleteAppConfigCluster(), {
      wrapper: TestQueryClientProvider,
    });

    await act(async () => {
      await expect(result.current.mutateAsync('prod')).rejects.toThrow();
    });

    expect(fetchMock.calls(configPath, 'PUT')).toHaveLength(0);
  });
});
