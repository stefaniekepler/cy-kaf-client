import { act, renderHook } from '@testing-library/react';
import fetchMock from 'fetch-mock';
import { TestQueryClientProvider } from 'lib/testHelpers';
import {
  useUpdateAppConfig,
  useValidateAppConfig,
} from 'lib/hooks/api/appConfig';
import { ApplicationConfigPropertiesKafkaClusters } from 'generated-sources';

const configPath = '/api/config';
const validatePath = '/api/config/validated';
const sampleCluster: ApplicationConfigPropertiesKafkaClusters = {
  name: 'sampleCluster',
  bootstrapServers: '192.0.2.92:9093',
};

const requestBody = (path: string) => {
  const body = fetchMock.lastCall(path)?.[1]?.body;
  if (typeof body !== 'string') throw new Error(`missing body for ${path}`);
  return JSON.parse(body);
};

describe('Application config mutations', () => {
  beforeEach(() => fetchMock.restore());

  it('validates only the cluster currently being edited', async () => {
    fetchMock.putOnce(validatePath, { clusters: {} });
    const { result } = renderHook(() => useValidateAppConfig(), {
      wrapper: TestQueryClientProvider,
    });

    await act(async () => {
      await result.current.mutateAsync(sampleCluster);
    });

    expect(requestBody(validatePath)).toEqual({
      properties: { kafka: { clusters: [sampleCluster] } },
    });
  });

  it('submits the merged config without calling the validation endpoint', async () => {
    const existingCluster: ApplicationConfigPropertiesKafkaClusters = {
      name: 'existingCluster',
      bootstrapServers: 'existingCluster.example.com:9092',
    };
    fetchMock.getOnce(configPath, {
      properties: { kafka: { clusters: [existingCluster] } },
    });
    fetchMock.putOnce(configPath, 204);
    const { result } = renderHook(
      () => useUpdateAppConfig({ initialName: undefined }),
      { wrapper: TestQueryClientProvider }
    );

    await act(async () => {
      await result.current.mutateAsync(sampleCluster);
    });

    expect(fetchMock.calls(validatePath)).toHaveLength(0);
    expect(requestBody(configPath)).toEqual({
      config: {
        properties: { kafka: { clusters: [existingCluster, sampleCluster] } },
      },
    });
  });
});
