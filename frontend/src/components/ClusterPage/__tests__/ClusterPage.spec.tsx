import React from 'react';
import { Cluster, ClusterFeaturesEnum } from 'generated-sources';
import ClusterPageComponent from 'components/ClusterPage/ClusterPage';
import { screen, waitFor } from '@testing-library/react';
import { render, WithRoute } from 'lib/testHelpers';
import {
  clusterBrokersPath,
  clusterConnectorsPath,
  clusterConsumerGroupsPath,
  clusterKsqlDbPath,
  clusterPath,
  clusterSchemasPath,
  clusterTopicsPath,
  kafkaConnectPath,
} from 'lib/paths';
import { useClusters } from 'lib/hooks/api/clusters';
import { onlineClusterPayload } from 'lib/fixtures/clusters';
import { useLocation, useNavigationType } from 'react-router-dom';

const CLusterCompText = {
  Topics: 'Topics',
  Schemas: 'Schemas',
  Connect: 'Kafka Connect',
  Brokers: 'Brokers',
  ConsumerGroups: 'ConsumerGroups',
  KsqlDb: 'KsqlDb',
};

jest.mock('components/Topics/Topics', () => () => (
  <div>{CLusterCompText.Topics}</div>
));
jest.mock('components/Schemas/Schemas', () => () => (
  <div>{CLusterCompText.Schemas}</div>
));
jest.mock('components/Connect/Connect', () => () => (
  <div>{CLusterCompText.Connect}</div>
));
jest.mock('components/Brokers/Brokers', () => () => (
  <div>{CLusterCompText.Brokers}</div>
));
jest.mock('components/ConsumerGroups/ConsumerGroups', () => () => (
  <div>{CLusterCompText.ConsumerGroups}</div>
));
jest.mock('components/KsqlDb/KsqlDb', () => () => (
  <div>{CLusterCompText.KsqlDb}</div>
));

jest.mock('lib/hooks/api/clusters', () => ({
  useClusters: jest.fn(),
}));

const RouteState = () => {
  const location = useLocation();
  const navigationType = useNavigationType();

  return (
    <output data-testid="route-state">
      {navigationType}:{location.pathname}
    </output>
  );
};

describe('ClusterPage', () => {
  const renderComponent = async (pathname: string, payload: Cluster[] = []) => {
    (useClusters as jest.Mock).mockImplementation(() => ({
      data: payload,
      isFetched: true,
    }));
    await render(
      <WithRoute path={`${clusterPath()}/*`}>
        <>
          <ClusterPageComponent />
          <RouteState />
        </>
      </WithRoute>,
      { initialEntries: [pathname] }
    );
    await waitFor(() => {
      expect(screen.queryByRole('progressbar')).not.toBeInTheDocument();
    });
  };

  it('renders Brokers', async () => {
    await renderComponent(clusterBrokersPath('second'));
    expect(screen.getByText(CLusterCompText.Brokers)).toBeInTheDocument();
  });
  it('renders Topics', async () => {
    await renderComponent(clusterTopicsPath('second'));
    expect(screen.getByText(CLusterCompText.Topics)).toBeInTheDocument();
  });
  it('renders ConsumerGroups', async () => {
    await renderComponent(clusterConsumerGroupsPath('second'));
    expect(
      screen.getByText(CLusterCompText.ConsumerGroups)
    ).toBeInTheDocument();
  });

  describe('configured features', () => {
    const itCorrectlyHandlesConfiguredFeature = (
      feature: ClusterFeaturesEnum,
      text: string,
      path: string
    ) => {
      it(`renders ${text} if ${feature} is configured`, async () => {
        await renderComponent(path, [
          {
            ...onlineClusterPayload,
            features: [feature],
          },
        ]);
        expect(screen.getByText(text)).toBeInTheDocument();
      });

      it(`redirects to Brokers if ${feature} is not configured`, async () => {
        await renderComponent(path, [
          { ...onlineClusterPayload, features: [] },
        ]);
        expect(screen.queryByText(text)).not.toBeInTheDocument();
        expect(
          await screen.findByText(CLusterCompText.Brokers)
        ).toBeInTheDocument();
        expect(screen.getByTestId('route-state')).toHaveTextContent(
          `REPLACE:${clusterBrokersPath(onlineClusterPayload.name)}`
        );
      });
    };

    itCorrectlyHandlesConfiguredFeature(
      ClusterFeaturesEnum.SCHEMA_REGISTRY,
      CLusterCompText.Schemas,
      clusterSchemasPath(onlineClusterPayload.name)
    );
    itCorrectlyHandlesConfiguredFeature(
      ClusterFeaturesEnum.KAFKA_CONNECT,
      CLusterCompText.Connect,
      kafkaConnectPath(onlineClusterPayload.name)
    );
    itCorrectlyHandlesConfiguredFeature(
      ClusterFeaturesEnum.KAFKA_CONNECT,
      CLusterCompText.Connect,
      clusterConnectorsPath(onlineClusterPayload.name)
    );
    itCorrectlyHandlesConfiguredFeature(
      ClusterFeaturesEnum.KSQL_DB,
      CLusterCompText.KsqlDb,
      clusterKsqlDbPath(onlineClusterPayload.name)
    );
  });

  it('redirects an unknown cluster route to Brokers', async () => {
    const clusterName = 'encoded cluster';
    await renderComponent(`${clusterPath(clusterName)}/unknown-feature`, [
      { ...onlineClusterPayload, name: clusterName, features: [] },
    ]);
    expect(
      await screen.findByText(CLusterCompText.Brokers)
    ).toBeInTheDocument();
    expect(screen.getByTestId('route-state')).toHaveTextContent(
      `REPLACE:${clusterBrokersPath(clusterName)}`
    );
  });
});
