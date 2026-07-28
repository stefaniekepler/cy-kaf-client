import React from 'react';
import { screen } from '@testing-library/react';
import App from 'components/App';
import { render } from 'lib/testHelpers';
import { useGetUserInfo } from 'lib/hooks/api/roles';
import { useAppInfo } from 'lib/hooks/api/appConfig';
import toast from 'react-hot-toast';

const mockQueryScenario: {
  id: number;
  status: number;
  suppressedStatuses?: number[];
} = {
  id: 0,
  status: 404,
  suppressedStatuses: [404],
};

jest.mock('components/Dashboard/Dashboard', () => {
  const ReactActual = jest.requireActual('react');
  const { useQuery } = jest.requireActual('@tanstack/react-query');
  const QueryErrorDashboard = () => {
    const query = useQuery({
      queryKey: ['global-query-error-test', mockQueryScenario.id],
      queryFn: async () => {
        const error = Object.assign(new Error('Expected test error'), {
          status: mockQueryScenario.status,
          statusText: 'Expected test error',
          json: () => Promise.resolve({}),
        });
        throw error;
      },
      retry: false,
      meta: mockQueryScenario.suppressedStatuses
        ? {
            suppressGlobalErrorStatuses: mockQueryScenario.suppressedStatuses,
          }
        : undefined,
    });

    return ReactActual.createElement(
      'div',
      null,
      query.isError ? 'Query failed' : 'Query pending'
    );
  };

  return {
    __esModule: true,
    default: QueryErrorDashboard,
  };
});

jest.mock('components/Nav/Nav', () => () => <div>Navigation</div>);
jest.mock('components/Version/Version', () => () => <div>Version</div>);
jest.mock('components/NavBar/NavBar', () => () => <div>NavBar</div>);

jest.mock('lib/hooks/api/roles', () => ({
  useGetUserInfo: jest.fn(),
}));
jest.mock('lib/hooks/api/appConfig', () => ({
  useAppInfo: jest.fn(),
}));
describe('App global query error handling', () => {
  const renderApp = () => {
    return render(<App />, {
      initialEntries: ['/'],
    });
  };

  beforeEach(() => {
    toast.remove();
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: jest.fn().mockImplementation(() => ({
        matches: false,
        addListener: jest.fn(),
      })),
    });
    jest.mocked(useGetUserInfo).mockReturnValue({ data: {} } as never);
    jest.mocked(useAppInfo).mockReturnValue({ data: {} } as never);
    mockQueryScenario.id += 1;
  });

  it('does not globally report a status suppressed by query metadata', async () => {
    mockQueryScenario.status = 404;
    mockQueryScenario.suppressedStatuses = [404];

    renderApp();

    expect(await screen.findByText('Query failed')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('still globally reports a status not suppressed by query metadata', async () => {
    mockQueryScenario.status = 500;
    mockQueryScenario.suppressedStatuses = [404];

    renderApp();

    expect(await screen.findByText('Query failed')).toBeInTheDocument();
    expect(
      await screen.findByRole('heading', {
        name: '500 Expected test error',
      })
    ).toBeInTheDocument();
  });

  it('still globally reports a 404 when the query has no suppression metadata', async () => {
    mockQueryScenario.status = 404;
    mockQueryScenario.suppressedStatuses = undefined;

    renderApp();

    expect(await screen.findByText('Query failed')).toBeInTheDocument();
    expect(
      await screen.findByRole('heading', {
        name: '404 Expected test error',
      })
    ).toBeInTheDocument();
  });
});
