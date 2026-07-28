import React from 'react';
import { render, WithRoute } from 'lib/testHelpers';
import Messages from 'components/Topics/Topic/Messages/Messages';
import { useTopicMessages } from 'lib/hooks/api/topicMessages';
import { clusterTopicMessagesPath } from 'lib/paths';
import { screen, waitFor } from '@testing-library/react';

const mockFilterComponents = 'mockFilterComponents';
const mockMessagesTable = 'mockMessagesTable';
const clusterName = 'cluster-name';
const topicName = 'topic-name';

jest.mock('lib/hooks/api/topicMessages', () => ({
  useTopicMessages: jest.fn(),
}));

jest.mock('components/Topics/Topic/Messages/MessagesTable', () => () => (
  <div>{mockMessagesTable}</div>
));

jest.mock('components/Topics/Topic/Messages/Filters/Filters', () => () => (
  <div>{mockFilterComponents}</div>
));

describe('Messages', () => {
  const renderComponent = () => {
    return render(
      <WithRoute path={clusterTopicMessagesPath()}>
        <Messages />
      </WithRoute>,
      {
        initialEntries: [clusterTopicMessagesPath(clusterName, topicName)],
      }
    );
  };

  beforeEach(() => {
    jest.mocked(useTopicMessages).mockClear();
    (useTopicMessages as jest.Mock).mockImplementation(() => ({
      messages: [],
      isFetching: false,
    }));
  });

  describe('component rendering default behavior with the search params', () => {
    beforeEach(() => {
      renderComponent();
    });

    it('should check if the filters are shown in the messages', () => {
      expect(screen.getByText(mockFilterComponents)).toBeInTheDocument();
    });

    it('should check if the table of messages are shown in the messages', () => {
      expect(screen.getByText(mockMessagesTable)).toBeInTheDocument();
    });

    it('enables message reads only after the parent filter is initialized', async () => {
      expect(useTopicMessages).toHaveBeenCalledWith({
        clusterName,
        topicName,
        enabled: false,
      });

      await waitFor(() =>
        expect(useTopicMessages).toHaveBeenLastCalledWith({
          clusterName,
          topicName,
          enabled: true,
        })
      );
    });
  });
});
