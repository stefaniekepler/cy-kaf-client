import { ApplicationConfigValidation } from 'generated-sources';
import { showAlert } from 'lib/errorHandling';
import { getIsValidConfig } from 'widgets/ClusterConfigForm/utils/getIsValidConfig';

jest.mock('lib/errorHandling', () => ({
  ...jest.requireActual('lib/errorHandling'),
  showAlert: jest.fn(),
}));

describe('getIsValidConfig', () => {
  it('shows the current Kafka validation failure with a Chinese title and full diagnosis', () => {
    const message = [
      '现象：目标端口 192.0.2.92:9093 当前不接受 TCP 连接。',
      '常见原因：Kafka 未启动。',
      '建议检查：确认 Broker 进程和端口监听状态。',
    ].join('\n');
    const validation: ApplicationConfigValidation = {
      clusters: {
        sampleCluster: {
          kafka: { error: true, errorMessage: message },
        },
      },
    };

    expect(getIsValidConfig(validation, 'sampleCluster')).toBe(false);
    expect(showAlert).toHaveBeenCalledWith('error', {
      id: 'cluster-sampleCluster-kafka',
      title: 'Kafka 连接验证失败',
      message,
    });
  });
});
