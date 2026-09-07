import React from 'react';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import fetchMock from 'fetch-mock';
import { render } from 'lib/testHelpers';
import ConfigTransferSection from 'components/Settings/ConfigTransferSection';

const previewPath = '/api/config/import/preview';
const importPath = '/api/config/import';
const content = 'kafka: {clusters: []}';
const entry = (
  index: number,
  name: string,
  conflicts: unknown[] = [],
  overlaps: number[] = []
) => ({ index, name, bootstrapServers: `${name}:9092`, conflicts, overlaps });
const conflict = {
  name: 'local',
  bootstrapServers: 'local:9092',
  reason: 'name',
};
const upload = async () =>
  userEvent.upload(
    screen.getByLabelText('选择 Kafka 配置文件'),
    new File([content], 'team.yaml', { type: 'application/yaml' })
  );

afterEach(() => fetchMock.restore());

it('defaults conflicts to local and submits only the selected entries', async () => {
  fetchMock.post(previewPath, {
    revision: 'r1',
    entries: [entry(0, 'conflict', [conflict]), entry(1, 'new')],
  });
  fetchMock.post(importPath, { added: 1, replaced: 1, skipped: 0 });
  render(<ConfigTransferSection />);
  await upload();
  expect(
    await screen.findByRole('checkbox', { name: '导入 conflict' })
  ).not.toBeChecked();
  expect(screen.getByRole('checkbox', { name: '导入 new' })).toBeChecked();
  expect(fetchMock.called(importPath)).toBe(false);
  await userEvent.click(
    screen.getByRole('checkbox', { name: '导入 conflict' })
  );
  await userEvent.click(screen.getByRole('button', { name: '确认导入（2）' }));
  await waitFor(() =>
    expect(screen.getByRole('status')).toHaveTextContent(
      '新增 1 个，替换 1 个本地环境，跳过 0 个'
    )
  );
  expect(
    JSON.parse(fetchMock.lastCall(importPath)?.[1]?.body as string)
  ).toEqual({ content, selected: [0, 1], revision: 'r1' });
});

it('imports nonconflicting files automatically', async () => {
  fetchMock.post(previewPath, { revision: 'r2', entries: [entry(0, 'new')] });
  fetchMock.post(importPath, { added: 1, replaced: 0, skipped: 0 });
  render(<ConfigTransferSection />);
  await upload();
  await waitFor(() =>
    expect(screen.getByRole('status')).toHaveTextContent('新增 1 个')
  );
});

it('lets the user cancel without changing local configuration', async () => {
  fetchMock.post(previewPath, {
    revision: 'r1',
    entries: [entry(0, 'conflict', [conflict])],
  });
  render(<ConfigTransferSection />);
  await upload();
  await userEvent.click(
    await screen.findByRole('button', { name: '取消导入' })
  );
  expect(fetchMock.called(importPath)).toBe(false);
  expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
});

it('makes incoming entries that target the same local environment mutually exclusive', async () => {
  fetchMock.post(previewPath, {
    revision: 'r1',
    entries: [
      entry(0, 'first', [conflict], [1]),
      entry(1, 'second', [conflict], [0]),
    ],
  });
  render(<ConfigTransferSection />);
  await upload();
  await userEvent.click(
    await screen.findByRole('checkbox', { name: '导入 first' })
  );
  await userEvent.click(screen.getByRole('checkbox', { name: '导入 second' }));
  expect(
    screen.getByRole('checkbox', { name: '导入 first' })
  ).not.toBeChecked();
  expect(screen.getByRole('checkbox', { name: '导入 second' })).toBeChecked();
});

it('shows validation errors without submitting any environment', async () => {
  fetchMock.post(previewPath, {
    status: 400,
    body: { message: 'kafka.clusters[1]: name 必须是非空字符串' },
  });
  render(<ConfigTransferSection />);
  await upload();
  expect(await screen.findByRole('alert')).toHaveTextContent(
    'kafka.clusters[1]'
  );
  expect(fetchMock.called(importPath)).toBe(false);
});

it('requires a fresh preview after local configuration changes', async () => {
  fetchMock.post(previewPath, {
    revision: 'r1',
    entries: [entry(0, 'conflict', [conflict])],
  });
  fetchMock.post(importPath, {
    status: 409,
    body: { message: '本地配置已变化，请重新选择文件并预览' },
  });
  render(<ConfigTransferSection />);
  await upload();
  await userEvent.click(
    await screen.findByRole('checkbox', { name: '导入 conflict' })
  );
  await userEvent.click(screen.getByRole('button', { name: '确认导入（1）' }));
  expect(await screen.findByRole('alert')).toHaveTextContent('本地配置已变化');
  await waitFor(() =>
    expect(screen.queryByRole('checkbox')).not.toBeInTheDocument()
  );
});
