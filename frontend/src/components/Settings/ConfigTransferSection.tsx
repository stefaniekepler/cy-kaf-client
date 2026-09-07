import React from 'react';
import { useQueryClient } from '@tanstack/react-query';
import Alert from 'components/common/Alert/Alert';
import { Button } from 'components/common/Button/Button';
import {
  applyConfigImport,
  ConfigTransferError,
  exportKafkaConfig,
  ImportConflict,
  ImportPreview,
  previewConfigImport,
} from 'lib/hooks/api/configTransfer';

import * as S from './SettingsModal.styled';
import * as T from './ConfigTransferSection.styled';

const reasons: Record<ImportConflict['reason'], string> = {
  name: '名称相同',
  address: '连接地址及端口相同',
  name_and_address: '名称、连接地址及端口相同',
};
const readFile = (file: File): Promise<string> =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(new Error('无法读取配置文件'));
    reader.readAsText(file);
  });

type Review = ImportPreview & { content: string };

const ConfigTransferSection: React.FC = () => {
  const client = useQueryClient();
  const inputRef = React.useRef<HTMLInputElement>(null);
  const reviewRef = React.useRef<HTMLHeadingElement>(null);
  const active = React.useRef(true);
  const [busy, setBusy] = React.useState(false);
  const [review, setReview] = React.useState<Review | null>(null);
  const [selected, setSelected] = React.useState<number[]>([]);
  const [error, setError] = React.useState<string | null>(null);
  const [result, setResult] = React.useState<string | null>(null);

  React.useEffect(() => {
    const onExportFinished = (event: Event) => {
      const success = (event as CustomEvent<{ success: boolean }>).detail
        ?.success;
      if (success) setResult('配置已导出到“下载”目录');
      else setError('配置文件下载失败，请重试');
    };
    window.addEventListener('cy-kaf-config-export-finished', onExportFinished);
    return () =>
      window.removeEventListener(
        'cy-kaf-config-export-finished',
        onExportFinished
      );
  }, []);

  React.useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  React.useEffect(() => {
    if (review) reviewRef.current?.focus();
  }, [review]);

  const showError = (cause: unknown) => {
    if (!active.current) return;
    setError(cause instanceof Error ? cause.message : '配置读写失败，请重试');
    if (cause instanceof ConfigTransferError && cause.status === 409)
      setReview(null);
  };

  const apply = async (pending: Review, indices: number[]) => {
    const outcome = await applyConfigImport(
      pending.content,
      indices,
      pending.revision
    );
    // Replaced names may affect any currently open cluster page.
    await client.invalidateQueries();
    if (!active.current) return;
    setReview(null);
    setResult(
      `新增 ${outcome.added} 个，替换 ${outcome.replaced} 个本地环境，跳过 ${outcome.skipped} 个`
    );
  };

  const chooseFile = async (file?: File) => {
    if (!file || busy) return;
    setError(null);
    setResult(null);
    setReview(null);
    if (file.size > 10 * 1024 * 1024) {
      setError('配置文件不能超过 10 MiB');
      return;
    }
    setBusy(true);
    try {
      const content = await readFile(file);
      if (!active.current) return;
      const preview = await previewConfigImport(content);
      if (!active.current) return;
      const pending = { ...preview, content };
      if (
        preview.entries.some(
          (entry) => entry.conflicts.length || entry.overlaps.length
        )
      ) {
        const defaults: number[] = [];
        preview.entries.forEach((entry) => {
          if (
            !entry.conflicts.length &&
            !entry.overlaps.some((index) => defaults.includes(index))
          )
            defaults.push(entry.index);
        });
        setSelected(defaults);
        setReview(pending);
      } else {
        await apply(
          pending,
          preview.entries.map((entry) => entry.index)
        );
      }
    } catch (cause) {
      showError(cause);
    } finally {
      if (active.current) setBusy(false);
    }
  };

  const confirm = async () => {
    if (!review || busy) return;
    setBusy(true);
    setError(null);
    try {
      await apply(review, selected);
    } catch (cause) {
      showError(cause);
    } finally {
      if (active.current) setBusy(false);
    }
  };

  const download = async () => {
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      await exportKafkaConfig();
    } catch (cause) {
      showError(cause);
    } finally {
      if (active.current) setBusy(false);
    }
  };

  return (
    <S.Section>
      <S.SectionHeading>环境配置</S.SectionHeading>
      <T.Description>
        共享全部 Kafka 环境的 YAML
        配置。文件包含认证信息；本地证书文件需另行准备。
      </T.Description>
      <S.Actions>
        <Button
          buttonType="secondary"
          buttonSize="M"
          disabled={busy || !!review}
          onClick={download}
        >
          一键导出配置
        </Button>
        <Button
          buttonType="secondary"
          buttonSize="M"
          disabled={busy || !!review}
          onClick={() => inputRef.current?.click()}
        >
          一键导入配置
        </Button>
        <input
          ref={inputRef}
          type="file"
          hidden
          accept=".yaml,.yml"
          aria-label="选择 Kafka 配置文件"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (inputRef.current) inputRef.current.value = '';
            chooseFile(file);
          }}
        />
      </S.Actions>
      {busy && <T.Description role="status">正在处理配置…</T.Description>}
      {error && (
        <S.Alerts>
          <Alert type="error" title="配置导入导出失败" message={error} />
        </S.Alerts>
      )}
      {result && <T.Description role="status">{result}</T.Description>}
      {review && (
        <T.Review>
          <h3 ref={reviewRef} tabIndex={-1}>
            选择要导入的环境
          </h3>
          <T.Description>
            勾选冲突项将替换下方列出的全部本地环境；未勾选则保留本地。同一替换目标或相同连接地址的导入项只能选择一个。
          </T.Description>
          <T.EntryList>
            {review.entries.map((entry) => (
              <T.Entry key={entry.index}>
                <T.EntryLabel>
                  <input
                    type="checkbox"
                    aria-label={`导入 ${entry.name}`}
                    checked={selected.includes(entry.index)}
                    disabled={busy}
                    onChange={(event) => {
                      setSelected((previous) =>
                        event.target.checked
                          ? [
                              ...previous.filter(
                                (index) => !entry.overlaps.includes(index)
                              ),
                              entry.index,
                            ].sort((a, b) => a - b)
                          : previous.filter((index) => index !== entry.index)
                      );
                    }}
                  />
                  <strong>{entry.name}</strong>
                </T.EntryLabel>
                <T.Detail>{entry.bootstrapServers}</T.Detail>
                {entry.conflicts.length ? (
                  entry.conflicts.map((local) => (
                    <T.Detail key={local.name}>
                      替换本地：{local.name} · {local.bootstrapServers}（
                      {reasons[local.reason]}）
                    </T.Detail>
                  ))
                ) : (
                  <T.Detail>新增环境</T.Detail>
                )}
                {!!entry.overlaps.length && (
                  <T.Detail>
                    不能同时导入：
                    {entry.overlaps
                      .map((index) => review.entries[index].name)
                      .join('、')}
                  </T.Detail>
                )}
              </T.Entry>
            ))}
          </T.EntryList>
          <S.Actions>
            <Button
              buttonType="primary"
              buttonSize="M"
              disabled={busy || selected.length === 0}
              onClick={confirm}
            >
              确认导入（{selected.length}）
            </Button>
            <Button
              buttonType="secondary"
              buttonSize="M"
              disabled={busy}
              onClick={() => {
                setReview(null);
                setError(null);
              }}
            >
              取消导入
            </Button>
          </S.Actions>
        </T.Review>
      )}
    </S.Section>
  );
};

export default ConfigTransferSection;
