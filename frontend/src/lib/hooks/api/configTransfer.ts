export type ImportConflict = {
  name: string;
  bootstrapServers: string;
  reason: 'name' | 'address' | 'name_and_address';
};
export type ImportEntry = {
  index: number;
  name: string;
  bootstrapServers: string;
  conflicts: ImportConflict[];
  overlaps: number[];
};
export type ImportPreview = { revision: string; entries: ImportEntry[] };
export type ImportResult = { added: number; replaced: number; skipped: number };

export class ConfigTransferError extends Error {
  constructor(
    message: string,
    readonly status: number
  ) {
    super(message);
    this.name = 'ConfigTransferError';
  }
}

async function request(path: string, body?: unknown): Promise<Response> {
  const response = await fetch(`${window.basePath || ''}/api/config/${path}`, {
    method: body === undefined ? 'GET' : 'POST',
    credentials: 'include',
    cache: 'no-store',
    headers: { 'Content-Type': 'application/json' },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
  if (!response.ok) {
    let message = '配置读写失败，请重试';
    if (response.status === 400 || response.status === 409) {
      const error = await response.json().catch(() => null);
      if (typeof error?.message === 'string') message = error.message;
    }
    throw new ConfigTransferError(message, response.status);
  }
  return response;
}

export async function previewConfigImport(
  content: string
): Promise<ImportPreview> {
  return (await request('import/preview', { content })).json();
}
export async function applyConfigImport(
  content: string,
  selected: number[],
  revision: string
): Promise<ImportResult> {
  return (await request('import', { content, selected, revision })).json();
}
export async function exportKafkaConfig(): Promise<void> {
  const response = await request('export');
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `kafka-environments-${new Date().toISOString().slice(0, 10)}.yaml`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  // Allow WebKit to begin its download before releasing the blob.
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
