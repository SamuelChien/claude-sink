import logger from '../utils/logger';
import type { ConsumptionChunk } from '../types/chunk';

export class SinkHttpClient {
  private baseUrl: string;
  private batchSize: number;

  constructor(serverUrl: string, batchSize = 50) {
    this.baseUrl = serverUrl.replace(/\/$/, '');
    this.batchSize = batchSize;
  }

  async sendChunks(topic: string, chunks: ConsumptionChunk[]): Promise<number> {
    let sent = 0;

    for (let i = 0; i < chunks.length; i += this.batchSize) {
      const batch = chunks.slice(i, i + this.batchSize);
      const res = await fetch(`${this.baseUrl}/api/ingest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic, chunks: batch }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(`Server error ${res.status}: ${(body as Record<string, string>).error}`);
      }

      sent += batch.length;
      const progress = Math.min(i + this.batchSize, chunks.length);
      logger.info(`Sent batch: ${batch.length} chunks (${progress}/${chunks.length})`);
    }

    return sent;
  }

  async health(): Promise<boolean> {
    try {
      const res = await fetch(`${this.baseUrl}/health`);
      return res.ok;
    } catch {
      return false;
    }
  }
}
