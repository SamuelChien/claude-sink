import logger from '../utils/logger';
import type { ConsumptionChunk } from '../types/chunk';

const TIMEOUT_MS = 30_000;
const MAX_RETRIES = 3;
const RETRY_BASE_MS = 1000;

export class SinkHttpClient {
  private baseUrl: string;
  private batchSize: number;
  private apiKey?: string;

  constructor(serverUrl: string, batchSize = 50, apiKey?: string) {
    this.baseUrl = serverUrl.replace(/\/$/, '');
    this.batchSize = batchSize;
    this.apiKey = apiKey || process.env.SINK_API_KEY;
  }

  async sendChunks(topic: string, chunks: ConsumptionChunk[]): Promise<number> {
    let sent = 0;

    for (let i = 0; i < chunks.length; i += this.batchSize) {
      const batch = chunks.slice(i, i + this.batchSize);
      await this.sendWithRetry(`${this.baseUrl}/api/ingest`, { topic, chunks: batch });
      sent += batch.length;
      const progress = Math.min(i + this.batchSize, chunks.length);
      logger.info(`Sent batch: ${batch.length} chunks (${progress}/${chunks.length})`);
    }

    return sent;
  }

  async health(): Promise<boolean> {
    try {
      const res = await this.fetchWithTimeout(`${this.baseUrl}/health`, { method: 'GET' });
      return res.ok;
    } catch {
      return false;
    }
  }

  private async sendWithRetry(url: string, body: unknown): Promise<void> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const headers: Record<string, string> = { 'Content-Type': 'application/json' };
        if (this.apiKey) headers['X-API-Key'] = this.apiKey;

        const res = await this.fetchWithTimeout(url, {
          method: 'POST',
          headers,
          body: JSON.stringify(body),
        });

        if (res.ok) return;

        const resBody = await res.json().catch(() => ({ error: res.statusText }));
        const errMsg = (resBody as Record<string, string>).error || res.statusText;

        if (res.status >= 400 && res.status < 500) {
          throw new Error(`Server error ${res.status}: ${errMsg}`);
        }

        lastError = new Error(`Server error ${res.status}: ${errMsg}`);
      } catch (err) {
        if ((err as Error).name === 'AbortError') {
          lastError = new Error(`Request timed out after ${TIMEOUT_MS}ms`);
        } else {
          lastError = err as Error;
        }
        if ((err as Error).message?.includes('4')) throw err;
      }

      if (attempt < MAX_RETRIES) {
        const delay = RETRY_BASE_MS * Math.pow(2, attempt);
        logger.warn(`Retry ${attempt + 1}/${MAX_RETRIES} in ${delay}ms: ${lastError?.message}`);
        await new Promise(r => setTimeout(r, delay));
      }
    }

    throw lastError || new Error('Request failed after retries');
  }

  private async fetchWithTimeout(url: string, init: RequestInit): Promise<Response> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);
    try {
      return await fetch(url, { ...init, signal: controller.signal });
    } finally {
      clearTimeout(timeout);
    }
  }
}
