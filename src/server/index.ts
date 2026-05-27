import express, { Request, Response } from 'express';
import { SinkProducer } from '../kafka/producer';
import { buildKafkaConfig } from '../kafka/config';
import { provisionTopics } from '../kafka/admin';
import logger from '../utils/logger';
import type { ConsumptionChunk } from '../types/chunk';

export interface ServerOptions {
  port: number;
  brokers: string;
}

export async function startServer(options: ServerOptions): Promise<void> {
  const kafkaConfig = buildKafkaConfig(options.brokers);

  logger.info('Provisioning topics...');
  await provisionTopics(kafkaConfig);

  const producers = new Map<string, SinkProducer>();
  const topics = ['sink.skills', 'sink.sessions', 'sink.code'];

  for (const topic of topics) {
    const producer = new SinkProducer(kafkaConfig, { batchSize: 100, topic, dryRun: false });
    await producer.connect();
    producers.set(topic, producer);
  }

  const app = express();
  app.use(express.json({ limit: '50mb' }));

  app.get('/health', (_req: Request, res: Response) => {
    res.json({ status: 'ok', topics: [...producers.keys()] });
  });

  app.post('/api/ingest', async (req: Request, res: Response) => {
    const { topic, chunks } = req.body as { topic: string; chunks: ConsumptionChunk[] };

    if (!topic || !chunks || !Array.isArray(chunks)) {
      res.status(400).json({ error: 'Missing topic or chunks array' });
      return;
    }

    const producer = producers.get(topic);
    if (!producer) {
      res.status(400).json({ error: `Unknown topic: ${topic}. Valid: ${[...producers.keys()].join(', ')}` });
      return;
    }

    try {
      const sent = await producer.sendChunks(chunks);
      res.json({ ok: true, sent, topic });
    } catch (err) {
      logger.error(`Ingest error: ${(err as Error).message}`);
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.post('/api/ingest/skills', async (req: Request, res: Response) => {
    const chunks = req.body.chunks as ConsumptionChunk[];
    if (!chunks || !Array.isArray(chunks)) { res.status(400).json({ error: 'Missing chunks array' }); return; }
    try {
      const sent = await producers.get('sink.skills')!.sendChunks(chunks);
      res.json({ ok: true, sent, topic: 'sink.skills' });
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.post('/api/ingest/sessions', async (req: Request, res: Response) => {
    const chunks = req.body.chunks as ConsumptionChunk[];
    if (!chunks || !Array.isArray(chunks)) { res.status(400).json({ error: 'Missing chunks array' }); return; }
    try {
      const sent = await producers.get('sink.sessions')!.sendChunks(chunks);
      res.json({ ok: true, sent, topic: 'sink.sessions' });
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  app.post('/api/ingest/code', async (req: Request, res: Response) => {
    const chunks = req.body.chunks as ConsumptionChunk[];
    if (!chunks || !Array.isArray(chunks)) { res.status(400).json({ error: 'Missing chunks array' }); return; }
    try {
      const sent = await producers.get('sink.code')!.sendChunks(chunks);
      res.json({ ok: true, sent, topic: 'sink.code' });
    } catch (err) {
      res.status(500).json({ error: (err as Error).message });
    }
  });

  const shutdown = async () => {
    logger.info('Shutting down server...');
    for (const p of producers.values()) await p.disconnect();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  app.listen(options.port, () => {
    logger.info(`claude-sink server listening on :${options.port}`);
    logger.info(`  POST /api/ingest          { topic, chunks }`);
    logger.info(`  POST /api/ingest/skills   { chunks }`);
    logger.info(`  POST /api/ingest/sessions { chunks }`);
    logger.info(`  POST /api/ingest/code     { chunks }`);
    logger.info(`  GET  /health`);
  });
}
