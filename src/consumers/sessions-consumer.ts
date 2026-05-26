import { SinkConsumer, ProcessingResult } from '../kafka/consumer';
import { analyzeSession } from '../processing/sessions-analyzer';
import type { ConsumptionChunk } from '../types/chunk';
import type { AnalyzedSession } from '../types/analyzed';

export class SessionsConsumer extends SinkConsumer<AnalyzedSession> {
  get name() { return 'sessions-consumer'; }

  process(batch: ConsumptionChunk[]): ProcessingResult<AnalyzedSession>[] {
    return batch.map(chunk => ({
      key: chunk.sourceId,
      value: analyzeSession(chunk),
    }));
  }
}
