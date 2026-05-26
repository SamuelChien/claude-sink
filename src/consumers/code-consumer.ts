import { SinkConsumer, ProcessingResult } from '../kafka/consumer';
import { analyzeCode } from '../processing/code-analyzer';
import type { ConsumptionChunk } from '../types/chunk';
import type { AnalyzedCode } from '../types/analyzed';

export class CodeConsumer extends SinkConsumer<AnalyzedCode> {
  get name() { return 'code-consumer'; }

  process(batch: ConsumptionChunk[]): ProcessingResult<AnalyzedCode>[] {
    return batch.map(chunk => ({
      key: chunk.sourceId,
      value: analyzeCode(chunk),
    }));
  }
}
