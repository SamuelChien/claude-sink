export interface KafkaConfig {
  brokers: string[];
  clientId: string;
  connectionTimeout: number;
  retry: {
    initialRetryTime: number;
    retries: number;
  };
}

export interface ProducerConfig {
  batchSize: number;
  topic: string;
  dryRun: boolean;
}

export function buildKafkaConfig(brokers: string): KafkaConfig {
  return {
    brokers: brokers.split(',').map(b => b.trim()),
    clientId: 'claude-sink',
    connectionTimeout: 10_000,
    retry: {
      initialRetryTime: 100,
      retries: 8,
    },
  };
}
