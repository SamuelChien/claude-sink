#!/usr/bin/env node

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

import { createCli } from './cli';

const program = createCli();
program.parseAsync(process.argv).catch((err) => {
  console.error(err.message);
  process.exit(1);
});
