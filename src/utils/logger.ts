import { createLogger, format, transports } from 'winston';

let currentLevel = process.env.LOG_LEVEL || 'info';

export function setLogLevel(level: string) {
  currentLevel = level;
  logger.level = level;
}

const logger = createLogger({
  level: currentLevel,
  format: format.combine(
    format.timestamp({ format: 'HH:mm:ss' }),
    format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}] ${message}`;
    })
  ),
  transports: [new transports.Console()],
});

export default logger;
