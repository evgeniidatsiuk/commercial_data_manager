import { ConsoleLogger, type LogLevel } from '@nestjs/common';
import { logs, SeverityNumber } from '@opentelemetry/api-logs';

const SEVERITY: Record<LogLevel, SeverityNumber> = {
  verbose: SeverityNumber.TRACE,
  debug: SeverityNumber.DEBUG,
  log: SeverityNumber.INFO,
  warn: SeverityNumber.WARN,
  error: SeverityNumber.ERROR,
  fatal: SeverityNumber.FATAL,
};

/** Nest console logger that also ships each line as an OTLP log record, correlated with the active trace. */
export class OtelConsoleLogger extends ConsoleLogger {
  protected printMessages(
    messages: unknown[],
    context = '',
    logLevel: LogLevel = 'log',
    writeStreamType?: 'stdout' | 'stderr',
    errorStack?: unknown,
  ): void {
    super.printMessages(messages, context, logLevel, writeStreamType, errorStack);
    const logger = logs.getLogger('commercial-data-manager');
    for (const message of messages) {
      logger.emit({
        severityNumber: SEVERITY[logLevel],
        severityText: logLevel.toUpperCase(),
        body: typeof message === 'string' ? message : JSON.stringify(message),
        attributes: { 'log.context': context, ...(errorStack ? { 'exception.stacktrace': String(errorStack) } : {}) },
      });
    }
  }
}
