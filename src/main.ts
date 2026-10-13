import './observability/otel';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ENV, type Env } from './config/env';
import { OtelConsoleLogger } from './observability/otel-logger';

async function bootstrap(): Promise<void> {
  // rawBody is required to verify HubSpot webhook signatures.
  const app = await NestFactory.create(AppModule, { rawBody: true, logger: new OtelConsoleLogger() });
  app.enableShutdownHooks();
  await app.listen(app.get<Env>(ENV).PORT);
}

void bootstrap();
