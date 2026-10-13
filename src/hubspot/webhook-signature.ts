import { createHmac, timingSafeEqual } from 'node:crypto';

export const MAX_WEBHOOK_AGE_MS = 5 * 60 * 1000;

export interface SignedRequest {
  method: string;
  uri: string;
  body: string;
  timestamp: string;
}

/** HubSpot signature v3: base64(HMAC-SHA256(clientSecret, method + uri + body + timestamp)). */
export const signHubSpotV3 = (secret: string, req: SignedRequest): string =>
  createHmac('sha256', secret).update(`${req.method}${req.uri}${req.body}${req.timestamp}`).digest('base64');

export function verifyHubSpotV3(
  secret: string,
  req: Omit<SignedRequest, 'timestamp'> & { timestamp?: string; signature?: string },
  now = Date.now(),
): boolean {
  const { timestamp, signature } = req;
  if (!timestamp || !signature) return false;

  const ts = Number(timestamp);
  if (!Number.isFinite(ts) || Math.abs(now - ts) > MAX_WEBHOOK_AGE_MS) return false;

  const expected = Buffer.from(signHubSpotV3(secret, { ...req, timestamp }));
  const actual = Buffer.from(signature);
  return expected.length === actual.length && timingSafeEqual(expected, actual);
}
