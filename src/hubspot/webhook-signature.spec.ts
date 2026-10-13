import { MAX_WEBHOOK_AGE_MS, signHubSpotV3, verifyHubSpotV3 } from './webhook-signature';

describe('HubSpot webhook signature v3', () => {
  const secret = 'client-secret';
  const now = 1_760_000_000_000;
  const req = {
    method: 'POST',
    uri: 'https://cdm.example.com/webhooks/hubspot',
    body: '[{"eventId":1}]',
    timestamp: String(now),
  };
  const signature = signHubSpotV3(secret, req);

  it('accepts a valid, fresh signature', () => {
    expect(verifyHubSpotV3(secret, { ...req, signature }, now)).toBe(true);
  });

  it('rejects a tampered body', () => {
    expect(verifyHubSpotV3(secret, { ...req, body: '[{"eventId":2}]', signature }, now)).toBe(false);
  });

  it('rejects a different secret', () => {
    expect(verifyHubSpotV3('other-secret', { ...req, signature }, now)).toBe(false);
  });

  it('rejects stale timestamps (replay protection)', () => {
    expect(verifyHubSpotV3(secret, { ...req, signature }, now + MAX_WEBHOOK_AGE_MS + 1)).toBe(false);
  });

  it('rejects missing headers', () => {
    expect(verifyHubSpotV3(secret, { ...req, signature: undefined }, now)).toBe(false);
    expect(verifyHubSpotV3(secret, { ...req, timestamp: undefined, signature }, now)).toBe(false);
  });
});
