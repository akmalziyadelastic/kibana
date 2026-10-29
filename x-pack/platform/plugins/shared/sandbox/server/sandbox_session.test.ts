/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import { loggerMock } from '@kbn/logging-mocks';
import type { Logger } from '@kbn/core/server';
import { SandboxApiClient } from './grpc_client';
import { SandboxSessionImpl } from './sandbox_session';

const mockClose = jest.fn();
const mockMetadataSet = jest.fn();

type MockRpc = (
  request: unknown,
  metadata: unknown,
  options: unknown,
  callback: (err: Error | null, response: unknown) => void
) => void;

interface MockServiceClient {
  close: () => void;
  runCommand: MockRpc;
  statFiles: MockRpc;
  readFiles: MockRpc;
  writeFiles: MockRpc;
  mkdirs: MockRpc;
}

jest.mock('@grpc/grpc-js', () => {
  function MockSandboxServiceClient(this: MockServiceClient): void {
    this.close = mockClose;
    const succeed: MockRpc = (_request, _metadata, _options, callback) => callback(null, []);
    this.runCommand = succeed;
    this.statFiles = succeed;
    this.readFiles = succeed;
    this.writeFiles = succeed;
    this.mkdirs = succeed;
  }

  return {
    makeClientConstructor: () => MockSandboxServiceClient,
    credentials: {
      createInsecure: () => ({}),
      createSsl: () => ({}),
    },
    Metadata: class MockMetadata {
      set(key: string, value: string) {
        mockMetadataSet(key, value);
      }
    },
  };
});

const sandboxConfig = {
  host: 'sandbox-api',
  port: 50051,
  apiKey: 'secret-key',
  clientCertPem: Buffer.from('mock-cert'),
  clientKeyPem: Buffer.from('mock-key'),
};

const unavailable = (): Error => Object.assign(new Error('UNAVAILABLE'), { code: 14 });

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: Error) => void;
}
const defer = <T>(): Deferred<T> => {
  let resolve!: (value: T) => void;
  let reject!: (err: Error) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const okResult = { stdout: '', stderr: '', exit_code: 0, timed_out: false };

describe('SandboxApiClient metadata', () => {
  const metadataSent = () => mockMetadataSet.mock.calls.map(([key]) => key);

  beforeEach(() => jest.clearAllMocks());

  it('sends the conversation and correlation ids as headers', async () => {
    const client = new SandboxApiClient(sandboxConfig);

    await client.statFiles('space__conv-1', ['/a'], { correlationId: 'tool-call-1' });

    expect(mockMetadataSet).toHaveBeenCalledWith('x-conversation-id', 'space__conv-1');
    expect(mockMetadataSet).toHaveBeenCalledWith('x-correlation-id', 'tool-call-1');
  });

  it('omits the correlation header when no correlation id is given', async () => {
    const client = new SandboxApiClient(sandboxConfig);

    await client.statFiles('space__conv-1', ['/a']);

    expect(metadataSent()).toEqual(['authorization', 'x-conversation-id']);
  });

  it.each([
    ['non-ASCII characters', 'caf\u00e9'],
    ['a newline', 'a\nb'],
    ['more than 128 characters', 'x'.repeat(129)],
  ])('drops a correlation id with %s instead of failing the RPC', async (_label, correlationId) => {
    const client = new SandboxApiClient(sandboxConfig);

    await expect(client.statFiles('space__conv-1', ['/a'], { correlationId })).resolves.toEqual([]);

    expect(metadataSent()).not.toContain('x-correlation-id');
  });
});

describe('SandboxSessionImpl', () => {
  let logger: jest.Mocked<Logger>;
  let apiClient: SandboxApiClient;
  let session: SandboxSessionImpl;
  let runCommand: jest.SpyInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    logger = loggerMock.create();
    apiClient = new SandboxApiClient(sandboxConfig);
    session = new SandboxSessionImpl('space:conv-1', apiClient, logger);
    runCommand = jest.spyOn(apiClient, 'runCommand');
  });

  const run = () => session.runCommand({ command: 'true' });

  it('forwards the correlation id to the API client for every RPC type', async () => {
    const options = { correlationId: 'correlation-1' };
    const readFiles = jest.spyOn(apiClient, 'readFiles').mockResolvedValue([]);
    const writeFiles = jest.spyOn(apiClient, 'writeFiles').mockResolvedValue([]);
    const mkdirs = jest.spyOn(apiClient, 'mkdirs').mockResolvedValue([]);
    const statFiles = jest.spyOn(apiClient, 'statFiles').mockResolvedValue([]);
    runCommand.mockResolvedValue(okResult);

    await session.runCommand({ command: 'true' }, options);
    await session.readFiles([{ path: '/a' }], options);
    await session.writeFiles([{ path: '/a', content: Buffer.from('') }], options);
    await session.mkdirs(['/a'], options);
    await session.statFiles(['/a'], options);

    const conversationId = 'space:conv-1';
    expect(runCommand).toHaveBeenCalledWith(conversationId, { command: 'true' }, options);
    expect(readFiles).toHaveBeenCalledWith(conversationId, [{ path: '/a' }], options);
    expect(writeFiles).toHaveBeenCalledWith(
      conversationId,
      [{ path: '/a', content: Buffer.from('') }],
      options
    );
    expect(mkdirs).toHaveBeenCalledWith(conversationId, ['/a'], options);
    expect(statFiles).toHaveBeenCalledWith(conversationId, ['/a'], options);
  });

  describe('per-RPC debug log', () => {
    it('logs the sandbox.* correlation fields and rpc metadata for a successful call', async () => {
      runCommand.mockResolvedValue(okResult);

      await session.runCommand({ command: 'true' }, { correlationId: 'correlation-1' });

      expect(logger.debug).toHaveBeenCalledWith(
        'Sandbox RPC RunCommand finished with gRPC status 0',
        {
          sandbox: { conversation_id: 'space:conv-1', correlation_id: 'correlation-1' },
          rpc: {
            system: 'grpc',
            service: 'sandbox.SandboxService',
            method: 'RunCommand',
            grpc: { status_code: 0 },
          },
          event: { duration: expect.any(Number) },
        }
      );
    });

    it('logs the gRPC status code of a failed call and omits a missing correlation id', async () => {
      runCommand.mockRejectedValue(unavailable());

      await expect(run()).rejects.toThrow('UNAVAILABLE');

      expect(logger.debug).toHaveBeenCalledWith(
        'Sandbox RPC RunCommand finished with gRPC status 14',
        expect.objectContaining({
          sandbox: { conversation_id: 'space:conv-1' },
          rpc: expect.objectContaining({ grpc: { status_code: 14 } }),
        })
      );
    });
  });

  it('starts with isReset=true', () => {
    expect(session.isReset).toBe(true);
  });

  it('sets isReset=false after a successful call', async () => {
    runCommand.mockResolvedValueOnce(okResult);
    await run();
    expect(session.isReset).toBe(false);
  });

  it('sets isReset=true after an UNAVAILABLE failure', async () => {
    runCommand.mockResolvedValueOnce(okResult);
    await run();
    expect(session.isReset).toBe(false);

    runCommand.mockRejectedValueOnce(unavailable());
    await expect(run()).rejects.toMatchObject({ code: 14 });
    expect(session.isReset).toBe(true);
    expect(logger.warn).toHaveBeenCalledTimes(1);
  });

  it('does not set isReset=true for non-UNAVAILABLE errors', async () => {
    runCommand.mockResolvedValueOnce(okResult);
    await run();
    expect(session.isReset).toBe(false);

    runCommand.mockRejectedValueOnce(Object.assign(new Error('INTERNAL'), { code: 13 }));
    await expect(run()).rejects.toMatchObject({ code: 13 });
    expect(session.isReset).toBe(false);
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('does not clear isReset when a stale success from an older generation arrives late', async () => {
    // Call A starts under generation 0. Before it resolves, call B fails with UNAVAILABLE,
    // bumping the generation to 1 and setting isReset=true.
    // A's late success must not clear the isReset that generation 1 installed.
    const callA = defer<typeof okResult>();
    runCommand.mockReturnValueOnce(callA.promise);

    const resultA = run();

    // B fails UNAVAILABLE → generation bumped to 1, isReset=true.
    runCommand.mockRejectedValueOnce(unavailable());
    await expect(run()).rejects.toMatchObject({ code: 14 });
    expect(session.isReset).toBe(true);

    // A's stale success (started in generation 0) arrives now.
    callA.resolve(okResult);
    await resultA;

    // isReset must still be true — a stale success from the old pod must not win.
    expect(session.isReset).toBe(true);
  });

  it('does not set isReset=true when a stale UNAVAILABLE from an older generation arrives late', async () => {
    // Call A and call B both run and fail with UNAVAILABLE (generation 0).
    const callB = defer<never>();
    runCommand.mockRejectedValueOnce(unavailable()).mockReturnValueOnce(callB.promise);

    const resultA = run();
    const resultB = run();
    await expect(resultA).rejects.toMatchObject({ code: 14 });
    // isReset is now true, generation bumped to 1.

    // Call G succeeds under generation 1.
    runCommand.mockResolvedValueOnce(okResult);
    await run();
    expect(session.isReset).toBe(false);

    // B's stale UNAVAILABLE (started in generation 0) arrives now.
    callB.reject(unavailable());
    await expect(resultB).rejects.toMatchObject({ code: 14 });

    // isReset must still be false — the stale failure must not win.
    expect(session.isReset).toBe(false);
    expect(logger.warn).toHaveBeenCalledTimes(1); // only A's reset logged
  });
});

describe('SandboxApiClient', () => {
  let client: SandboxApiClient;

  beforeEach(() => {
    jest.clearAllMocks();
    client = new SandboxApiClient({
      host: 'sandbox-api',
      port: 50051,
      apiKey: 'secret-key',
      rootCertPem: Buffer.from('mock-ca'),
      clientCertPem: Buffer.from('mock-cert'),
      clientKeyPem: Buffer.from('mock-key'),
    });
  });

  it('closes the underlying gRPC client', () => {
    client.close();
    expect(mockClose).toHaveBeenCalledTimes(1);
  });
});
