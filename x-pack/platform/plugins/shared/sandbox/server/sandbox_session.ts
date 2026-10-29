/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import { status as grpcStatus } from '@grpc/grpc-js';
import type { Logger } from '@kbn/core/server';
import type {
  SandboxApiClient,
  RpcOptions,
  RunCommandParams,
  RunCommandResult,
  FileMetadata,
  ReadFileResult,
  WriteFileResult,
} from './grpc_client';

/**
 * A per-session handle to a gVisor sandbox pod.
 *
 * Each session is scoped to a `(spaceId, sessionId)` pair and proxies RPCs
 * to a single pod via the sandbox-api gRPC service. Obtain a session from
 * {@link SandboxPluginStart.getSession} — the plugin manages session lifecycle
 * and pod allocation transparently.
 *
 * **Pod eviction and `isReset`**
 *
 * Pods can be evicted at any time (OOM, node pressure, deploy). When a call
 * fails with gRPC status `UNAVAILABLE` (code 14) the session treats that as a
 * pod eviction: `isReset` flips to `true` and the error is re-thrown so callers
 * can react (e.g. surface a retry hint). After the next successful call
 * `isReset` returns to `false`.
 *
 * Callers that need to seed a fresh pod (write config files, restore state)
 * should check `isReset` before each invocation and perform setup when it is
 * `true`. The check-then-setup pattern is inherently best-effort: a pod eviction
 * that occurs between the check and the setup call will be caught on the next
 * invocation.
 *
 * **Concurrency**
 *
 * Multiple concurrent calls are safe. A generation counter ensures that a
 * late-arriving UNAVAILABLE from an already-replaced pod does not incorrectly
 * flip `isReset` back to `true`, and a late-arriving success from an old pod
 * does not prematurely clear a `true` set by a newer eviction.
 */
export interface SandboxSession {
  /**
   * `true` when the pod is fresh and has not yet been seeded by the caller.
   *
   * Starts `true`. Flips to `false` after the first successful operation.
   * Flips back to `true` after a pod eviction (gRPC `UNAVAILABLE`, code 14).
   *
   * Check this before each tool invocation and run any workspace setup
   * (e.g. writing a connector manifest) when it is `true`.
   */
  readonly isReset: boolean;

  /**
   * Runs a shell command inside the sandbox pod and streams stdout/stderr.
   *
   * Every method accepts optional {@link RpcOptions}. `correlationId` is sent to sandbox-api as
   * `x-correlation-id` so its logs can be joined with the Kibana tool call or workflow step that
   * issued the RPC. A value that is not 1-128 printable ASCII characters is not sent.
   *
   * Log correlation contract: each RPC also writes one debug log line here, and sandbox-api is
   * expected to log the same fields, with the same values, for the same call. The `rpc.*` fields
   * follow the OpenTelemetry semantic conventions as of `@opentelemetry/semantic-conventions`
   * 1.43.0 (the version Kibana pins), not the deprecated `rpc.system`, `rpc.service` and
   * `rpc.grpc.status_code`. `event.duration` is the ECS field. Overall outcome is derived from
   * `rpc.response.status_code` rather than logged separately as `event.outcome`.
   *
   * | Field                       | Value                                     | sandbox-api source         |
   * | --------------------------- | ----------------------------------------- | -------------------------- |
   * | `sandbox.correlation_id`    | `RpcOptions.correlationId` (when sent)    | `x-correlation-id` header  |
   * | `sandbox.conversation_id`   | `<space>__<conversation>`                 | `x-conversation-id` header |
   * | `rpc.system.name`           | `grpc`                                    | `grpc`                     |
   * | `rpc.method`                | `sandbox.SandboxService/RunCommand`, ...  | fully-qualified gRPC method |
   * | `rpc.response.status_code`  | gRPC status name, e.g. `OK`, `UNAVAILABLE` | gRPC status name           |
   * | `event.duration`            | nanoseconds, measured client-side         | nanoseconds, server-side   |
   */
  runCommand(params: RunCommandParams, options?: RpcOptions): Promise<RunCommandResult>;

  /** Reads one or more files from the sandbox pod's filesystem. */
  readFiles(
    requests: Array<{ path: string; maxReadBytes?: number }>,
    options?: RpcOptions
  ): Promise<ReadFileResult[]>;

  /** Writes one or more files to the sandbox pod's filesystem. */
  writeFiles(
    files: Array<{ path: string; content: Buffer }>,
    options?: RpcOptions
  ): Promise<WriteFileResult[]>;

  /** Creates directories (including parents) inside the sandbox pod. */
  mkdirs(paths: string[], options?: RpcOptions): Promise<boolean[]>;

  /** Returns metadata (existence, size, type) for paths inside the sandbox pod. */
  statFiles(paths: string[], options?: RpcOptions): Promise<FileMetadata[]>;
}

const RPC_SERVICE = 'sandbox.SandboxService';

// OpenTelemetry `rpc.response.status_code` for gRPC is the status name, e.g. `OK` or `UNAVAILABLE`.
const grpcStatusName = (code: number): string => grpcStatus[code] ?? String(code);

// ---------------------------------------------------------------------------
// SandboxSessionImpl — per-session wrapper over SandboxApiClient.
//
// Uses a generation counter to protect against a classic race: caller A and
// caller B both start under generation N against a dead pod. A's UNAVAILABLE
// arrives first and increments the generation to N+1. Caller G then runs
// successfully under N+1, setting isReset=false. B's stale UNAVAILABLE arrives
// late — its stored generation (N) no longer matches the current generation
// (N+1), so it does NOT reset isReset back to true.
// ---------------------------------------------------------------------------

export class SandboxSessionImpl implements SandboxSession {
  private _isReset = true;
  private _generation = 0;

  constructor(
    private readonly sessionId: string,
    private readonly apiClient: SandboxApiClient,
    private readonly logger: Logger
  ) {}

  public get isReset(): boolean {
    return this._isReset;
  }

  async runCommand(params: RunCommandParams, options?: RpcOptions): Promise<RunCommandResult> {
    return this.execute('RunCommand', options, (rpcOptions) =>
      this.apiClient.runCommand(this.sessionId, params, rpcOptions)
    );
  }

  async readFiles(
    requests: Array<{ path: string; maxReadBytes?: number }>,
    options?: RpcOptions
  ): Promise<ReadFileResult[]> {
    return this.execute('ReadFiles', options, (rpcOptions) =>
      this.apiClient.readFiles(this.sessionId, requests, rpcOptions)
    );
  }

  async writeFiles(
    files: Array<{ path: string; content: Buffer }>,
    options?: RpcOptions
  ): Promise<WriteFileResult[]> {
    return this.execute('WriteFiles', options, (rpcOptions) =>
      this.apiClient.writeFiles(this.sessionId, files, rpcOptions)
    );
  }

  async mkdirs(paths: string[], options?: RpcOptions): Promise<boolean[]> {
    return this.execute('Mkdirs', options, (rpcOptions) =>
      this.apiClient.mkdirs(this.sessionId, paths, rpcOptions)
    );
  }

  async statFiles(paths: string[], options?: RpcOptions): Promise<FileMetadata[]> {
    return this.execute('StatFiles', options, (rpcOptions) =>
      this.apiClient.statFiles(this.sessionId, paths, rpcOptions)
    );
  }

  private async execute<T>(
    method: string,
    options: RpcOptions = {},
    call: (options: RpcOptions) => Promise<T>
  ): Promise<T> {
    const gen = this._generation;
    const startedAt = process.hrtime.bigint();
    let grpcStatusCode: number = grpcStatus.OK;
    try {
      const result = await call(options);
      if (this._generation === gen) {
        this._isReset = false;
      }
      return result;
    } catch (err) {
      const code = (err as any)?.code;
      grpcStatusCode = typeof code === 'number' ? code : grpcStatus.UNKNOWN;
      if (code === 14 /* UNAVAILABLE */ && this._generation === gen) {
        this._generation++;
        this._isReset = true;
        this.logger.warn(
          `Sandbox pod evicted for session ${this.sessionId} — isReset set for next call`
        );
      }
      throw err;
    } finally {
      // Field contract with sandbox-api: see SandboxSession.
      const statusName = grpcStatusName(grpcStatusCode);
      this.logger.debug(`Sandbox RPC ${method} finished with gRPC status ${statusName}`, {
        sandbox: {
          conversation_id: this.sessionId,
          ...(options.correlationId ? { correlation_id: options.correlationId } : {}),
        },
        rpc: {
          system: { name: 'grpc' },
          method: `${RPC_SERVICE}/${method}`,
          response: { status_code: statusName },
        },
        event: { duration: Number(process.hrtime.bigint() - startedAt) },
      });
    }
  }
}
