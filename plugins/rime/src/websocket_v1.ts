// SPDX-FileCopyrightText: 2026 LiveKit, Inc.
//
// SPDX-License-Identifier: Apache-2.0
import {
  APIConnectionError,
  APIStatusError,
  APITimeoutError,
  asError,
  shortuuid,
} from '@livekit/agents';
import { type RawData, WebSocket } from 'ws';

export const RIME_V1_SUBPROTOCOL = 'rime.v1.json';
const CANCEL_TIMEOUT_MS = 1000;
const OPEN = 1;
const CONNECTING = 0;

type MessageListener = (data: RawData, isBinary: boolean) => void;
type CloseListener = (code: number, reason: Buffer) => void;
type ErrorListener = (error: Error) => void;
type OpenListener = () => void;

export interface RimeV1Socket {
  readonly protocol: string;
  readonly readyState: number;
  on(event: 'open', listener: OpenListener): this;
  on(event: 'message', listener: MessageListener): this;
  on(event: 'close', listener: CloseListener): this;
  on(event: 'error', listener: ErrorListener): this;
  off(event: 'open', listener: OpenListener): this;
  off(event: 'message', listener: MessageListener): this;
  off(event: 'close', listener: CloseListener): this;
  off(event: 'error', listener: ErrorListener): this;
  send(data: string): void;
  close(): void;
  terminate(): void;
}

export type RimeV1SocketFactory = (options: {
  url: string;
  apiKey: string;
  timeoutMs: number;
}) => Promise<RimeV1Socket>;

export interface RimeV1StartOptions {
  speaker: string;
  language: string;
  sampleRate: number;
  timeScaleFactor?: number;
  maxTokens?: number;
  textLookaheadTokens?: number;
}

export type RimeV1Input = { type: 'text'; text: string } | { type: 'flush' };

export type RimeV1Output =
  | { type: 'started'; contextId: string; requestId: string }
  | { type: 'audio'; contextId: string; data: Uint8Array };

type Envelope = Record<string, unknown>;

type Waiter = {
  resolve: (value: Envelope) => void;
  reject: (error: Error) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
};

class MessageInbox {
  #messages: Envelope[] = [];
  #waiters: Waiter[] = [];
  #error?: Error;

  push(message: Envelope): void {
    const waiter = this.#waiters.shift();
    if (waiter) {
      this.#cleanup(waiter);
      waiter.resolve(message);
      return;
    }
    this.#messages.push(message);
  }

  fail(error: Error): void {
    if (this.#error) return;
    this.#error = error;
    for (const waiter of this.#waiters.splice(0)) {
      this.#cleanup(waiter);
      waiter.reject(error);
    }
  }

  next(signal?: AbortSignal): Promise<Envelope> {
    const message = this.#messages.shift();
    if (message) return Promise.resolve(message);
    if (this.#error) return Promise.reject(this.#error);
    if (signal?.aborted) return Promise.reject(abortError());

    return new Promise<Envelope>((resolve, reject) => {
      const waiter: Waiter = { resolve, reject, signal };
      if (signal) {
        waiter.onAbort = () => {
          const index = this.#waiters.indexOf(waiter);
          if (index >= 0) this.#waiters.splice(index, 1);
          this.#cleanup(waiter);
          reject(abortError());
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.#waiters.push(waiter);
    });
  }

  #cleanup(waiter: Waiter): void {
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener('abort', waiter.onAbort);
    }
  }
}

function abortError(): Error {
  const error = new Error('The operation was aborted.');
  error.name = 'AbortError';
  return error;
}

function statusForErrorKind(kind: string): { statusCode: number; retryable: boolean } {
  const known: Record<string, { statusCode: number; retryable: boolean }> = {
    invalid_input: { statusCode: 400, retryable: false },
    unauthenticated: { statusCode: 401, retryable: false },
    resource_exhausted: { statusCode: 429, retryable: true },
    unimplemented: { statusCode: 501, retryable: false },
    unavailable: { statusCode: 503, retryable: true },
    internal: { statusCode: 500, retryable: true },
  };
  return known[kind] ?? { statusCode: 500, retryable: true };
}

function engineError(value: unknown): APIStatusError {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return new APIStatusError({
      message: 'Rime v1 returned an invalid error payload',
      options: { statusCode: 500, retryable: false },
    });
  }

  const data = value as Record<string, unknown>;
  const kind = typeof data.kind === 'string' ? data.kind : 'unknown';
  const { statusCode, retryable } = statusForErrorKind(kind);
  return new APIStatusError({
    message: typeof data.message === 'string' ? data.message : 'Rime synthesis failed',
    options: {
      statusCode,
      retryable,
      requestId: typeof data.requestId === 'string' ? data.requestId : null,
      body: { kind },
    },
  });
}

function startEnvelope(contextId: string, options: RimeV1StartOptions): Envelope {
  const audioParameters: Record<string, unknown> = {
    audioFormat: 'audio/pcm',
    samplingRate: options.sampleRate,
  };
  if (options.timeScaleFactor !== undefined) {
    audioParameters.timeScaleFactor = options.timeScaleFactor;
  }

  const codaParameters: Record<string, unknown> = {};
  if (options.maxTokens !== undefined) codaParameters.maxTokens = options.maxTokens;
  if (options.textLookaheadTokens !== undefined) {
    codaParameters.textLookaheadTokens = options.textLookaheadTokens;
  }

  return {
    contextId,
    start: {
      speaker: options.speaker,
      language: options.language,
      text: '',
      audioParameters,
      ...(Object.keys(codaParameters).length > 0 ? { codaParameters } : {}),
    },
  };
}

async function openWebSocket({
  url,
  apiKey,
  timeoutMs,
}: {
  url: string;
  apiKey: string;
  timeoutMs: number;
}): Promise<RimeV1Socket> {
  return new WebSocket(url, RIME_V1_SUBPROTOCOL, {
    headers: { Authorization: `Api-Key ${apiKey}` },
    handshakeTimeout: timeoutMs,
  });
}

export class RimeV1Connection {
  #socket: RimeV1Socket;
  #inbox = new MessageInbox();
  #healthy = true;
  #ready = false;
  #activeContextId?: string;

  #onMessage: MessageListener = (data, isBinary) => {
    if (isBinary) {
      this.#fail(this.#protocolError('Rime v1 JSON mode received a binary frame'));
      return;
    }
    try {
      let buffer: Buffer;
      if (Buffer.isBuffer(data)) buffer = data;
      else if (Array.isArray(data)) buffer = Buffer.concat(data);
      else buffer = Buffer.from(data as ArrayBuffer);
      const value: unknown = JSON.parse(buffer.toString('utf8'));
      if (!value || typeof value !== 'object' || Array.isArray(value)) {
        throw this.#protocolError('Rime v1 returned a non-object envelope');
      }
      this.#inbox.push(value as Envelope);
    } catch (error) {
      this.#fail(
        error instanceof APIConnectionError
          ? error
          : this.#protocolError('Rime v1 returned malformed JSON'),
      );
    }
  };

  #onClose: CloseListener = () => {
    this.#fail(new APIConnectionError({ message: 'Rime v1 WebSocket closed unexpectedly' }));
  };

  #onError: ErrorListener = () => {
    this.#fail(new APIConnectionError({ message: 'Rime v1 WebSocket failed' }));
  };

  private constructor(socket: RimeV1Socket) {
    this.#socket = socket;
    socket.on('message', this.#onMessage);
    socket.on('close', this.#onClose);
    socket.on('error', this.#onError);
  }

  static async connect({
    url,
    apiKey,
    timeoutMs,
    socketFactory = openWebSocket,
  }: {
    url: string;
    apiKey: string;
    timeoutMs: number;
    socketFactory?: RimeV1SocketFactory;
  }): Promise<RimeV1Connection> {
    const socket = await socketFactory({ url, apiKey, timeoutMs });
    const connection = new RimeV1Connection(socket);
    try {
      await connection.#waitOpen(timeoutMs);
      if (socket.protocol !== RIME_V1_SUBPROTOCOL) {
        throw connection.#protocolError(
          `Rime selected WebSocket subprotocol ${JSON.stringify(socket.protocol)}, expected ${JSON.stringify(RIME_V1_SUBPROTOCOL)}`,
        );
      }
      await connection.#waitReady(timeoutMs);
      return connection;
    } catch (error) {
      await connection.close();
      throw error;
    }
  }

  get reusable(): boolean {
    return (
      this.#healthy &&
      this.#ready &&
      this.#activeContextId === undefined &&
      this.#socket.readyState === OPEN
    );
  }

  async close(): Promise<void> {
    this.#healthy = false;
    this.#socket.off('message', this.#onMessage);
    this.#socket.off('close', this.#onClose);
    if (this.#socket.readyState === OPEN) this.#socket.close();
    else if (this.#socket.readyState === CONNECTING) this.#socket.terminate();
  }

  async *synthesize({
    start,
    inputs,
    signal,
  }: {
    start: RimeV1StartOptions;
    inputs: AsyncIterable<RimeV1Input>;
    signal?: AbortSignal;
  }): AsyncGenerator<RimeV1Output> {
    if (!this.reusable) {
      throw new APIConnectionError({
        message: 'Rime v1 connection is not ready for a new context',
        options: { retryable: false },
      });
    }

    const contextId = shortuuid();
    this.#activeContextId = contextId;
    let terminal = false;
    let started = false;
    const sendController = new AbortController();
    const sender = this.#sendContext(contextId, start, inputs, sendController.signal);
    const senderFailure = sender.then(
      () => new Promise<never>(() => {}),
      (error: unknown) => ({ type: 'sender_error' as const, error }),
    );

    try {
      while (true) {
        const receiveController = new AbortController();
        const choices: Promise<
          | { type: 'message'; message: Envelope }
          | { type: 'receive_error'; error: unknown }
          | { type: 'sender_error'; error: unknown }
          | { type: 'abort' }
        >[] = [
          this.#inbox.next(receiveController.signal).then(
            (message) => ({ type: 'message' as const, message }),
            (error: unknown) => ({ type: 'receive_error' as const, error }),
          ),
          senderFailure,
        ];
        if (signal) choices.push(waitForAbort(signal));

        const result = await Promise.race(choices);
        if (result.type !== 'message' && result.type !== 'receive_error') {
          receiveController.abort();
        }

        if (result.type === 'abort') {
          sendController.abort();
          await sender.catch(() => {});
          terminal = await this.#cancelContext(contextId);
          return;
        }
        if (result.type === 'sender_error') throw result.error;
        if (result.type === 'receive_error') throw result.error;

        const [event, payload] = this.#payload(result.message);
        const responseContext =
          typeof result.message.contextId === 'string' ? result.message.contextId : '';
        if (event === 'error' && responseContext === '') {
          this.#healthy = false;
          throw engineError(payload);
        }
        if (responseContext !== contextId) {
          throw this.#protocolError('Rime v1 returned an event for an unexpected context');
        }

        if (event === 'started') {
          if (started || !payload || typeof payload !== 'object' || Array.isArray(payload)) {
            throw this.#protocolError('Rime v1 returned an invalid started event');
          }
          const requestId = (payload as Record<string, unknown>).requestId;
          if (typeof requestId !== 'string') {
            throw this.#protocolError('Rime v1 started event has no requestId');
          }
          started = true;
          yield { type: 'started', contextId, requestId };
        } else if (event === 'audio') {
          if (!started || typeof payload !== 'string') {
            throw this.#protocolError('Rime v1 returned audio before started');
          }
          const data = Buffer.from(payload, 'base64');
          if (data.toString('base64').replace(/=+$/, '') !== payload.replace(/=+$/, '')) {
            throw this.#protocolError('Rime v1 returned invalid base64 audio');
          }
          yield { type: 'audio', contextId, data };
        } else if (event === 'done') {
          if (!started) throw this.#protocolError('Rime v1 returned done before started');
          terminal = true;
          return;
        } else if (event === 'cancelled') {
          terminal = true;
          throw new APIStatusError({
            message: 'Rime synthesis was cancelled',
            options: { statusCode: 499, retryable: false },
          });
        } else if (event === 'error') {
          terminal = true;
          throw engineError(payload);
        } else {
          throw this.#protocolError(`Rime v1 returned unexpected ${JSON.stringify(event)} event`);
        }
      }
    } catch (error) {
      if (!terminal) this.#healthy = false;
      throw error;
    } finally {
      sendController.abort();
      await sender.catch(() => {});
      this.#activeContextId = undefined;
      if (!terminal) this.#healthy = false;
    }
  }

  async #waitReady(timeoutMs: number): Promise<void> {
    let message: Envelope;
    try {
      message = await this.#nextWithTimeout(
        timeoutMs,
        'Rime v1 did not send ready before the timeout',
      );
    } catch (error) {
      throw error;
    }
    const [event, payload] = this.#payload(message);
    const contextId = typeof message.contextId === 'string' ? message.contextId : '';
    if (event === 'error' && contextId === '') throw engineError(payload);
    if (event !== 'ready' || contextId !== '') {
      throw this.#protocolError('Rime v1 must send ready before context events');
    }
    if (!payload || typeof payload !== 'object' || (payload as Envelope).protocol !== 1) {
      throw this.#protocolError('Rime v1 returned an unsupported protocol version');
    }
    this.#ready = true;
  }

  async #waitOpen(timeoutMs: number): Promise<void> {
    if (this.#socket.readyState === OPEN) return;
    if (this.#socket.readyState !== CONNECTING) {
      throw new APIConnectionError({ message: 'Rime v1 WebSocket failed to open' });
    }

    await new Promise<void>((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const cleanup = () => {
        if (timer) clearTimeout(timer);
        this.#socket.off('open', onOpen);
        this.#socket.off('error', onError);
        this.#socket.off('close', onClose);
      };
      const onOpen = () => {
        cleanup();
        resolve();
      };
      const onError = () => {
        cleanup();
        reject(new APIConnectionError({ message: 'Rime v1 WebSocket connection failed' }));
      };
      const onClose = () => {
        cleanup();
        reject(new APIConnectionError({ message: 'Rime v1 WebSocket closed during connection' }));
      };

      this.#socket.on('open', onOpen);
      this.#socket.on('error', onError);
      this.#socket.on('close', onClose);
      if (timeoutMs > 0) {
        timer = setTimeout(() => {
          cleanup();
          reject(new APITimeoutError({ message: 'Rime v1 WebSocket connection timed out' }));
        }, timeoutMs);
      }
    });
  }

  async #sendContext(
    contextId: string,
    start: RimeV1StartOptions,
    inputs: AsyncIterable<RimeV1Input>,
    signal: AbortSignal,
  ): Promise<void> {
    this.#send(startEnvelope(contextId, start));
    const iterator = inputs[Symbol.asyncIterator]();
    try {
      while (!signal.aborted) {
        const result = await Promise.race([
          iterator.next().then((next) => ({ type: 'input' as const, next })),
          waitForAbort(signal),
        ]);
        if (result.type === 'abort') return;
        if (result.next.done) {
          this.#send({ contextId, end: {} });
          return;
        }
        if (result.next.value.type === 'text') {
          if (result.next.value.text) {
            this.#send({ contextId, text: result.next.value.text });
          }
        } else {
          this.#send({ contextId, flush: {} });
        }
      }
    } finally {
      if (!signal.aborted) await iterator.return?.();
    }
  }

  async #cancelContext(contextId: string): Promise<boolean> {
    if (this.#socket.readyState !== OPEN) {
      this.#healthy = false;
      return false;
    }
    try {
      this.#send({ contextId, cancel: {} });
      while (true) {
        const message = await this.#nextWithTimeout(
          CANCEL_TIMEOUT_MS,
          'Rime v1 cancellation acknowledgement timed out',
        );
        const [event, payload] = this.#payload(message);
        const responseContext = typeof message.contextId === 'string' ? message.contextId : '';
        if (event === 'error' && responseContext === '') {
          this.#healthy = false;
          return false;
        }
        if (responseContext !== contextId) {
          this.#healthy = false;
          return false;
        }
        if (event === 'cancelled' || event === 'done' || event === 'error') return true;
        if (event !== 'started' && event !== 'audio') {
          this.#healthy = false;
          return false;
        }
        void payload;
      }
    } catch {
      this.#healthy = false;
      return false;
    }
  }

  async #nextWithTimeout(timeoutMs: number, message: string): Promise<Envelope> {
    if (timeoutMs <= 0) return await this.#inbox.next();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await this.#inbox.next(controller.signal);
    } catch (error) {
      if (controller.signal.aborted) throw new APITimeoutError({ message });
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }

  #payload(message: Envelope): [string, unknown] {
    const events = ['ready', 'started', 'audio', 'done', 'cancelled', 'error'].filter(
      (name) => name in message,
    );
    if (events.length !== 1) {
      throw this.#protocolError('Rime v1 envelope must contain exactly one event');
    }
    const event = events[0]!;
    return [event, message[event]];
  }

  #send(value: Envelope): void {
    if (this.#socket.readyState !== OPEN) {
      this.#healthy = false;
      throw new APIConnectionError({ message: 'Rime v1 WebSocket is closed' });
    }
    try {
      this.#socket.send(JSON.stringify(value));
    } catch {
      this.#healthy = false;
      throw new APIConnectionError({ message: 'Rime v1 WebSocket send failed' });
    }
  }

  #protocolError(message: string): APIConnectionError {
    this.#healthy = false;
    return new APIConnectionError({ message, options: { retryable: false } });
  }

  #fail(error: Error): void {
    this.#healthy = false;
    this.#inbox.fail(error);
  }
}

function waitForAbort(signal: AbortSignal): Promise<{ type: 'abort' }> {
  if (signal.aborted) return Promise.resolve({ type: 'abort' });
  return new Promise((resolve) => {
    signal.addEventListener('abort', () => resolve({ type: 'abort' }), { once: true });
  });
}

export function rimeV1WebSocketUrl(baseURL: string): string {
  let url: URL;
  try {
    url = new URL(baseURL);
  } catch (error) {
    throw new Error(`baseURL for rime.v1 must be an absolute HTTP or WebSocket URL`, {
      cause: asError(error),
    });
  }
  if (url.protocol === 'http:') url.protocol = 'ws:';
  else if (url.protocol === 'https:') url.protocol = 'wss:';
  else if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('baseURL for rime.v1 must use HTTP or WebSocket');
  }
  const path = url.pathname.replace(/\/+$/, '');
  url.pathname = path.endsWith('/ws') ? path : `${path}/ws`;
  url.hash = '';
  return url.toString();
}
