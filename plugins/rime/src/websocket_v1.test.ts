// SPDX-FileCopyrightText: 2026 LiveKit, Inc.
//
// SPDX-License-Identifier: Apache-2.0
import { APIConnectionError, type APIStatusError, initializeLogger, tts } from '@livekit/agents';
import { EventEmitter, once } from 'node:events';
import { afterEach, describe, expect, it } from 'vitest';
import { type RawData, type WebSocket, WebSocketServer } from 'ws';
import { TTS } from './tts.js';
import {
  RIME_V1_SUBPROTOCOL,
  RimeV1Connection,
  type RimeV1Input,
  type RimeV1Socket,
  type RimeV1SocketFactory,
  type RimeV1StartOptions,
  rimeV1WebSocketUrl,
} from './websocket_v1.js';

initializeLogger({ pretty: false, level: 'silent' });

class FakeSocket extends EventEmitter {
  protocol: string;
  readyState = 1;
  sent: Record<string, unknown>[] = [];

  constructor(protocol = RIME_V1_SUBPROTOCOL) {
    super();
    this.protocol = protocol;
  }

  send(data: string): void {
    this.sent.push(JSON.parse(data) as Record<string, unknown>);
    this.emit('sent');
  }

  close(): void {
    this.readyState = 3;
  }

  terminate(): void {
    this.readyState = 3;
  }

  push(value: Record<string, unknown>): void {
    this.emit('message', Buffer.from(JSON.stringify(value)), false);
  }

  async waitForSent(count: number): Promise<void> {
    while (this.sent.length < count) await once(this, 'sent');
  }
}

const startOptions: RimeV1StartOptions = {
  speaker: 'lyra',
  language: 'eng',
  sampleRate: 24000,
  timeScaleFactor: 1.1,
  maxTokens: 128,
  textLookaheadTokens: 4,
};

function fakeFactory(
  socket: FakeSocket,
  capture?: (options: { url: string; apiKey: string; timeoutMs: number }) => void,
): RimeV1SocketFactory {
  return async (options) => {
    capture?.(options);
    setTimeout(() => socket.push({ ready: { protocol: 1, languages: ['eng'] } }), 0);
    return socket as unknown as RimeV1Socket;
  };
}

async function connectFake(socket = new FakeSocket()): Promise<RimeV1Connection> {
  return await RimeV1Connection.connect({
    url: 'wss://engine.test/ws',
    apiKey: 'test-key',
    timeoutMs: 1000,
    socketFactory: fakeFactory(socket),
  });
}

describe('Rime v1 WebSocket connection', () => {
  it('waits for ready and sends the streaming envelope order', async () => {
    const socket = new FakeSocket();
    let connectOptions: { url: string; apiKey: string; timeoutMs: number } | undefined;
    const connection = await RimeV1Connection.connect({
      url: 'wss://engine.test/ws',
      apiKey: 'test-key',
      timeoutMs: 1000,
      socketFactory: fakeFactory(socket, (options) => {
        connectOptions = options;
      }),
    });
    expect(connectOptions).toEqual({
      url: 'wss://engine.test/ws',
      apiKey: 'test-key',
      timeoutMs: 1000,
    });

    async function* inputs(): AsyncGenerator<RimeV1Input> {
      yield { type: 'text', text: 'Hello ' };
      yield { type: 'flush' };
      yield { type: 'text', text: 'again.' };
    }

    const outputs: unknown[] = [];
    const consume = async () => {
      for await (const output of connection.synthesize({ start: startOptions, inputs: inputs() })) {
        outputs.push(output);
      }
    };
    const task = consume();
    await socket.waitForSent(5);
    const contextId = socket.sent[0]!.contextId as string;
    socket.push({ contextId, started: { requestId: 'request-1' } });
    socket.push({ contextId, audio: Buffer.from([1, 0, 2, 0]).toString('base64') });
    socket.push({ contextId, done: {} });
    await task;

    expect(
      socket.sent.map((frame) => Object.keys(frame).find((key) => key !== 'contextId')),
    ).toEqual(['start', 'text', 'flush', 'text', 'end']);
    expect(socket.sent[0]!.start).toEqual({
      speaker: 'lyra',
      language: 'eng',
      text: '',
      audioParameters: {
        audioFormat: 'audio/pcm',
        samplingRate: 24000,
        timeScaleFactor: 1.1,
      },
      codaParameters: { maxTokens: 128, textLookaheadTokens: 4 },
    });
    expect(outputs).toEqual([
      { type: 'started', contextId, requestId: 'request-1' },
      { type: 'audio', contextId, data: Buffer.from([1, 0, 2, 0]) },
    ]);
    expect(connection.reusable).toBe(true);
  });

  it('maps context errors without discarding the connection', async () => {
    const socket = new FakeSocket();
    const connection = await connectFake(socket);
    async function* inputs(): AsyncGenerator<RimeV1Input> {
      yield { type: 'text', text: 'Hello' };
    }
    const consume = async () => {
      for await (const _ of connection.synthesize({ start: startOptions, inputs: inputs() })) {
        // No output is expected.
      }
    };
    const task = consume();
    await socket.waitForSent(3);
    const contextId = socket.sent[0]!.contextId as string;
    socket.push({
      contextId,
      error: {
        kind: 'invalid_input',
        message: 'speaker not found',
        requestId: 'request-2',
      },
    });

    await expect(task).rejects.toMatchObject({
      statusCode: 400,
      requestId: 'request-2',
      retryable: false,
    } satisfies Partial<APIStatusError>);
    expect(connection.reusable).toBe(true);
  });

  it('discards the connection after invalid base64 audio', async () => {
    const socket = new FakeSocket();
    const connection = await connectFake(socket);
    async function* inputs(): AsyncGenerator<RimeV1Input> {
      yield { type: 'text', text: 'Hello' };
    }
    const consume = async () => {
      for await (const _ of connection.synthesize({ start: startOptions, inputs: inputs() })) {
        // The invalid audio fails before output.
      }
    };
    const task = consume();
    await socket.waitForSent(3);
    const contextId = socket.sent[0]!.contextId as string;
    socket.push({ contextId, started: { requestId: 'request-3' } });
    socket.push({ contextId, audio: 'not-base64' });

    await expect(task).rejects.toBeInstanceOf(APIConnectionError);
    expect(connection.reusable).toBe(false);
  });

  it('reuses a connection after acknowledged cancellation', async () => {
    const socket = new FakeSocket();
    const connection = await connectFake(socket);
    const hold = new Promise<void>(() => {});
    async function* inputs(): AsyncGenerator<RimeV1Input> {
      yield { type: 'text', text: 'Hello' };
      await hold;
    }
    const controller = new AbortController();
    const consume = async () => {
      for await (const _ of connection.synthesize({
        start: startOptions,
        inputs: inputs(),
        signal: controller.signal,
      })) {
        // The test cancels after started.
      }
    };
    const task = consume();
    await socket.waitForSent(2);
    const contextId = socket.sent[0]!.contextId as string;
    socket.push({ contextId, started: { requestId: 'request-4' } });
    controller.abort();
    await socket.waitForSent(3);
    expect(socket.sent.at(-1)).toEqual({ contextId, cancel: {} });
    socket.push({ contextId, cancelled: {} });

    await task;
    expect(connection.reusable).toBe(true);
  });

  it('rejects a missing selected subprotocol', async () => {
    const socket = new FakeSocket('');
    await expect(
      RimeV1Connection.connect({
        url: 'wss://engine.test/ws',
        apiKey: 'test-key',
        timeoutMs: 1000,
        socketFactory: fakeFactory(socket),
      }),
    ).rejects.toBeInstanceOf(APIConnectionError);
    expect(socket.readyState).toBe(3);
  });

  it('builds the engine WebSocket URL', () => {
    expect(rimeV1WebSocketUrl('https://engine.test')).toBe('wss://engine.test/ws');
    expect(rimeV1WebSocketUrl('wss://engine.test/base/')).toBe('wss://engine.test/base/ws');
    expect(rimeV1WebSocketUrl('ws://engine.test/ws')).toBe('ws://engine.test/ws');
    expect(() => rimeV1WebSocketUrl('engine.test')).toThrow(/absolute/);
  });
});

describe('Rime v1 TTS adapter', () => {
  let server: WebSocketServer | undefined;

  afterEach(async () => {
    if (!server) return;
    await new Promise<void>((resolve) => server!.close(() => resolve()));
    server = undefined;
  });

  it('keeps rime.v1 opt-in and validates its preview limits', () => {
    const legacy = new TTS({
      apiKey: 'test-key',
      modelId: 'coda',
      useWebsocket: true,
    });
    expect(legacy.capabilities.alignedTranscript).toBe(true);

    const v1 = new TTS({
      apiKey: 'test-key',
      baseURL: 'https://engine.test',
      modelId: 'coda',
      useWebsocket: true,
      websocketAPI: 'rime.v1',
    });
    expect(v1.sampleRate).toBe(24000);
    expect(v1.capabilities.alignedTranscript).toBe(false);

    expect(
      () =>
        new TTS({
          apiKey: 'test-key',
          modelId: 'coda',
          useWebsocket: true,
          websocketAPI: 'rime.v1',
        }),
    ).toThrow(/baseURL/);
    expect(
      () =>
        new TTS({
          apiKey: 'test-key',
          baseURL: 'https://engine.test',
          modelId: 'arcana',
          useWebsocket: true,
          websocketAPI: 'rime.v1',
        }),
    ).toThrow(/only modelId='coda'/);
    expect(
      () =>
        new TTS({
          apiKey: 'test-key',
          baseURL: 'https://engine.test',
          modelId: 'coda',
          speedAlpha: 1.1,
          useWebsocket: true,
          websocketAPI: 'rime.v1',
        }),
    ).toThrow(/speedAlpha/);
  });

  it('emits engine audio with the engine request ID', async () => {
    let authorization: string | undefined;
    let connectionCount = 0;
    server = new WebSocketServer({ port: 0 });
    await once(server, 'listening');
    server.on('connection', (socket: WebSocket, request) => {
      connectionCount += 1;
      authorization = request.headers.authorization;
      socket.send(JSON.stringify({ ready: { protocol: 1, languages: ['eng'] } }));
      socket.on('message', (raw: RawData) => {
        const message = JSON.parse(raw.toString()) as Record<string, unknown>;
        const contextId = message.contextId as string;
        if ('start' in message) {
          socket.send(JSON.stringify({ contextId, started: { requestId: 'engine-request' } }));
        } else if ('text' in message) {
          socket.send(
            JSON.stringify({
              contextId,
              audio: Buffer.alloc(4800 * 2, 1).toString('base64'),
            }),
          );
        } else if ('end' in message) {
          socket.send(JSON.stringify({ contextId, done: {} }));
        }
      });
    });

    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('test server has no TCP address');
    const rime = new TTS({
      apiKey: 'test-key',
      baseURL: `http://127.0.0.1:${address.port}`,
      modelId: 'coda',
      speaker: 'lyra',
      useWebsocket: true,
      websocketAPI: 'rime.v1',
    });
    const stream = rime.stream();
    stream.pushText('Hello');
    stream.endInput();

    const events = [];
    for await (const event of stream) {
      if (event !== tts.SynthesizeStream.END_OF_STREAM) events.push(event);
    }
    expect(authorization).toBe('Api-Key test-key');
    expect(events.length).toBeGreaterThan(0);
    expect(events[0]!.requestId).toBe('engine-request');
    expect(events[0]!.frame.sampleRate).toBe(24000);
    expect(events.at(-1)!.final).toBe(true);

    const secondStream = rime.stream();
    secondStream.pushText('Again');
    secondStream.endInput();
    const secondEvents = [];
    for await (const event of secondStream) {
      if (event !== tts.SynthesizeStream.END_OF_STREAM) secondEvents.push(event);
    }
    expect(secondEvents[0]!.requestId).toBe('engine-request');
    expect(secondEvents[0]!.segmentId).not.toBe(events[0]!.segmentId);
    expect(connectionCount).toBe(1);
    await rime.close();
  });
});
