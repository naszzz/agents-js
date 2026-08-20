<!--
SPDX-FileCopyrightText: 2024 LiveKit, Inc.

SPDX-License-Identifier: Apache-2.0
-->

# Rime plugin for LiveKit Agents

The Agents Framework is designed for building realtime, programmable
participants that run on servers. Use it to create conversational, multi-modal
voice agents that can see, hear, and understand.

This package contains the Rime plugin, which provides high-quality text-to-speech (TTS) capabilities for voice synthesis. Refer to the
[documentation](https://docs.livekit.io/agents/overview/) for information on how to use it,
or browse the [API reference](https://docs.livekit.io/agents-js/modules/plugins_agents_plugin_rime.html).
See the [repository](https://github.com/livekit/agents-js) for more information
about the framework as a whole.

## Streaming WebSocket interfaces

The plugin keeps the public Rime `/ws3` interface as its default streaming path. Set
`websocketAPI: 'rime.v1'` to use the Rime engine streaming interface during its preview:

```typescript
import { TTS } from '@livekit/agents-plugin-rime';

const tts = new TTS({
  modelId: 'coda',
  speaker: 'lyra',
  useWebsocket: true,
  websocketAPI: 'rime.v1',
  baseURL: 'https://your-rime-engine.example.com',
  textLookaheadTokens: 4,
});
```

The preview interface requires an explicit `baseURL` and supports Coda only. It uses
`rime.v1.json` at `GET /ws`, sends the API key with the `Api-Key` authorization scheme, and
defaults to 24 kHz PCM. It does not provide aligned transcripts or support `speedAlpha`.
