# phoenix_socket

[![ci-test](https://github.com/braverhealth/phoenix-socket-dart/actions/workflows/test.yaml/badge.svg)](https://github.com/braverhealth/phoenix-socket-dart/actions/workflows/test.yaml)
[![pub-package](https://img.shields.io/pub/v/phoenix_socket.svg)](https://pub.dev/packages/phoenix_socket)
![Pub Points](https://img.shields.io/pub/points/phoenix_socket?color=blue&label=pub%20points)

Dart library to interact with [Phoenix][1] [Channels][2] ([Presence][3] support is currently _experimental_) over WebSockets.

This library uses [web_socket_channel][4] for WebSockets, making the API consistent across web and native
environments.

## Getting Started

Look at the [example project][5] for an example on how to use this library. The API was designed to
look like javascript's as much as possible, but leveraging Dart's unique native advantages like Streams
and Futures.

## Optional connection diagnostics

`PhoenixSocketOptions.onDiagnostic` observes payload-free enum events from the
existing heartbeat and socket handlers. It does not subscribe to messages,
decode frames again, or change heartbeat/reconnect policy. Keep the callback
inexpensive; exceptions from it are ignored.

`heartbeatSent` means the local sink accepted the heartbeat, and
`heartbeatAcknowledged` means the current heartbeat reference was matched by the
existing receive handler. `heartbeatClosePending` and `heartbeatCloseTimeout`
identify two different local close decisions. They do not establish why a reply
was missing. A local close cause cannot be inferred from the peer close reason:
an adapter may complete its stream before it receives peer close details.

For a deeper development-only probe, `onHeartbeatDiagnostic` enables immutable
scalar snapshots at `heartbeatSent` and `heartbeatClosePending`. It counts
complete accepted text messages (UTF-16 code units, not encoded bytes) and binary
messages/bytes, primary serializer attempts/errors/duration, and fixed
`phoenix`/`phx_reply` matched/unmatched/no-pending classifications. It exposes no
raw references, topics, payloads, endpoints, or errors. The current-send interval
is reset before the Sent snapshot, so it starts at zero; generation totals retain
earlier activity. Interval decode maxima are measured separately. Receive and
decode counters describe their respective boundaries: a message received before
Sent can be decoded afterward, so their interval counts need not be equal.

This optional probe times only the existing primary decoder. Public
`messageStream` consumers retain their existing independent decoding behavior,
and are not added to diagnostic decode totals. Internal debug envelopes retain
the original raw object and transport-generation accumulator without copying
payloads. A virtual `onSocketDataCallback` override still receives the raw object;
explicit external calls to that callback use the current attempt's accumulator.
Queued/late old-transport delivery cannot add to the new attempt's counters.
With this observer absent, no receive-diagnostic clocks/envelopes are created.

Complete-message silence is not evidence of server silence: the WebSocket
adapter delivers data after a complete message, so an unfinished large message,
network delay and delayed server response cannot be distinguished by this probe.
Serializer errors keep their original propagation behavior; observers cannot
throw into the transport. Enable this only when investigating a connection.

[1]: https://www.phoenixframework.org/
[2]: https://hexdocs.pm/phoenix/Phoenix.Channel.html#content
[3]: https://hexdocs.pm/phoenix/Phoenix.Presence.html#content
[4]: https://pub.dev/packages/web_socket_channel
[5]: https://github.com/matehat/phoenix-socket-dart/tree/master/example
