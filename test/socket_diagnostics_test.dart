import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:mockito/mockito.dart';
import 'package:phoenix_socket/phoenix_socket.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'mocks.dart';

void main() {
  test('heartbeat diagnostics observe replies without another decode', () {
    fakeAsync((async) {
      final harness = _SocketHarness(replyToEveryHeartbeat: true);
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 15));

      expect(harness.socket.isConnected, isTrue);
      expect(harness.events, [
        PhoenixSocketDiagnosticEvent.heartbeatSent,
        PhoenixSocketDiagnosticEvent.heartbeatAcknowledged,
        PhoenixSocketDiagnosticEvent.heartbeatSent,
        PhoenixSocketDiagnosticEvent.heartbeatAcknowledged,
      ]);
      expect(harness.decodedMessages, 2);
      harness.socket.dispose();
      async.flushMicrotasks();
    });
  });

  test('outstanding heartbeat identifies local close without peer details', () {
    fakeAsync((async) {
      final harness = _SocketHarness();
      PhoenixSocketCloseEvent? close;
      harness.socket.closeStream.listen((event) => close = event);
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 30));

      expect(harness.events, [
        PhoenixSocketDiagnosticEvent.heartbeatSent,
        PhoenixSocketDiagnosticEvent.heartbeatAcknowledged,
        PhoenixSocketDiagnosticEvent.heartbeatSent,
        PhoenixSocketDiagnosticEvent.heartbeatClosePending,
        PhoenixSocketDiagnosticEvent.socketStreamDone,
      ]);
      expect(close?.code, isNull);
      expect(close?.reason, 'WebSocket could not establish a connection');
      expect(harness.socket.isConnected, isFalse);
      harness.socket.dispose();
      async.flushMicrotasks();
    });
  });

  test('reply future timeout is distinct from the next heartbeat tick', () {
    fakeAsync((async) {
      final harness = _SocketHarness(
        heartbeat: const Duration(seconds: 100),
        heartbeatTimeout: const Duration(seconds: 5),
      );
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 105));

      expect(
          harness.events,
          contains(
            PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout,
          ));
      expect(
          harness.events,
          isNot(contains(
            PhoenixSocketDiagnosticEvent.heartbeatClosePending,
          )));
      expect(harness.socket.isConnected, isFalse);
      harness.socket.dispose();
      async.flushMicrotasks();
    });
  });

  test('stream errors and stream completion have different observations', () {
    fakeAsync((async) {
      final failed = _SocketHarness();
      failed.socket.connect();
      async.flushMicrotasks();
      failed.events.clear();
      failed.incoming.addError(WebSocketChannelException('private detail'));
      async.flushMicrotasks();
      expect(failed.events, [PhoenixSocketDiagnosticEvent.socketError]);
      failed.socket.dispose();

      final completed = _SocketHarness();
      completed.socket.connect();
      async.flushMicrotasks();
      completed.events.clear();
      completed.incoming.close();
      async.flushMicrotasks();
      expect(completed.events, [PhoenixSocketDiagnosticEvent.socketStreamDone]);
      completed.socket.dispose();
      async.flushMicrotasks();
    });
  });

  test('a failing diagnostic observer cannot interrupt heartbeat handling', () {
    fakeAsync((async) {
      final harness = _SocketHarness(
        replyToEveryHeartbeat: true,
        throwFromObserver: true,
      );
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 30));

      expect(harness.socket.isConnected, isTrue);
      expect(harness.decodedMessages, 3);
      expect(
          harness.events.where((event) =>
              event == PhoenixSocketDiagnosticEvent.heartbeatAcknowledged),
          hasLength(3));
      harness.socket.dispose();
      async.flushMicrotasks();
    });
  });
}

class _SocketHarness {
  _SocketHarness({
    bool replyToEveryHeartbeat = false,
    bool throwFromObserver = false,
    Duration heartbeat = const Duration(seconds: 15),
    Duration heartbeatTimeout = const Duration(seconds: 60),
  }) {
    final channel = MockWebSocketChannel();
    final sink = MockWebSocketSink();
    when(channel.stream).thenAnswer((_) => incoming.stream);
    when(channel.sink).thenReturn(sink);
    when(channel.ready).thenAnswer((_) => Future.value());
    when(sink.close(any, any)).thenAnswer((_) => incoming.close());
    var sent = 0;
    when(sink.add(any)).thenAnswer((invocation) {
      final message = Message.fromJson(jsonDecode(
        invocation.positionalArguments.single as String,
      ));
      if (++sent == 1 || replyToEveryHeartbeat) {
        incoming.add(jsonEncode(Message(
          ref: message.ref,
          topic: 'phoenix',
          event: PhoenixChannelEvent.reply,
          payload: {'status': 'ok', 'response': <String, dynamic>{}},
        ).encode()));
      }
    });
    socket = PhoenixSocket(
      'ws://localhost/socket',
      webSocketChannelFactory: (_) => channel,
      socketOptions: PhoenixSocketOptions(
        heartbeat: heartbeat,
        heartbeatTimeout: heartbeatTimeout,
        reconnectDelays: const [Duration(hours: 1)],
        serializer: MessageSerializer(decoder: (raw) {
          decodedMessages += 1;
          return jsonDecode(raw);
        }),
        onDiagnostic: (event) {
          events.add(event);
          if (throwFromObserver) throw StateError('observer failed');
        },
      ),
    );
  }

  final incoming = StreamController<String>();
  final events = <PhoenixSocketDiagnosticEvent>[];
  late final PhoenixSocket socket;
  int decodedMessages = 0;
}
