import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:mockito/mockito.dart';
import 'package:phoenix_socket/phoenix_socket.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'mocks.dart';

void main() {
  // Catches duplicate sends, periodic deadline extension, and early close.
  test('grace keeps one request pending and closes at its original timeout',
      () {
    fakeAsync((async) {
      final harness = _GraceHarness();
      harness.socket.connect();
      async.flushMicrotasks();
      final connection = harness.connections.single;
      async.elapse(const Duration(seconds: 15));
      expect(connection.sent, hasLength(2)); // Initial handshake and one probe.
      for (var i = 0; i < 3; i++) {
        async.elapse(const Duration(seconds: 15));
        expect(harness.socket.isConnected, isTrue);
        expect(connection.sent, hasLength(2));
      }
      async.elapse(const Duration(seconds: 14, milliseconds: 999));
      expect(harness.socket.isConnected, isTrue);
      async.elapse(const Duration(milliseconds: 1));
      expect(harness.socket.isConnected, isFalse);
      expect(
          harness.events.where((event) =>
              event == PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout),
          hasLength(1));
      expect(harness.events,
          isNot(contains(PhoenixSocketDiagnosticEvent.heartbeatClosePending)));
      expect(connection.heartbeatTimeoutCloses, 1);
      final snapshot = harness.snapshots.last;
      expect(
          snapshot.event, PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout);
      expect(snapshot.sinceHeartbeatSent.textMessages, 0);
      expect(snapshot.sinceHeartbeatSent.decodeCount, 0);
      expect(snapshot.sinceHeartbeatSent.heartbeatRepliesMatched, 0);
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  // Catches lost pending refs and a deadline firing after an accepted late reply.
  for (final replyDelay in [20, 59]) {
    test('a matching reply after ${replyDelay}s preserves the connection', () {
      fakeAsync((async) {
        final harness = _GraceHarness();
        harness.socket.connect();
        async.flushMicrotasks();
        final connection = harness.connections.single;
        async.elapse(const Duration(seconds: 15));
        final pending = connection.sent.last;
        async.elapse(Duration(seconds: replyDelay));
        connection.replyToEveryHeartbeat = true;
        connection.reply(pending.ref);
        async.flushMicrotasks();
        expect(harness.socket.isConnected, isTrue);
        // The original 15-second timer resumes sending at its next idle tick.
        final untilNextTick = 15 - replyDelay % 15;
        async.elapse(Duration(seconds: untilNextTick));
        expect(connection.sent, hasLength(3));
        async.elapse(const Duration(seconds: 60));
        expect(harness.socket.isConnected, isTrue);
        expect(connection.heartbeatTimeoutCloses, 0);
        expect(
            harness.events,
            isNot(
                contains(PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout)));
        harness.dispose();
        async.flushMicrotasks();
      });
    });
  }

  // Catches treating arbitrary traffic as an acknowledgement/deadline refresh.
  test('wrong refs and ordinary messages do not extend the heartbeat deadline',
      () {
    fakeAsync((async) {
      final harness = _GraceHarness();
      harness.socket.connect();
      async.flushMicrotasks();
      final connection = harness.connections.single;
      async.elapse(const Duration(seconds: 15));
      for (var i = 0; i < 3; i++) {
        async.elapse(const Duration(seconds: 15));
        connection.reply('wrong');
        connection.incoming.add(jsonEncode(Message(
          topic: 'room:test',
          event: PhoenixChannelEvent.custom('update'),
          payload: const {},
        ).encode()));
        async.flushMicrotasks();
      }
      async.elapse(const Duration(seconds: 15));
      expect(harness.socket.isConnected, isFalse);
      expect(connection.sent, hasLength(2));
      final snapshot = harness.snapshots.last;
      expect(
          snapshot.event, PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout);
      expect(snapshot.sinceHeartbeatSent.textMessages, 6);
      expect(snapshot.sinceHeartbeatSent.heartbeatRepliesUnmatched, 3);
      expect(snapshot.sinceHeartbeatSent.heartbeatRepliesMatched, 0);
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  // Catches accidentally waiting for a periodic tick when the timeout is shorter.
  test('grace honors a timeout shorter than the periodic interval', () {
    fakeAsync((async) {
      final harness = _GraceHarness(timeout: const Duration(seconds: 5));
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 19, milliseconds: 999));
      expect(harness.socket.isConnected, isTrue);
      async.elapse(const Duration(milliseconds: 1));
      expect(harness.socket.isConnected, isFalse);
      expect(harness.snapshots.last.event,
          PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout);
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  // Catches extending peer/error handling or suppressing normal reconnects.
  for (final peerError in [false, true]) {
    test(
        'grace preserves immediate ${peerError ? 'error' : 'peer close'} and reconnect',
        () {
      fakeAsync((async) {
        final harness =
            _GraceHarness(reconnectDelay: const Duration(seconds: 5));
        harness.socket.connect();
        async.flushMicrotasks();
        async.elapse(const Duration(seconds: 15));
        final old = harness.connections.single;
        if (peerError) {
          old.incoming.addError(WebSocketChannelException('transport failed'));
        } else {
          old.incoming.close();
        }
        async.flushMicrotasks();
        expect(harness.socket.isConnected, isFalse);
        async.elapse(const Duration(seconds: 6)); // Existing 5s + <1s jitter.
        expect(harness.connections, hasLength(2));
        expect(harness.socket.isConnected, isTrue);
        harness.dispose();
        async.flushMicrotasks();
      });
    });
  }

  // Catches an old in-flight timeout closing a newly connected transport.
  test('a pending timeout cannot close a replacement transport', () {
    fakeAsync((async) {
      final harness = _GraceHarness(closeIncomingOnSinkClose: false);
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 15));
      final old = harness.connections.single;
      final pending = old.sent.last;
      harness.socket.close(null, null, true);
      harness.socket.connect();
      async.flushMicrotasks();
      expect(harness.connections, hasLength(2));
      final current = harness.connections.last;
      current.replyToEveryHeartbeat = true;
      async.elapse(const Duration(seconds: 61));
      expect(harness.socket.isConnected, isTrue);
      expect(old.heartbeatTimeoutCloses, 0);
      expect(current.heartbeatTimeoutCloses, 0);
      expect(harness.events,
          isNot(contains(PhoenixSocketDiagnosticEvent.heartbeatCloseTimeout)));
      Object? lookupError;
      harness.socket.waitForMessage(pending).then<void>(
          (_) => fail('expired heartbeat must no longer be registered'),
          onError: (Object error) {
        lookupError = error;
      });
      async.flushMicrotasks();
      expect(lookupError, isA<ArgumentError>());
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  // Catches a pending future causing post-dispose traffic, reconnect or timeout.
  test('dispose cancels periodic work and leaves no late timeout close', () {
    fakeAsync((async) {
      final harness = _GraceHarness(closeIncomingOnSinkClose: false);
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 15));
      final connection = harness.connections.single;
      harness.socket.dispose();
      final events = harness.events.length;
      async.elapse(const Duration(seconds: 90));
      expect(harness.connections, hasLength(1));
      expect(connection.sent, hasLength(2));
      expect(connection.heartbeatTimeoutCloses, 0);
      expect(harness.events, hasLength(events));
      harness.dispose();
      async.flushMicrotasks();
    });
  });
}

class _GraceHarness {
  _GraceHarness({
    Duration timeout = const Duration(seconds: 60),
    Duration reconnectDelay = const Duration(hours: 1),
    bool closeIncomingOnSinkClose = true,
  }) {
    socket = PhoenixSocket(
      'ws://localhost/socket',
      webSocketChannelFactory: (_) {
        final connection = _Connection(closeIncomingOnSinkClose);
        connections.add(connection);
        return connection.channel;
      },
      socketOptions: PhoenixSocketOptions(
        heartbeat: const Duration(seconds: 15),
        heartbeatTimeout: timeout,
        waitForHeartbeatTimeout: true,
        reconnectDelays: [reconnectDelay],
        onDiagnostic: events.add,
        onHeartbeatDiagnostic: snapshots.add,
      ),
    );
  }

  late final PhoenixSocket socket;
  final connections = <_Connection>[];
  final events = <PhoenixSocketDiagnosticEvent>[];
  final snapshots = <PhoenixHeartbeatDiagnosticSnapshot>[];

  void dispose() {
    socket.dispose();
    for (final connection in connections) {
      connection.incoming.close();
    }
  }
}

class _Connection {
  _Connection(bool closeIncomingOnSinkClose) {
    when(channel.stream).thenAnswer((_) => incoming.stream);
    when(channel.sink).thenReturn(sink);
    when(channel.ready).thenAnswer((_) => Future.value());
    when(sink.close(any, any)).thenAnswer((invocation) {
      if (invocation.positionalArguments[1] == 'heartbeat timeout') {
        heartbeatTimeoutCloses++;
      }
      return closeIncomingOnSinkClose ? incoming.close() : Future.value();
    });
    when(sink.add(any)).thenAnswer((invocation) {
      final message = Message.fromJson(
          jsonDecode(invocation.positionalArguments.single as String));
      sent.add(message);
      if (sent.length == 1 || replyToEveryHeartbeat) reply(message.ref);
    });
  }

  void reply(String? ref) => incoming.add(jsonEncode(Message(
        ref: ref,
        topic: 'phoenix',
        event: PhoenixChannelEvent.reply,
        payload: const {'status': 'ok', 'response': <String, dynamic>{}},
      ).encode()));

  final incoming = StreamController<dynamic>();
  final channel = MockWebSocketChannel();
  final sink = MockWebSocketSink();
  final sent = <Message>[];
  bool replyToEveryHeartbeat = false;
  int heartbeatTimeoutCloses = 0;
}
