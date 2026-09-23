import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:fake_async/fake_async.dart';
import 'package:mockito/mockito.dart';
import 'package:phoenix_socket/phoenix_socket.dart';
import 'package:test/test.dart';

import 'mocks.dart';

void main() {
  test('public consumers keep values and do not multiply primary counters', () {
    fakeAsync((async) {
      final harness = _ReceiveHarness();
      harness.socket.connect();
      async.flushMicrotasks();
      final first = harness.snapshots.single;
      final publicMessages = <Message>[];
      harness.socket.messageStream.listen(publicMessages.add);

      final text = _reply('unmatched', payload: {'value': '🙂'});
      // Incoming Phoenix broadcasts use a different header from client pushes.
      final bytes = Uint8List.fromList([
        2, 3, 4, // broadcast kind, topic length, event length
        ...utf8.encode('top'),
        ...utf8.encode('test'),
        1, 2, 3,
      ]);
      harness.connections.single.incoming.add(text);
      harness.connections.single.incoming.add(bytes);
      async.flushMicrotasks();

      expect(publicMessages, hasLength(2));
      expect(publicMessages.first.payload, {'value': '🙂'});
      expect(publicMessages.last.payload, [1, 2, 3]);
      final binaryInputs = harness.serializer.inputs.whereType<Uint8List>();
      expect(binaryInputs, hasLength(2));
      expect(binaryInputs.every((input) => identical(input, bytes)), isTrue);
      expect(
          harness.socket.receivedInputs.whereType<Uint8List>(), hasLength(1));
      expect(identical(harness.socket.receivedInputs.last, bytes), isTrue);

      async.elapse(const Duration(seconds: 15));
      final sent = harness.snapshots.last;
      expect(sent.event, PhoenixSocketDiagnosticEvent.heartbeatSent);
      expect(sent.generationTotals.decodeCount, 3);
      expect(sent.generationTotals.textMessages, 2);
      expect(sent.generationTotals.textCodeUnits,
          harness.connections.single.initialReply.length + text.length);
      expect(sent.generationTotals.binaryMessages, 1);
      expect(sent.generationTotals.binaryBytes, bytes.length);
      expect(sent.sinceHeartbeatSent.decodeCount, 0);
      expect(sent.sinceHeartbeatSent.decodeMaxMicroseconds, 0);
      expect(first.generationTotals.decodeCount, 0);
      expect(first.lastReceiveAgeMilliseconds, isNull);
      // One initial primary decode plus three subsequent messages for both
      // existing consumers. Instrumentation has introduced no third consumer.
      expect(harness.serializer.inputs, hasLength(7));
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  test('primary serializer errors keep propagating and are counted once', () {
    fakeAsync((async) {
      final errors = <Object>[];
      runZonedGuarded(() {
        final harness = _ReceiveHarness();
        harness.socket.connect();
        async.flushMicrotasks();
        harness.connections.single.incoming.add('not valid json');
        async.flushMicrotasks();
        async.elapse(const Duration(seconds: 15));

        final totals = harness.snapshots.last.generationTotals;
        expect(totals.decodeCount, 2);
        expect(totals.decodeErrors, 1);
        expect(totals.decodeTotalMicroseconds,
            greaterThanOrEqualTo(totals.decodeMaxMicroseconds));
        expect(harness.socket.isConnected, isTrue);
        harness.dispose();
        async.flushMicrotasks();
      }, (error, _) => errors.add(error));
      expect(errors, hasLength(1));
      expect(errors.single, isA<FormatException>());
    });
  });

  test('heartbeat classifications use only the fixed reply route', () {
    fakeAsync((async) {
      final harness = _ReceiveHarness();
      harness.socket.connect();
      async.flushMicrotasks();
      final connection = harness.connections.single;
      connection.incoming.add(_reply('late'));
      connection.incoming.add(_reply(null));
      connection.incoming.add(_reply('ignored', topic: 'another-topic'));
      async.flushMicrotasks();
      connection.replyToHeartbeats = false;
      async.elapse(const Duration(seconds: 15));

      final sent = harness.snapshots.last;
      expect(sent.generationTotals.heartbeatRepliesMatched, 1);
      expect(sent.generationTotals.heartbeatRepliesNoPending, 2);
      expect(sent.generationTotals.heartbeatRepliesUnmatched, 0);
      expect(sent.sinceHeartbeatSent.textMessages, 0);

      connection.incoming.add(_reply('wrong-reference'));
      connection.incoming.add(_reply(null));
      connection.incoming.add(_reply('ignored', topic: 'another-topic'));
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 15));

      final close = harness.snapshots.last;
      expect(close.event, PhoenixSocketDiagnosticEvent.heartbeatClosePending);
      expect(close.sinceHeartbeatSent.textMessages, 3);
      expect(close.sinceHeartbeatSent.decodeCount, 3);
      expect(close.sinceHeartbeatSent.heartbeatRepliesUnmatched, 2);
      expect(close.sinceHeartbeatSent.heartbeatRepliesMatched, 0);
      expect(close.sinceHeartbeatSent.heartbeatRepliesNoPending, 0);
      expect(close.lastReceiveAgeMilliseconds, isNotNull);
      expect(harness.socket.isConnected, isFalse);
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  test('queued and late old-transport messages cannot contaminate a reconnect',
      () {
    fakeAsync((async) {
      final harness = _ReceiveHarness(closeIncomingOnSinkClose: false);
      harness.socket.connect();
      async.flushMicrotasks();
      final publicMessages = <Message>[];
      final publicSubscription =
          harness.socket.messageStream.listen(publicMessages.add)..pause();
      final oldConnection = harness.connections.single;

      // Queue a complete message before starting the next connection attempt.
      harness.socket.onSocketDataCallback(_reply('queued-old'));
      harness.socket.close(null, null, true);
      harness.socket.connect();
      async.flushMicrotasks();
      expect(harness.connections, hasLength(2));

      // The old transport can still deliver a callback. Its subscription must
      // retain the old accumulator even though a new generation is active.
      oldConnection.incoming.add(_reply('late-old'));
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 15));

      final current = harness.snapshots.last;
      expect(current.connectionGeneration, 2);
      expect(current.generationTotals.textMessages, 1);
      expect(current.generationTotals.decodeCount, 1);
      expect(current.generationTotals.heartbeatRepliesMatched, 1);
      expect(current.generationTotals.heartbeatRepliesNoPending, 0);
      publicSubscription.resume();
      async.flushMicrotasks();
      expect(
          publicMessages.where((message) =>
              message.ref == 'queued-old' || message.ref == 'late-old'),
          hasLength(2));
      async.elapse(const Duration(seconds: 15));
      expect(harness.snapshots.last.generationTotals.decodeCount, 2);
      expect(current.generationTotals.decodeCount, 1);
      harness.dispose();
      async.flushMicrotasks();
    });
  });

  test('both observers can throw without suppressing snapshots or replies', () {
    fakeAsync((async) {
      final harness = _ReceiveHarness(throwFromObservers: true);
      harness.socket.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 15));
      expect(harness.snapshots, hasLength(2));
      expect(
          harness.snapshots.last.generationTotals.heartbeatRepliesMatched, 1);
      expect(harness.socket.isConnected, isTrue);
      harness.dispose();
      async.flushMicrotasks();
    });
  });
}

String _reply(String? ref,
        {String topic = 'phoenix', dynamic payload = const {}}) =>
    jsonEncode(Message(
      ref: ref,
      topic: topic,
      event: PhoenixChannelEvent.reply,
      payload: payload,
    ).encode());

class _TrackingSerializer extends MessageSerializer {
  final inputs = <dynamic>[];

  @override
  Message decode(dynamic rawData) {
    inputs.add(rawData);
    return super.decode(rawData);
  }
}

class _RecordingSocket extends PhoenixSocket {
  _RecordingSocket(super.endpoint,
      {super.socketOptions, super.webSocketChannelFactory});

  final receivedInputs = <dynamic>[];

  @override
  dynamic onSocketDataCallback(dynamic message) {
    receivedInputs.add(message);
    return super.onSocketDataCallback(message);
  }
}

class _ReceiveHarness {
  _ReceiveHarness({
    bool closeIncomingOnSinkClose = true,
    bool throwFromObservers = false,
  }) {
    socket = _RecordingSocket(
      'ws://localhost/socket',
      webSocketChannelFactory: (_) {
        final connection = _TestConnection(closeIncomingOnSinkClose);
        connections.add(connection);
        return connection.channel;
      },
      socketOptions: PhoenixSocketOptions(
        serializer: serializer,
        heartbeat: const Duration(seconds: 15),
        heartbeatTimeout: const Duration(seconds: 60),
        reconnectDelays: const [Duration(hours: 1)],
        onDiagnostic: (_) {
          if (throwFromObservers) throw StateError('enum observer failure');
        },
        onHeartbeatDiagnostic: (snapshot) {
          snapshots.add(snapshot);
          if (throwFromObservers) throw StateError('scalar observer failure');
        },
      ),
    );
  }

  final serializer = _TrackingSerializer();
  final snapshots = <PhoenixHeartbeatDiagnosticSnapshot>[];
  final connections = <_TestConnection>[];
  late final _RecordingSocket socket;

  void dispose() {
    socket.dispose();
    for (final connection in connections) {
      connection.incoming.close();
    }
  }
}

class _TestConnection {
  _TestConnection(bool closeIncomingOnSinkClose) {
    when(channel.stream).thenAnswer((_) => incoming.stream);
    when(channel.sink).thenReturn(sink);
    when(channel.ready).thenAnswer((_) => Future.value());
    when(sink.close(any, any)).thenAnswer(
        (_) => closeIncomingOnSinkClose ? incoming.close() : Future.value());
    when(sink.add(any)).thenAnswer((invocation) {
      final message = Message.fromJson(
          jsonDecode(invocation.positionalArguments.single as String));
      if (replyToHeartbeats) {
        final reply = _reply(message.ref);
        initialReply = initialReply.isEmpty ? reply : initialReply;
        incoming.add(reply);
      }
    });
  }

  final incoming = StreamController<dynamic>();
  final channel = MockWebSocketChannel();
  final sink = MockWebSocketSink();
  bool replyToHeartbeats = true;
  String initialReply = '';
}
