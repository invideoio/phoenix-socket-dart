part of 'socket.dart';

// The envelope retains the existing raw reference, never a copied payload. It
// keeps queued/late messages tied to their transport's diagnostic generation.
class _DiagnosticSocketMessage {
  _DiagnosticSocketMessage(this.raw, this.diagnostics);

  final dynamic raw;
  final _SocketReceiveDiagnostics diagnostics;
}

class _MutableReceiveCounters {
  int textMessages = 0;
  int textCodeUnits = 0;
  int binaryMessages = 0;
  int binaryBytes = 0;
  int decodeCount = 0;
  int decodeErrors = 0;
  int decodeTotalMicroseconds = 0;
  int decodeMaxMicroseconds = 0;
  int heartbeatRepliesMatched = 0;
  int heartbeatRepliesUnmatched = 0;
  int heartbeatRepliesNoPending = 0;

  void received(dynamic raw) {
    if (raw is String) {
      textMessages++;
      textCodeUnits += raw.length;
    } else if (raw is Uint8List) {
      binaryMessages++;
      binaryBytes += raw.lengthInBytes;
    }
  }

  void decoded(int elapsedMicroseconds, bool failed) {
    decodeCount++;
    if (failed) decodeErrors++;
    decodeTotalMicroseconds += elapsedMicroseconds;
    decodeMaxMicroseconds = max(decodeMaxMicroseconds, elapsedMicroseconds);
  }

  void heartbeatReply(String? ref, String? pendingRef) {
    if (pendingRef == null) {
      heartbeatRepliesNoPending++;
    } else if (ref != null && ref == pendingRef) {
      heartbeatRepliesMatched++;
    } else {
      heartbeatRepliesUnmatched++;
    }
  }

  PhoenixSocketReceiveCounters snapshot() => PhoenixSocketReceiveCounters(
        textMessages: textMessages,
        textCodeUnits: textCodeUnits,
        binaryMessages: binaryMessages,
        binaryBytes: binaryBytes,
        decodeCount: decodeCount,
        decodeErrors: decodeErrors,
        decodeTotalMicroseconds: decodeTotalMicroseconds,
        decodeMaxMicroseconds: decodeMaxMicroseconds,
        heartbeatRepliesMatched: heartbeatRepliesMatched,
        heartbeatRepliesUnmatched: heartbeatRepliesUnmatched,
        heartbeatRepliesNoPending: heartbeatRepliesNoPending,
      );
}

class _SocketReceiveDiagnostics {
  _SocketReceiveDiagnostics(this.generation);

  final int generation;
  final Stopwatch clock = Stopwatch()..start();
  final _MutableReceiveCounters totals = _MutableReceiveCounters();
  _MutableReceiveCounters sinceSend = _MutableReceiveCounters();
  int _sentAtMicroseconds = 0;
  int? _lastReceiveMicroseconds;

  void received(dynamic raw) {
    _lastReceiveMicroseconds = clock.elapsedMicroseconds;
    totals.received(raw);
    sinceSend.received(raw);
  }

  void decoded(int elapsedMicroseconds, bool failed) {
    totals.decoded(elapsedMicroseconds, failed);
    sinceSend.decoded(elapsedMicroseconds, failed);
  }

  void heartbeatReply(Message message, String? pendingRef) {
    if (message.topic != 'phoenix' ||
        message.event != PhoenixChannelEvent.reply) {
      return;
    }
    totals.heartbeatReply(message.ref, pendingRef);
    sinceSend.heartbeatReply(message.ref, pendingRef);
  }

  PhoenixHeartbeatDiagnosticSnapshot snapshot(
      PhoenixSocketDiagnosticEvent event) {
    final now = clock.elapsedMicroseconds;
    if (event == PhoenixSocketDiagnosticEvent.heartbeatSent) {
      _sentAtMicroseconds = now;
      sinceSend = _MutableReceiveCounters();
    }
    final received = _lastReceiveMicroseconds;
    return PhoenixHeartbeatDiagnosticSnapshot(
      event: event,
      connectionGeneration: generation,
      generationElapsedMilliseconds: now ~/ 1000,
      heartbeatAgeMilliseconds: (now - _sentAtMicroseconds) ~/ 1000,
      lastReceiveAgeMilliseconds:
          received == null ? null : (now - received) ~/ 1000,
      generationTotals: totals.snapshot(),
      sinceHeartbeatSent: sinceSend.snapshot(),
    );
  }
}
