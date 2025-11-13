// lib/predictions_today.dart
import 'dart:async';
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'supabase_bootstrap.dart';

class PredictionsTodayPage extends StatefulWidget {
  const PredictionsTodayPage({super.key});

  @override
  State<PredictionsTodayPage> createState() => _PredictionsTodayPageState();
}

class _PredictionsTodayPageState extends State<PredictionsTodayPage> {
  final _rows = <Map<String, dynamic>>[];
  RealtimeChannel? _channel;
  late final String _today; // yyyy-MM-dd
  late final String display_window_date;   // yyyy-MM-dd

  @override
  void initState() {
    super.initState();
    _today = DateFormat('yyyy-MM-dd').format(DateTime.now().add(const Duration(days: -1)));
    display_window_date = DateFormat('yyyy-MM-dd').format(DateTime.now().add(const Duration(days: -7)));
    _load();
    _subscribeRealtime();
  }

  @override
  void dispose() {
    _channel?.unsubscribe();
    super.dispose();
  }

final _numFmt2 = NumberFormat("0.00");

num? _toNum(dynamic v) {
  if (v == null) return null;
  if (v is num) return v;
  if (v is String) return num.tryParse(v);
  return null;
}

String _fmt2(dynamic v) {
  final n = _toNum(v);
  if (n == null) return "—";
  return _numFmt2.format(n);
}

  Map<String, dynamic>? _find(String playerId, String target) {
    try {
      return _rows.firstWhere(
        (r) => r['player_id'] == playerId && r['target'] == target,
      );
    } catch (_) {
      return null;
    }
  }

  void _upsert(Map<String, dynamic> row) {
    final playerId = row['player_id'];
    final target = row['target'];
    final idx = _rows.indexWhere(
      (r) => r['player_id'] == playerId && r['target'] == target,
    );
    if (idx >= 0) {
      _rows[idx] = row;
    } else {
      _rows.add(row);
    }
    _rows.sort((a, b) {
      final an = (a['date'] ?? '') as String;
      final bn = (b['date'] ?? '') as String;
      return an.compareTo(bn);
    });
  }

  Future<void> _load() async {
    final supabase = SupabaseBootstrap.client;
    final data = await supabase
        .from('predictions')
        .select()
        .gt('date', display_window_date)
        .order('date', ascending: false)
        .order('elfies_number', ascending: false);
    if (mounted) {
      setState(() {
        _rows
          ..clear()
          ..addAll(List<Map<String, dynamic>>.from(data));
      });
    }
  }

  void _subscribeRealtime() {
    final supabase = SupabaseBootstrap.client;
    _channel = supabase
        .channel('predictions-today-$_today')
        .onPostgresChanges(
          event: PostgresChangeEvent.insert,
          schema: 'public',
          table: 'predictions',
          filter: PostgresChangeFilter(column: 'date', value: _today, type: PostgresChangeFilterType.eq),
          callback: (payload) {
            final newRow = Map<String, dynamic>.from(payload.newRecord!);
            setState(() => _upsert(newRow));
          },
        )
        .onPostgresChanges(
          event: PostgresChangeEvent.update,
          schema: 'public',
          table: 'predictions',
          filter: PostgresChangeFilter(column: 'date', value: _today, type: PostgresChangeFilterType.eq),
          callback: (payload) {
            final newRow = Map<String, dynamic>.from(payload.newRecord!);
            setState(() => _upsert(newRow));
          },
        )
        .onPostgresChanges(
          event: PostgresChangeEvent.delete,
          schema: 'public',
          table: 'predictions',
          filter: PostgresChangeFilter(column: 'date', value: _today, type: PostgresChangeFilterType.eq),
          callback: (payload) {
            final oldRow = Map<String, dynamic>.from(payload.oldRecord!);
            setState(() {
              _rows.removeWhere((r) =>
                  r['player_id'] == oldRow['player_id'] &&
                  r['target'] == oldRow['target']);
            });
          },
        )
        .subscribe();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('WhiteShorts • Today')),
      body: _rows.isEmpty
          ? const Center(child: Text('No predictions yet for today.'))
          : ListView.separated(
              itemCount: _rows.length,
              separatorBuilder: (_, __) => const Divider(height: 1),
              itemBuilder: (context, i) {
                final r = _rows[i];
                final name = (r['name'] ?? '') as String;
                final team = (r['team'] ?? '') as String;
                final opp = (r['opponent'] ?? '') as String;
                final target = (r['target'] ?? '') as String;
                final mean = r['lambda_or_mu'];
                final q10 = r['q10'];
                final q90 = r['q90'];
                final game_date = r['date'];
                final actual_points = r['actual_points'];
                final elfies_number = r['elfies_number'];
                return ListTile(
                  title: Text("$name  •  $target"),
                  subtitle: Text("$team vs $opp  •  $game_date"),
                  trailing: Column(
                    crossAxisAlignment: CrossAxisAlignment.end,
                    mainAxisAlignment: MainAxisAlignment.center,
                    children: [
                      Text("Prediction: ${_fmt2(mean)} --- Actual Result: ${_fmt2(actual_points)}"),
                      Text("Conf. Int: ${_fmt2(q10)}–${_fmt2(q90)}"),
                      Text("elfies_number: ${_fmt2(elfies_number)}"),
                    ],
                  ),
                );
              },
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: _load,
        child: const Icon(Icons.refresh),
      ),
    );
  }
}
