import 'package:flutter/material.dart';
import 'package:intl/intl.dart';

import '../models/broadcast_prediction.dart';
import '../services/prediction_service.dart';
import '../widgets/prediction_card.dart';

class TodayPredictionsPage extends StatefulWidget {
  const TodayPredictionsPage({super.key});

  @override
  State<TodayPredictionsPage> createState() => _TodayPredictionsPageState();
}

class _TodayPredictionsPageState extends State<TodayPredictionsPage> {
  final PredictionService _service = PredictionService();
  late Future<List<BroadcastPrediction>> _future;
  String? _selectedTeam;
  String? _selectedTarget;

  // New: selected date + historical days offset
  late DateTime _selectedDate;
  late int _historicalDays;

  @override
  void initState() {
    super.initState();

    // 'NOTES for historical mode' this should be nowDate - some number of historical days.
    // For now, default to "yesterday" as in your original code (1 day back).
    final now = DateTime.now();
    _selectedDate = DateTime(now.year, now.month, now.day).subtract(const Duration(days: 1));
    _historicalDays = 0;

    _future = _service.fetchToday(_historicalDays);
  }

  void _refreshForSelectedDate() {
    setState(() {
      _future = _service.fetchToday(_historicalDays);
    });
  }

  Future<void> _pickDate() async {
    final now = DateTime.now().subtract(const Duration(days: 1)); //latest (yesterday in U.S)
    final initial = _selectedDate;
    final firstDate = now.subtract(const Duration(days: 30)); // last 30 days
    final lastDate = now;

    final picked = await showDatePicker(
      context: context,
      initialDate: initial.isBefore(firstDate) || initial.isAfter(lastDate)
          ? lastDate
          : initial,
      firstDate: firstDate,
      lastDate: lastDate,
    );

    if (picked == null) return;

    final today = DateTime(now.year, now.month, now.day);
    final pickedDay = DateTime(picked.year, picked.month, picked.day);
    int days = today.subtract(const Duration(days: 1)).difference(pickedDay).inDays;
    if (days < 0) days = 0;

    setState(() {
      _selectedDate = DateTime.now().subtract(const Duration(days: 1));
      _historicalDays = days;
      _future = _service.fetchToday(0);
    });
  }

  Future<void> _handleVote(BroadcastPrediction prediction, int delta) async {
    await _service.updateCrowdScore(prediction, delta);
    setState(() {
      _future = _future.then((list) {
        return list
            .map((p) => p.id == prediction.id
                ? p.copyWith(crowdScore: p.crowdScore + delta)
                : p)
            .toList();
      });
    });
  }

  Future<void> _openFlagsDialog(BroadcastPrediction prediction) async {
    bool gameTotal = prediction.crowdFlagGameTotal;
    bool injury = prediction.crowdFlagInjury;
    bool notPlaying = prediction.notPlaying;

    final result = await showDialog<bool>(
      context: context,
      builder: (ctx) {
        return AlertDialog(
          title: const Text('Edit flags'),
          content: StatefulBuilder(
            builder: (context, setStateDialog) {
              return Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  SwitchListTile(
                    title: const Text('Game total may be too low'),
                    value: gameTotal,
                    onChanged: (val) {
                      setStateDialog(() {
                        gameTotal = val;
                      });
                    },
                  ),
                  SwitchListTile(
                    title: const Text('Injury / role risk'),
                    value: injury,
                    onChanged: (val) {
                      setStateDialog(() {
                        injury = val;
                      });
                    },
                  ),
                  SwitchListTile(
                    title: const Text('Player not playing'),
                    value: notPlaying,
                    onChanged: (val) {
                      setStateDialog(() {
                        notPlaying = val;
                      });
                    },
                  )
                ],
              );
            },
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(ctx).pop(false),
              child: const Text('Cancel'),
            ),
            ElevatedButton(
              onPressed: () => Navigator.of(ctx).pop(true),
              child: const Text('Save'),
            ),
          ],
        );
      },
    );

    if (result == true) {
      await _service.updateFlags(
        prediction,
        gameTotal: gameTotal,
        injury: injury,
        notPlaying: notPlaying,
      );

      // Re-fetch for the currently selected historical date
      setState(() {
        _future = _service.fetchToday(_historicalDays);
      });
    }
  }

  List<BroadcastPrediction> _applyFilters(
      List<BroadcastPrediction> predictions) {
    return predictions.where((p) {
      final teamOk =
          _selectedTeam == null || (_selectedTeam != null && p.team == _selectedTeam);
      final targetOk = _selectedTarget == null ||
          (_selectedTarget != null && p.target == _selectedTarget);
      return teamOk && targetOk;
    }).toList();
  }

  Widget _buildFilters(List<BroadcastPrediction> predictions) {
    final cs = Theme.of(context).colorScheme;

    final teams = <String>{
      for (final p in predictions)
        if (p.team != null && p.team!.isNotEmpty) p.team!,
    }.toList()
      ..sort();

    final targets = <String>{
      for (final p in predictions)
        if (p.target != null && p.target!.isNotEmpty) p.target!,
    }.toList()
      ..sort();

    final dateLabel = DateFormat('yyyy-MM-dd').format(_selectedDate);

    return Card(
      color: cs.surfaceContainerHighest,
      margin: const EdgeInsets.symmetric(horizontal: 4, vertical: 4),
      child: Padding(
        padding: const EdgeInsets.all(8),
        child: Column(
          children: [
            Row(
              children: [
                Expanded(
                  child: DropdownButton<String>(
                    isExpanded: true,
                    value: _selectedTeam,
                    hint: const Text('All teams'),
                    items: [
                      const DropdownMenuItem<String>(
                        value: null,
                        child: Text('All teams'),
                      ),
                      ...teams.map(
                        (t) => DropdownMenuItem<String>(
                          value: t,
                          child: Text(t),
                        ),
                      ),
                    ],
                    onChanged: (value) {
                      setState(() {
                        _selectedTeam = value;
                      });
                    },
                  ),
                ),
                const SizedBox(width: 8),
                Expanded(
                  child: DropdownButton<String>(
                    isExpanded: true,
                    value: _selectedTarget,
                    hint: const Text('All targets'),
                    items: [
                      const DropdownMenuItem<String>(
                        value: null,
                        child: Text('All targets'),
                      ),
                      ...targets.map(
                        (t) => DropdownMenuItem<String>(
                          value: t,
                          child: Text(t),
                        ),
                      ),
                    ],
                    onChanged: (value) {
                      setState(() {
                        _selectedTarget = value;
                      });
                    },
                  ),
                ),
              ],
            ),
            const SizedBox(height: 8),
            Align(
              alignment: Alignment.centerLeft,
              child: TextButton.icon(
                onPressed: _pickDate,
                icon: const Icon(Icons.calendar_today),
                label: Text('Date: $dateLabel'),
              ),
            ),
          ],
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Todays's preds/results"),
      ),
      body: FutureBuilder<List<BroadcastPrediction>>(
        future: _future,
        builder: (context, snapshot) {
          if (snapshot.connectionState == ConnectionState.waiting) {
            return const Center(child: CircularProgressIndicator());
          }
          if (snapshot.hasError) {
            return Center(
              child: Text(
                'Error loading predictions:\n${snapshot.error}',
                textAlign: TextAlign.center,
              ),
            );
          }

          final predictions = snapshot.data ?? [];

          if (predictions.isEmpty) {
            return const Center(
              child: Text('Still waiting on Teams...'),
            );
          }

          final filtered = _applyFilters(predictions);

          return Column(
            children: [
              _buildFilters(predictions),
              Expanded(
                child: ListView.builder(
                  padding: const EdgeInsets.all(8),
                  itemCount: filtered.length,
                  itemBuilder: (context, index) {
                    final p = filtered[index];
                    return PredictionCard(
                      prediction: p,
                      enableVoting: true,
                      onVote: _handleVote,
                      canEditFlags: true,
                      onEditFlags: () => _openFlagsDialog(p),
                    );
                  },
                ),
              ),
            ],
          );
        },
      ),
    );
  }
}
