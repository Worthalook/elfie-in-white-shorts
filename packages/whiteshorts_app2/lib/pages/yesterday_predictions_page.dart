import 'package:flutter/material.dart';

import '../models/broadcast_prediction.dart';
import '../services/prediction_service.dart';
import '../widgets/prediction_card.dart';

class YesterdayPredictionsPage extends StatefulWidget {
  const YesterdayPredictionsPage({super.key});

  @override
  State<YesterdayPredictionsPage> createState() =>
      _YesterdayPredictionsPageState();
}

class _YesterdayPredictionsPageState extends State<YesterdayPredictionsPage> {
  final PredictionService _service = PredictionService();
  late Future<List<BroadcastPrediction>> _future;
  String? _selectedTeam;
  String? _selectedTarget;

  @override
  void initState() {
    super.initState();
    _future = _service.fetchYesterday();
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

    return Card(
      color: cs.surfaceVariant,
      margin: const EdgeInsets.symmetric(horizontal: 4, vertical: 4),
      child: Padding(
        padding: const EdgeInsets.all(8),
        child: Row(
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
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Yesterday's preds/results"),
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
              child: Text('No predictions for yesterday.'),
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
                      enableVoting: false,
                      onVote: (_, __) async {},
                      canEditFlags: false,
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
