import 'package:flutter/material.dart';

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

  @override
  void initState() {
    super.initState();
    _future = _service.fetchToday();
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

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text("Today's preds/results"),
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
              child: Text('No predictions for today.'),
            );
          }

          return ListView.builder(
            padding: const EdgeInsets.all(8),
            itemCount: predictions.length,
            itemBuilder: (context, index) {
              final p = predictions[index];
              return PredictionCard(
                prediction: p,
                enableVoting: true,
                onVote: _handleVote,
              );
            },
          );
        },
      ),
    );
  }
}
