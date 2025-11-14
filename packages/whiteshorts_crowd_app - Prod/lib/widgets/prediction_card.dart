import 'package:flutter/material.dart';

import '../models/broadcast_prediction.dart';

typedef VoteCallback = Future<void> Function(
  BroadcastPrediction prediction,
  int delta,
);

class PredictionCard extends StatelessWidget {
  final BroadcastPrediction prediction;
  final bool enableVoting;
  final VoteCallback onVote;

  const PredictionCard({
    super.key,
    required this.prediction,
    required this.enableVoting,
    required this.onVote,
  });

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;

    final actual = prediction.actualPoints;
    final bool hasActual = actual != null;
    final bool isPositive = hasActual && actual! > 0;

    final actualStyle = TextStyle(
      fontSize: 16,
      fontWeight: FontWeight.bold,
      color: isPositive ? Colors.greenAccent : cs.onSurfaceVariant,
    );

    return Card(
      margin: const EdgeInsets.symmetric(vertical: 6, horizontal: 4),
      shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(16)),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Row(
          children: [
            // Main info
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    prediction.name ?? 'Unknown player',
                    style: const TextStyle(
                      fontSize: 18,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'Team: ${prediction.team ?? "-"} vs ${prediction.opponent ?? "-"}',
                    style: TextStyle(
                      color: cs.onSurfaceVariant,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'Target: ${prediction.target ?? "-"}',
                    style: TextStyle(
                      color: cs.onSurfaceVariant,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'λ/μ: ${prediction.lambdaOrMu?.toStringAsFixed(2) ?? "-"}',
                    style: TextStyle(
                      color: cs.onSurfaceVariant,
                      fontSize: 13,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'q10–q90: '
                    '${prediction.q10?.toStringAsFixed(2) ?? "-"}'
                    ' → '
                    '${prediction.q90?.toStringAsFixed(2) ?? "-"}',
                    style: TextStyle(
                      color: cs.onSurfaceVariant,
                      fontSize: 13,
                    ),
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'Actual: ${hasActual ? actual!.toStringAsFixed(1) : "-"}',
                    style: actualStyle,
                  ),
                  const SizedBox(height: 4),
                  Text(
                    'Crowd score: ${prediction.crowdScore}',
                    style: TextStyle(
                      fontSize: 13,
                      color: cs.onSurfaceVariant,
                    ),
                  ),
                ],
              ),
            ),

            if (enableVoting) ...[
              const SizedBox(width: 8),
              Column(
                children: [
                  IconButton(
                    icon: const Icon(Icons.thumb_up_alt_outlined),
                    onPressed: () => onVote(prediction, +1),
                    tooltip: 'Up-vote',
                  ),
                  IconButton(
                    icon: const Icon(Icons.thumb_down_alt_outlined),
                    onPressed: () => onVote(prediction, -1),
                    tooltip: 'Down-vote',
                  ),
                ],
              ),
            ],
          ],
        ),
      ),
    );
  }
}
