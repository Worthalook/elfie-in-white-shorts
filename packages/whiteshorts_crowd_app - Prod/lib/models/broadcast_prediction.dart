class BroadcastPrediction {
  final int id;
  final DateTime date;
  final String? gameId;
  final String? team;
  final String? opponent;
  final String? playerId;
  final String? name;
  final String? target;
  final String? modelName;
  final String? modelVersion;
  final String? distribution;
  final double? lambdaOrMu;
  final double? q10;
  final double? q90;
  final Map<String, dynamic>? pGeK;
  final String? runId;
  final DateTime? createdTs;
  final double? elfiesNumber;
  final double? actualPoints;
  final int crowdScore;

  BroadcastPrediction({
    required this.id,
    required this.date,
    this.gameId,
    this.team,
    this.opponent,
    this.playerId,
    this.name,
    this.target,
    this.modelName,
    this.modelVersion,
    this.distribution,
    this.lambdaOrMu,
    this.q10,
    this.q90,
    this.pGeK,
    this.runId,
    this.createdTs,
    this.elfiesNumber,
    this.actualPoints,
    required this.crowdScore,
  });

  factory BroadcastPrediction.fromJson(Map<String, dynamic> json) {
    return BroadcastPrediction(
      id: json['id'] as int,
      date: DateTime.parse(json['date'] as String),
      gameId: json['game_id'] as String?,
      team: json['team'] as String?,
      opponent: json['opponent'] as String?,
      playerId: json['player_id'] as String?,
      name: json['name'] as String?,
      target: json['target'] as String?,
      modelName: json['model_name'] as String?,
      modelVersion: json['model_version'] as String?,
      distribution: json['distribution'] as String?,
      lambdaOrMu: (json['lambda_or_mu'] as num?)?.toDouble(),
      q10: (json['q10'] as num?)?.toDouble(),
      q90: (json['q90'] as num?)?.toDouble(),
      pGeK: json['p_ge_k_json'] as Map<String, dynamic>?,
      runId: json['run_id'] as String?,
      createdTs: json['created_ts'] != null
          ? DateTime.parse(json['created_ts'] as String)
          : null,
      elfiesNumber: (json['elfies_number'] as num?)?.toDouble(),
      actualPoints: (json['actual_points'] as num?)?.toDouble(),
      crowdScore: (json['crowd_score'] ?? 0) as int,
    );
  }

  BroadcastPrediction copyWith({
    int? crowdScore,
  }) {
    return BroadcastPrediction(
      id: id,
      date: date,
      gameId: gameId,
      team: team,
      opponent: opponent,
      playerId: playerId,
      name: name,
      target: target,
      modelName: modelName,
      modelVersion: modelVersion,
      distribution: distribution,
      lambdaOrMu: lambdaOrMu,
      q10: q10,
      q90: q90,
      pGeK: pGeK,
      runId: runId,
      createdTs: createdTs,
      elfiesNumber: elfiesNumber,
      actualPoints: actualPoints,
      crowdScore: crowdScore ?? this.crowdScore,
    );
  }
}
