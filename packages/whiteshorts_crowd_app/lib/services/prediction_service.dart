import 'package:intl/intl.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import '../supabase_client.dart';
import '../models/broadcast_prediction.dart';

class PredictionService {
  final SupabaseClient _client;

  PredictionService() : _client = AppSupabase.client;

  Future<List<BroadcastPrediction>> fetchByDate(DateTime date) async {
    final dateStr = DateFormat('yyyy-MM-dd').format(date);

    final response = await _client
        .from('predictions_for_broadcast')
        .select()
        .eq('date', dateStr)
        .order('team', ascending: false)
        .order('elfies_number', ascending: false)
        .order('flag_not_playing',ascending: true);

    final list = (response as List)
        .map((row) => BroadcastPrediction.fromJson(row as Map<String, dynamic>))
        .toList();

    return list;
  }

//today is -1 (U.S time)
  Future<List<BroadcastPrediction>> fetchToday() {
    return fetchByDate(DateTime.now().subtract(const Duration(days: 1)));
  }
  

  Future<List<BroadcastPrediction>> fetchYesterday() {
    return fetchByDate(DateTime.now().subtract(const Duration(days: 2)));
  }

  Future<void> updateCrowdScore(BroadcastPrediction prediction, int delta) async {
    final newScore = prediction.crowdScore + delta;

    await _client
        .from('predictions_for_broadcast')
        .update({'crowd_score': newScore})
        .eq('id', prediction.id);
  }

  Future<void> updateFlags(
    BroadcastPrediction prediction, {
    required bool gameTotal,
    required bool injury,
    required bool notPlaying,
  }) async {
    await _client
        .from('predictions_for_broadcast')
        .update({
          'crowd_flag_game_total': gameTotal,
          'crowd_flag_injury': injury,
          'flag_not_playing': notPlaying,
        })
        .eq('id', prediction.id);
  }
}
