import 'package:intl/intl.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

import '../supabase_client.dart';
import '../models/broadcast_prediction.dart';

class PredictionService {
  final SupabaseClient _client;

  PredictionService() : _client = AppSupabase.client;

  Future<List<BroadcastPrediction>> fetchByDate(DateTime date) async {
  final dateStr = DateFormat('yyyy-MM-dd').format(date);

  // 1) Fetch without complex ordering; keep SQL simple
  final response = await _client
      .from('predictions_for_broadcast')
      .select()
      .eq('date', dateStr);

  final list = (response as List)
      .map((row) => BroadcastPrediction.fromJson(row as Map<String, dynamic>))
      .toList();

  // 2) Apply custom sort in service layer:
  //    - flag_not_playing == true → bottom
  //    - then actual_points DESC
  //    - then elfies_number DESC
  list.sort((a, b) {
    final aFlag = a.notPlaying == true;
    final bFlag = b.notPlaying == true;

    // First: push "not playing" items to bottom
    if (aFlag && !bFlag) return 1;   // a goes down
    if (!aFlag && bFlag) return -1;  // b goes down

    // Second: sort by actual_points DESC
    final aPoints = a.crowdScore;
    final bPoints = b.crowdScore;
    if (aPoints != bPoints) {
      return bPoints.compareTo(aPoints); // highest first
    }

    // Third: sort by elfies_number DESC
    final aElf = a.elfiesNumber ?? -double.infinity;
    final bElf = b.elfiesNumber ?? -double.infinity;
    if (aElf != bElf) {
      return bElf.compareTo(aElf); // highest first
    }

    // Optional: stable fallback (e.g. by name)
    return (a.name ?? '').compareTo(b.name ?? '');
  });

  return list;
}

//the day which is now - no. of historical days requested
  Future<List<BroadcastPrediction>> fetchToday(int historicalDays) {
    historicalDays += 1; //today = now - 1 (U.S)
  
    return fetchByDate(DateTime.now().subtract(Duration(days: historicalDays)));
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
