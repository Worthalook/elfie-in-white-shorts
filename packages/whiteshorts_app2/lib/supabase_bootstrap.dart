// lib/supabase_bootstrap.dart
import 'package:supabase_flutter/supabase_flutter.dart';

class SupabaseBootstrap {
  static Future<void> init({
    required String supabaseUrl,
    required String supabaseAnonKey,
  }) async {
    await Supabase.initialize(
      url: supabaseUrl,
      anonKey: supabaseAnonKey,
      authOptions: const FlutterAuthClientOptions(autoRefreshToken: false),
    );
  }

  static SupabaseClient get client => Supabase.instance.client;
}
