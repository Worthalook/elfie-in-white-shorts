import 'package:supabase_flutter/supabase_flutter.dart';

class AppSupabase {
  // TODO: replace with your actual project values
  static const String supabaseUrl = 'https://gbxxrfrmzgltdyfunwaa.supabase.co';
  static const String supabaseAnonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdieHhyZnJtemdsdGR5ZnVud2FhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI4NjAxOTgsImV4cCI6MjA3ODQzNjE5OH0.X_CnVXWArB8tPO1Cq2I18dpeZ7de4dRZliIKVzmGFok';

  static Future<void> init() async {
    await Supabase.initialize(
      url: supabaseUrl,
      anonKey: supabaseAnonKey,
    );
  }

  static SupabaseClient get client => Supabase.instance.client;
}
