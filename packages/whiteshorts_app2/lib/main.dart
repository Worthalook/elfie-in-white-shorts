import 'package:flutter/material.dart';
import 'supabase_bootstrap.dart';
import 'predictions_today.dart';
import 'package:google_fonts/google_fonts.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await SupabaseBootstrap.init(
    supabaseUrl: const String.fromEnvironment('SUPABASE_URL', defaultValue: 'https://gbxxrfrmzgltdyfunwaa.supabase.co'),
    supabaseAnonKey: const String.fromEnvironment('SUPABASE_ANON_KEY', defaultValue: 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdieHhyZnJtemdsdGR5ZnVud2FhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI4NjAxOTgsImV4cCI6MjA3ODQzNjE5OH0.X_CnVXWArB8tPO1Cq2I18dpeZ7de4dRZliIKVzmGFok'),
  );
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
  title: "Elfie In WhiteShorts",
  theme: ThemeData(
    // Define the default brightness and colors.
    colorScheme: ColorScheme.fromSeed(
      seedColor: Colors.purple,
      // ···
      brightness: Brightness.dark,
    ),

    // Define the default `TextTheme`. Use this to specify the default
    // text styling for headlines, titles, bodies of text, and more.
    textTheme: TextTheme(
      displayLarge: const TextStyle(
        fontSize: 72,
        fontWeight: FontWeight.bold,
      ),
      // ···
      titleLarge: GoogleFonts.oswald(
        fontSize: 30,
        fontStyle: FontStyle.italic,
      ),
      bodyMedium: GoogleFonts.merriweather(),
      displaySmall: GoogleFonts.pacifico(),
    ),
  ),
  home: const PredictionsTodayPage(),
);
  }
    /*return MaterialApp(
      home: const PredictionsTodayPage(),//
//Fire Brick
//#b22222 | rgb(178,34,34)
      theme: ThemeData(colorSchemeSeed: Color(0xdd221160221) , brightness: Brightness.dark ),
    );
  }*/

  
}