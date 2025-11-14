import 'package:flutter/material.dart';

import 'supabase_client.dart';
import 'pages/home_page.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await AppSupabase.init();
  runApp(const WhiteShortsCrowdApp());
}

class WhiteShortsCrowdApp extends StatelessWidget {
  const WhiteShortsCrowdApp({super.key});

  @override
  Widget build(BuildContext context) {
    const seedColor = Colors.teal; // change as needed

    return MaterialApp(
      title: 'WhiteShorts Crowd',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        useMaterial3: true,
        colorScheme: ColorScheme.fromSeed(
          seedColor: seedColor,
          brightness: Brightness.dark,
        ),
      ),
      home: const HomePage(),
    );
  }
}
