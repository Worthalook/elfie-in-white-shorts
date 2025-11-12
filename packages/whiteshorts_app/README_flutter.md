# WhiteShorts Flutter Bootstrap (Supabase Realtime)

## 1) Dependencies
Add to `pubspec.yaml`:
```yaml
dependencies:
  flutter:
    sdk: flutter
  supabase_flutter: ^2.5.0
  intl: ^0.19.0
```

## 2) Initialize Supabase
In your `main.dart`:
```dart
import 'package:flutter/material.dart';
import 'supabase_bootstrap.dart';
import 'predictions_today.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await SupabaseBootstrap.init(
    supabaseUrl: const String.fromEnvironment('SUPABASE_URL', defaultValue: 'https://YOUR.supabase.co'),
    supabaseAnonKey: const String.fromEnvironment('SUPABASE_ANON_KEY', defaultValue: 'YOUR_ANON_KEY'),
  );
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: const PredictionsTodayPage(),
      theme: ThemeData(useMaterial3: true),
    );
  }
}
```

You can pass keys at build time:
```
flutter run --dart-define SUPABASE_URL=https://... --dart-define SUPABASE_ANON_KEY=...
```

## 3) Database
Run the SQL in `db/001_create_predictions.sql` on your Supabase project (SQL Editor).  
This creates the table, indexes, RLS policies, and registers realtime.

## 4) CI writer
Use the previously provided `whiteshorts-broadcast` publisher with your service key to upsert rows.  
The app page `PredictionsTodayPage` will auto-stream inserts/updates/deletes for today’s date.

## Notes
- The filter uses device-local date (`yyyy-MM-dd`). If your jobs post in UTC for a different day, adapt to pass a specific date.
- To scope by team/player, add more filters or separate channels.
- Tighten RLS later: allow authenticated users only, or policy per API key/role.
