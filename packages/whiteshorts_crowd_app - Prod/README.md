# WhiteShorts Crowd App

Minimal Flutter app for broadcasting NHL model predictions with:

- Dark theme UI
- Home screen with big buttons:
  - Today's preds/results
  - Yesterday's preds/results
  - Create account / subscribe (holding page)
  - Tip from a win (holding page)
- Integration with Supabase table `predictions_for_broadcast`
- Crowd-sourced `crowd_score` up/down voting for today's predictions
- `actual_points` highlighted green when > 0
- GitHub Actions workflow to build Android APK and attach as artifact
- Supabase SQL migration to create the `predictions_for_broadcast` table

## Quick start

1. Create a new Flutter project:

   ```bash
   flutter create whiteshorts_crowd_app
   ```

2. Replace the generated contents with this package's files:
   - Overwrite `pubspec.yaml`
   - Replace the `lib/` folder with this `lib/`
   - Add `.github/workflows/flutter-ci.yml`
   - Add the `supabase/migrations/001_create_predictions_for_broadcast.sql` file

3. Set your Supabase credentials in `lib/supabase_client.dart`:

   ```dart
   static const String supabaseUrl = 'https://YOUR-PROJECT.supabase.co';
   static const String supabaseAnonKey = 'YOUR-ANON-KEY';
   ```

4. Run pub get:

   ```bash
   flutter pub get
   ```

5. Apply the Supabase migration (via Supabase SQL editor or CLI):

   ```sql
   -- contents of supabase/migrations/001_create_predictions_for_broadcast.sql
   ```

6. Run the app:

   ```bash
   flutter run
   ```

7. (Optional) Push to GitHub; the included workflow will build an APK on each push to `main`
   and upload it as an artifact.
