import 'package:flutter/material.dart';

import '../supabase_client.dart';
import 'today_predictions_page.dart';
import 'yesterday_predictions_page.dart';
import 'placeholder_page.dart';
import 'auth_page.dart';

class HomePage extends StatelessWidget {
  const HomePage({super.key});

  void _go(BuildContext context, Widget page) {
    Navigator.of(context).push(
      MaterialPageRoute(builder: (_) => page),
    );
  }

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;
    final user = AppSupabase.client.auth.currentUser;

    return Scaffold(
      appBar: AppBar(
        title: const Text('WhiteShorts'),
        centerTitle: true,
      ),
      body: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(
          children: [
            if (user != null) ...[
              Align(
                alignment: Alignment.centerLeft,
                child: Text(
                  'Signed in as: ${user.email ?? "anonymous"}',
                  style: TextStyle(
                    color: cs.onBackground.withOpacity(0.7),
                    fontSize: 13,
                  ),
                ),
              ),
              const SizedBox(height: 12),
            ],
            _BigMenuButton(
              label: "Today's preds/results",
              icon: Icons.today,
              color: cs.primaryContainer,
              onTap: () => _go(context, const TodayPredictionsPage()),
            ),
            const SizedBox(height: 16),
            _BigMenuButton(
              label: "Yesterday's preds/results",
              icon: Icons.history,
              color: cs.secondaryContainer,
              onTap: () => _go(context, const YesterdayPredictionsPage()),
            ),
            const SizedBox(height: 16),
            _BigMenuButton(
              label: "Create account / subscribe",
              icon: Icons.person_add_alt_1,
              color: cs.tertiaryContainer,
              onTap: () => _go(
                context,
                const AuthPage(),
              ),
            ),
            const SizedBox(height: 16),
            _BigMenuButton(
              label: "'Tip' from a win",
              icon: Icons.celebration,
              color: cs.surfaceVariant,
              onTap: () => _go(
                context,
                const PlaceholderPage(
                  title: "Tip from a win",
                  message:
                      "Soon you'll be able to send a tip or shout after a big win.",
                ),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _BigMenuButton extends StatelessWidget {
  final String label;
  final IconData icon;
  final Color color;
  final VoidCallback onTap;

  const _BigMenuButton({
    required this.label,
    required this.icon,
    required this.color,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    final cs = Theme.of(context).colorScheme;

    return SizedBox(
      width: double.infinity,
      height: 80,
      child: ElevatedButton(
        onPressed: onTap,
        style: ElevatedButton.styleFrom(
          backgroundColor: color,
          foregroundColor: cs.onSurface,
          shape: RoundedRectangleBorder(
            borderRadius: BorderRadius.circular(20),
          ),
        ),
        child: Row(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(icon, size: 28),
            const SizedBox(width: 12),
            Text(
              label,
              style: const TextStyle(
                fontSize: 20,
                fontWeight: FontWeight.w600,
              ),
            ),
          ],
        ),
      ),
    );
  }
}
