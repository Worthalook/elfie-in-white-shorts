
import 'dart:async';
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'supabase_bootstrap.dart';
import 'predictions_today.dart';
import 'package:google_fonts/google_fonts.dart';
import 'notification_service.dart';
import 'package:flutter_local_notifications/flutter_local_notifications.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await initLocalNotifications();
}

final FlutterLocalNotificationsPlugin flutterLocalNotificationsPlugin =
    FlutterLocalNotificationsPlugin();

Future<void> initLocalNotifications() async {
  const AndroidInitializationSettings androidInit =
      AndroidInitializationSettings('@mipmap/ic_launcher');

  const DarwinInitializationSettings iosInit = DarwinInitializationSettings();

  const InitializationSettings initSettings = InitializationSettings(
    android: androidInit,
    iOS: iosInit,
  );

  await flutterLocalNotificationsPlugin.initialize(initSettings);

  // Android 13+ permission (if you want to handle explicitly)
  final androidPlugin = flutterLocalNotificationsPlugin
      .resolvePlatformSpecificImplementation<
          AndroidFlutterLocalNotificationsPlugin>();
  await androidPlugin?.requestNotificationsPermission();
}

Future<void> showDataUpdatedNotification({
  String title = 'Data updated',
  String body = 'New data has been received from Supabase.',
}) async {
  const AndroidNotificationDetails androidDetails =
      AndroidNotificationDetails(
    'supabase_updates', // channel id
    'Supabase Updates', // channel name
    channelDescription: 'Notifications for Supabase data changes',
    importance: Importance.max,
    priority: Priority.high,
  );

  const DarwinNotificationDetails iosDetails = DarwinNotificationDetails();

  const NotificationDetails platformDetails = NotificationDetails(
    android: androidDetails,
    iOS: iosDetails,
  );

  await flutterLocalNotificationsPlugin.show(
    0, // notification id
    title,
    body,
    platformDetails,
  );
}
