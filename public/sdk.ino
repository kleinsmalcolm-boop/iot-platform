@'
/*
 * =====================================================
 * IoT Platform SDK — Arduino / ESP32
 * =====================================================
 * QUICK START:
 * 1. Change WIFI_SSID and WIFI_PASSWORD to your WiFi
 * 2. Change API_KEY to the key from your dashboard
 * 3. Upload to your ESP32
 * =====================================================
 */

#include <WiFi.h>
#include <HTTPClient.h>
#include <ArduinoJson.h>

const char* WIFI_SSID     = "YOUR_WIFI_NAME";
const char* WIFI_PASSWORD = "YOUR_WIFI_PASSWORD";
const char* API_KEY       = "iot_YOUR_API_KEY_HERE";
const char* SERVER_URL    = "http://192.168.2.11:3000";

WiFiClient wifiClient;
HTTPClient http;
unsigned long lastSend    = 0;
unsigned long lastCommand = 0;
const int SEND_INTERVAL    = 5000;
const int COMMAND_INTERVAL = 10000;

void connectWiFi() {
  Serial.print("Connecting to WiFi");
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  int attempts = 0;
  while (WiFi.status() != WL_CONNECTED && attempts < 20) {
    delay(500);
    Serial.print(".");
    attempts++;
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n✅ WiFi connected: " + WiFi.localIP().toString());
  } else {
    Serial.println("\n❌ WiFi failed. Check your credentials.");
  }
}

bool sendData(String pin, float value) {
  if (WiFi.status() != WL_CONNECTED) { connectWiFi(); return false; }
  String url = String(SERVER_URL) + "/api/device/data";
  http.begin(url);
  http.addHeader("Content-Type", "application/json");
  http.addHeader("X-API-Key", API_KEY);
  StaticJsonDocument<200> doc;
  doc["pin"] = pin;
  doc["value"] = value;
  String body;
  serializeJson(doc, body);
  int responseCode = http.POST(body);
  http.end();
  if (responseCode == 200) { Serial.println("✅ Sent " + pin + " = " + String(value)); return true; }
  Serial.println("❌ Failed: " + String(responseCode));
  return false;
}

bool sendMultiple(String pins[], float values[], int count) {
  if (WiFi.status() != WL_CONNECTED) { connectWiFi(); return false; }
  String url = String(SERVER_URL) + "/api/device/data";
  http.begin(url);
  http.addHeader("Content-Type", "application/json");
  http.addHeader("X-API-Key", API_KEY);
  StaticJsonDocument<1024> doc;
  JsonArray dataArray = doc.createNestedArray("data");
  for (int i = 0; i < count; i++) {
    JsonObject reading = dataArray.createNestedObject();
    reading["pin"] = pins[i];
    reading["value"] = values[i];
  }
  String body;
  serializeJson(doc, body);
  int responseCode = http.POST(body);
  http.end();
  if (responseCode == 200) { Serial.println("✅ Sent " + String(count) + " readings"); return true; }
  Serial.println("❌ Failed: " + String(responseCode));
  return false;
}

void checkCommands() {
  if (WiFi.status() != WL_CONNECTED) return;
  String url = String(SERVER_URL) + "/api/device/commands";
  http.begin(url);
  http.addHeader("X-API-Key", API_KEY);
  int responseCode = http.GET();
  if (responseCode == 200) {
    String payload = http.getString();
    StaticJsonDocument<1024> doc;
    deserializeJson(doc, payload);
    JsonArray commands = doc["commands"];
    for (JsonObject cmd : commands) {
      String command = cmd["command"].as<String>();
      String value   = cmd["value"].as<String>();
      Serial.println("📥 Command: " + command + " = " + value);
      handleCommand(command, value);
    }
  }
  http.end();
}

void handleCommand(String command, String value) {
  if (command == "LED_ON")        { digitalWrite(LED_BUILTIN, HIGH); Serial.println("💡 LED ON"); }
  else if (command == "LED_OFF")  { digitalWrite(LED_BUILTIN, LOW);  Serial.println("💡 LED OFF"); }
  else if (command == "RESTART")  { Serial.println("🔄 Restarting..."); delay(1000); ESP.restart(); }
  else if (command == "BLINK") {
    for (int i = 0; i < 5; i++) {
      digitalWrite(LED_BUILTIN, HIGH); delay(200);
      digitalWrite(LED_BUILTIN, LOW);  delay(200);
    }
  }
  else { Serial.println("❓ Unknown command: " + command); }
}

void setup() {
  Serial.begin(115200);
  delay(1000);
  pinMode(LED_BUILTIN, OUTPUT);
  Serial.println("\n=============================================");
  Serial.println("  ⚡ IoT Platform SDK v1.0");
  Serial.println("  Server: 192.168.2.11:3000");
  Serial.println("=============================================");
  connectWiFi();
  for (int i = 0; i < 2; i++) {
    digitalWrite(LED_BUILTIN, HIGH); delay(300);
    digitalWrite(LED_BUILTIN, LOW);  delay(300);
  }
  Serial.println("🚀 Ready. Sending every " + String(SEND_INTERVAL/1000) + "s");
}

void loop() {
  unsigned long now = millis();
  if (now - lastSend >= SEND_INTERVAL) {
    lastSend = now;
    float temperature = 20.0 + (random(0, 150) / 10.0);
    float humidity    = 40.0 + (random(0, 400) / 10.0);
    float pressure    = 1010.0 + (random(0, 50) / 10.0);
    int   motion      = random(0, 2);
    String pins[]   = {"temperature", "humidity", "pressure", "motion"};
    float  values[] = {temperature, humidity, pressure, (float)motion};
    sendMultiple(pins, values, 4);
  }
  if (now - lastCommand >= COMMAND_INTERVAL) {
    lastCommand = now;
    checkCommands();
  }
}
'@ | Set-Content -Path "C:\Users\Malcom\iot-platform\public\sdk.ino" -Encoding UTF8