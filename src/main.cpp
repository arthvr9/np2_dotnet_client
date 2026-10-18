#include <Arduino.h>
#include <WiFi.h>
#include <HTTPClient.h>

#include "SignalRHub.h"

#define WIFI_SSID "wifi_mesh"
#define WIFI_PASSWORD "Zeus@1709"
#define HUB_URL "http://192.168.1.9:5284/iot"
#define AUTH_URL "http://192.168.1.9:5284/auth"

SignalRHub hub;

const int pinoButton = PINO_BOTAO;
const int pinoBuzzer = PINO_BUZZER;
static String tokenEmCache;

static String pedirToken()
{
  HTTPClient http;
  if (!http.begin(AUTH_URL))
    return "";
  http.addHeader("Content-Type", "application/json");

  JsonDocument req;
  req["deviceId"] = DEVICE_NAME;
  req["deviceKey"] = DEVICE_KEY;
  String corpo;
  serializeJson(req, corpo);

  const int code = http.POST(corpo);
  if (code != 200)
  {
    Serial.printf("auth falhou: HTTP %d\n", code);
    http.end();
    return "";
  }

  JsonDocument resp;

  const DeserializationError erro = deserializeJson(resp, http.getString());

  http.end();

  if (erro)
  {
    Serial.printf("json invalid (%s)\n", erro.c_str());
    return "";
  }

  return resp["token"].as<String>();
}

void setup()
{
  Serial.begin(115200);

  pinMode(pinoButton, INPUT_PULLUP);
  pinMode(pinoBuzzer, OUTPUT);
  digitalWrite(pinoBuzzer, HIGH);

  while (!Serial && millis() < 3000)
    delay(10);

  WiFi.setAutoReconnect(true);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.print("wifi");
  while (WiFi.status() != WL_CONNECTED)
  {
    delay(500);
    Serial.print(".");
  }
  Serial.printf(" ok, ip: %s\n", WiFi.localIP().toString().c_str());

  hub.setTokenProvider([]() -> String
                       {
    if (!tokenEmCache.length()) tokenEmCache = pedirToken();
    return tokenEmCache; });

  hub.onError([](const char *motivo)
              {
    if (strstr(motivo, "401")) tokenEmCache = ""; });

  hub.onConnected([]
                  { Serial.println("hub conectado"); });

  hub.onDisconnected([]
                     { Serial.println("hub caiu, reconectando");
                      digitalWrite(pinoBuzzer, HIGH); });

  hub.on("ReceiveSignal", [](JsonArrayConst args)
         {
          Serial.printf("ReceiveSignal: %sd\n", args[0].as<const char*>());
          const bool pressed = args[1].as<bool>();
          digitalWrite(pinoBuzzer, pressed ? LOW : HIGH); });

  hub.begin(HUB_URL);

  for (int i = 0; i < 3; i++)
  {
    digitalWrite(pinoBuzzer, LOW);
    delay(150);
    digitalWrite(pinoBuzzer, HIGH);
    delay(150);
  }
}

void loop()
{
  static bool anterior = false;
  static uint32_t ultimaBorda = 0;

  const bool agora = (digitalRead(pinoButton) == LOW);

  if (agora != anterior && millis() - ultimaBorda >= 40)
  {
    ultimaBorda = millis();
    anterior = agora;
    if (hub.connected())
    {
      Serial.println("botão apertado");
      hub.send("SendSignal", agora);
    }
  }

  hub.loop();

  static uint32_t ultimo = 0;
  if (hub.connected() && millis() - ultimo >= 10000)
  {
    ultimo = millis();
    hub.invoke("Heartbeat", [](JsonVariantConst result, const char *error)
               {
      if (error) Serial.printf("heartbeat falhou: %s\n", error);
      else Serial.printf("heartbeat ok: %s\n", result.as<String>().c_str()); }, millis() / 1000);
  }
}
