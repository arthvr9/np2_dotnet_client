#include <Arduino.h>
#include <WiFi.h>

#include "SignalRHub.h"

#define WIFI_SSID ""
#define WIFI_PASSWORD ""
#define HUB_URL "http://192.168.1.9:5284/iot"

SignalRHub hub;

const int pinoButton = 1;
const int pinoBuzzer = 0;

void setup()
{
  Serial.begin(115200);

  pinMode(pinoButton, INPUT_PULLUP);
  pinMode(pinoBuzzer, OUTPUT);
  digitalWrite(pinoBuzzer, HIGH);

  while (!Serial && millis() < 3000)
    delay(10);

  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.print("wifi");
  while (WiFi.status() != WL_CONNECTED)
  {
    delay(500);
    Serial.print(".");
  }
  Serial.printf(" ok, ip: %s\n", WiFi.localIP().toString().c_str());

  hub.onConnected([]
                  { Serial.println("hub conectado"); });

  hub.onDisconnected([]
                     { Serial.println("hub caiu, reconectando"); });

  hub.on("ReceiveSignal", [](JsonArrayConst args)
         { Serial.printf("ReceiveSignal: %d\n", args[0].as<int>()); });

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
  const bool apertado = (digitalRead(pinoButton) == LOW);
  digitalWrite(pinoBuzzer, apertado ? LOW : HIGH);

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
