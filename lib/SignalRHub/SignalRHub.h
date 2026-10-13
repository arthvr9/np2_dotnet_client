// Cliente SignalR (protocolo JSON) sobre WebSocket, para ESP32.
//
// Protocolo: cada mensagem e um JSON terminado pelo separador de registro 0x1E.
// Referencia: https://github.com/dotnet/aspnetcore/blob/main/src/SignalR/docs/specs/HubProtocol.md
#pragma once

#include <Arduino.h>
#include <ArduinoJson.h>
#include <WebSocketsClient.h>

#include <functional>
#include <map>

class SignalRHub {
public:
  using MethodHandler = std::function<void(JsonArrayConst args)>;
  // error == nullptr quando a chamada teve sucesso.
  using CompletionHandler = std::function<void(JsonVariantConst result, const char* error)>;

  // hubUrl no formato http://host:porta/meuHub (https tambem).
  void begin(const String& hubUrl);
  void end();
  void loop();

  // Metodos que o servidor chama neste device.
  void on(const String& target, MethodHandler handler) { m_handlers[target] = handler; }

  void onConnected(std::function<void()> cb) { m_onConnected = cb; }
  void onDisconnected(std::function<void()> cb) { m_onDisconnected = cb; }

  // Enviado como "Authorization: Bearer ..." no negotiate e no WebSocket.
  void setAccessToken(const String& token) { m_accessToken = token; }

  bool connected() const { return m_state == State::Connected; }

  // Dispara e esquece.
  template <typename... Args>
  bool send(const char* target, Args&&... args) {
    JsonDocument doc;
    doc["type"] = 1;
    doc["target"] = target;
    JsonArray a = doc["arguments"].to<JsonArray>();
    (a.add(std::forward<Args>(args)), ...);
    return sendMessage(doc);
  }

  // Espera o retorno do servidor; onResult e chamado quando a completion chega.
  template <typename... Args>
  bool invoke(const char* target, CompletionHandler onResult, Args&&... args) {
    JsonDocument doc;
    const String id(m_nextInvocationId);
    doc["type"] = 1;
    doc["invocationId"] = id;
    doc["target"] = target;
    JsonArray a = doc["arguments"].to<JsonArray>();
    (a.add(std::forward<Args>(args)), ...);
    if (!sendMessage(doc)) return false;
    m_nextInvocationId++;
    m_pending[id] = onResult;
    return true;
  }

private:
  enum class State { Idle, Negotiating, Connecting, Handshaking, Connected, Backoff };

  void setState(State s);
  bool negotiate(String& outWsUrl);
  void openWebSocket(const String& wsUrl);
  void onWsEvent(WStype_t type, uint8_t* payload, size_t length);
  void onFrame(const char* data, size_t length);
  void dispatch(JsonDocument& doc);
  bool sendMessage(JsonDocument& doc);
  bool sendRaw(const String& payload);
  void dropConnection(const char* motivo);

  static bool parseWsUrl(const String& url, String& host, uint16_t& port, String& path, bool& secure);

  static constexpr char kSeparator = 0x1E;
  static constexpr uint32_t kPingIntervalMs = 15000;
  static constexpr uint32_t kServerTimeoutMs = 30000;
  static constexpr uint32_t kBackoffMs = 5000;

  WebSocketsClient m_ws;
  String m_hubUrl;
  String m_accessToken;
  String m_rxBuffer;

  State m_state = State::Idle;
  uint32_t m_stateSince = 0;
  uint32_t m_lastSent = 0;
  uint32_t m_lastReceived = 0;
  uint32_t m_nextInvocationId = 1;

  std::map<String, MethodHandler> m_handlers;
  std::map<String, CompletionHandler> m_pending;
  std::function<void()> m_onConnected;
  std::function<void()> m_onDisconnected;
};
