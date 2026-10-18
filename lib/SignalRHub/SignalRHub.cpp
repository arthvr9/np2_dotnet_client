#include "SignalRHub.h"

#include <HTTPClient.h>

void SignalRHub::begin(const String& hubUrl) {
  m_hubUrl = hubUrl;
  while (m_hubUrl.endsWith("/")) m_hubUrl.remove(m_hubUrl.length() - 1);

  m_ws.onEvent([this](WStype_t type, uint8_t* payload, size_t length) {
    onWsEvent(type, payload, length);
  });
  // Reconexao e nossa: o token do negotiate expira, nao da pra so reabrir a mesma URL.
  m_ws.setReconnectInterval(0);

  setState(State::Negotiating);
}

void SignalRHub::end() {
  dropConnection("end()");
  m_state = State::Idle;
}

void SignalRHub::setState(State s) {
  m_state = s;
  m_stateSince = millis();
}

void SignalRHub::loop() {
  switch (m_state) {
    case State::Idle:
      return;

    case State::Negotiating: {
      String wsUrl;
      if (!negotiate(wsUrl)) {
        if (m_authFalhou) {
          m_authFalhou = false;
          falhou("negotiate recusado (401/403)", kBackoffAuthMs);
        } else {
          falhou("negotiate falhou", 0);
        }
        return;
      }
      openWebSocket(wsUrl);
      if (m_state == State::Negotiating) setState(State::Connecting);
      return;
    }

    case State::Backoff:
      if (millis() - m_stateSince >= m_backoffMs) setState(State::Negotiating);
      return;

    case State::Connecting:
    case State::Handshaking: {
      m_ws.loop();
      // m_ws.loop() despacha eventos que mudam o estado e carimbam m_stateSince
      // com um millis() POSTERIOR ao desta passada. Ler o relogio antes daqui
      // faria a subtracao sem sinal estourar e derrubar a conexao recem-aberta.
      if (m_state != State::Connecting && m_state != State::Handshaking) return;
      if (millis() - m_stateSince >= kServerTimeoutMs) dropConnection("timeout conectando/handshake");
      return;
    }

    case State::Connected: {
      m_ws.loop();
      if (m_state != State::Connected) return;

      const uint32_t now = millis();
      if (now - m_lastReceived >= kServerTimeoutMs) {
        dropConnection("servidor mudo por 30s");
        return;
      }
      if (now - m_lastSent >= kPingIntervalMs) {
        JsonDocument ping;
        ping["type"] = 6;
        sendMessage(ping);
      }
      return;
    }
  }
}

bool SignalRHub::negotiate(String& outWsUrl) {
  // Renova o token antes de cada tentativa: um JWT expirado so se resolve
  // buscando outro, nunca repetindo o negotiate com o mesmo.
  if (m_tokenProvider) m_accessToken = m_tokenProvider();

  HTTPClient http;
  if (!http.begin(m_hubUrl + "/negotiate?negotiateVersion=1")) return false;

  http.addHeader("Content-Type", "text/plain;charset=UTF-8");
  if (m_accessToken.length()) http.addHeader("Authorization", "Bearer " + m_accessToken);

  const int code = http.POST("");
  if (code != HTTP_CODE_OK) {
    // Codigo negativo = erro de transporte do HTTPClient; positivo = status HTTP.
    if (code == HTTP_CODE_UNAUTHORIZED || code == HTTP_CODE_FORBIDDEN) {
      log_e("negotiate recusado: HTTP %d (token invalido ou expirado)", code);
      m_accessToken = "";  // forca o provider a emitir um novo na proxima volta
      m_authFalhou = true;
    } else if (code < 0) {
      log_e("negotiate falhou: %s (%d)", HTTPClient::errorToString(code).c_str(), code);
    } else {
      log_e("negotiate falhou: HTTP %d", code);
    }
    http.end();
    return false;
  }

  JsonDocument doc;
  // getString() e nao getStream(): o stream e o socket cru, sem de-chunking.
  // Se a resposta vier com Transfer-Encoding: chunked, o parser engasga no
  // tamanho hexadecimal do chunk antes de chegar no JSON.
  const DeserializationError err = deserializeJson(doc, http.getString());
  http.end();
  if (err) {
    log_e("negotiate json invalido: %s", err.c_str());
    return false;
  }

  // Azure SignalR Service responde com redirecionamento em vez de token.
  if (doc["url"].is<const char*>()) {
    log_e("negotiate pediu redirecionamento (Azure SignalR) - nao suportado");
    return false;
  }

  const char* token = doc["connectionToken"].is<const char*>() ? doc["connectionToken"]
                                                               : doc["connectionId"];
  if (!token) {
    log_e("negotiate sem connectionToken");
    return false;
  }

  outWsUrl = m_hubUrl;
  if (outWsUrl.startsWith("https://")) outWsUrl = "wss://" + outWsUrl.substring(8);
  else if (outWsUrl.startsWith("http://")) outWsUrl = "ws://" + outWsUrl.substring(7);
  outWsUrl += "?id=";
  outWsUrl += token;
  return true;
}

void SignalRHub::openWebSocket(const String& wsUrl) {
  String host, path;
  uint16_t port = 0;
  bool secure = false;
  if (!parseWsUrl(wsUrl, host, port, path, secure)) {
    log_e("url ws invalida: %s", wsUrl.c_str());
    setState(State::Backoff);
    return;
  }

  if (m_accessToken.length()) {
    m_ws.setExtraHeaders(("Authorization: Bearer " + m_accessToken).c_str());
  }

  m_rxBuffer = "";
  if (secure) m_ws.beginSSL(host.c_str(), port, path.c_str());
  else m_ws.begin(host.c_str(), port, path.c_str());
}

void SignalRHub::onWsEvent(WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED: {
      // Handshake: define o protocolo antes de qualquer outra mensagem.
      setState(State::Handshaking);
      m_lastReceived = millis();
      sendRaw(String("{\"protocol\":\"json\",\"version\":1}") + kSeparator);
      break;
    }

    case WStype_TEXT:
    case WStype_BIN:
      m_lastReceived = millis();
      onFrame(reinterpret_cast<const char*>(payload), length);
      break;

    case WStype_DISCONNECTED:
      dropConnection("websocket desconectado");
      break;

    case WStype_ERROR:
      dropConnection("erro no websocket");
      break;

    default:
      break;
  }
}

void SignalRHub::onFrame(const char* data, size_t length) {
  // Um frame pode trazer varias mensagens, e uma mensagem pode ficar partida
  // entre frames. Acumula e consome ate o ultimo separador.
  m_rxBuffer.concat(data, length);

  int start = 0;
  for (;;) {
    const int end = m_rxBuffer.indexOf(kSeparator, start);
    if (end < 0) break;

    const String raw = m_rxBuffer.substring(start, end);
    start = end + 1;

    JsonDocument doc;
    if (deserializeJson(doc, raw)) {
      log_w("mensagem invalida descartada");
      continue;
    }
    dispatch(doc);
  }
  m_rxBuffer.remove(0, start);
}

void SignalRHub::dispatch(JsonDocument& doc) {
  if (m_state == State::Handshaking) {
    // Resposta do handshake: {} em caso de sucesso, {"error":"..."} se falhou.
    if (doc["error"].is<const char*>()) {
      log_e("handshake recusado: %s", doc["error"].as<const char*>());
      dropConnection("handshake recusado");
      return;
    }
    setState(State::Connected);
    m_backoffMs = kBackoffMinMs;
    m_lastSent = millis();
    if (m_onConnected) m_onConnected();
    return;
  }

  switch (doc["type"].as<int>()) {
    case 1: {  // Invocation: o servidor chamando um metodo daqui.
      const char* target = doc["target"];
      if (!target) break;
      auto it = m_handlers.find(String(target));
      if (it != m_handlers.end()) it->second(doc["arguments"].as<JsonArrayConst>());
      break;
    }

    case 3: {  // Completion: resposta de um invoke nosso.
      const char* id = doc["invocationId"];
      if (!id) break;
      auto it = m_pending.find(String(id));
      if (it == m_pending.end()) break;
      const auto handler = it->second;
      m_pending.erase(it);
      if (handler) handler(doc["result"], doc["error"] | static_cast<const char*>(nullptr));
      break;
    }

    case 6:  // Ping do servidor: so renova o lastReceived, ja feito.
      break;

    case 7:  // Close
      log_w("servidor encerrou: %s", doc["error"] | "sem motivo");
      dropConnection("close vindo do servidor");
      break;

    default:
      break;
  }
}

bool SignalRHub::sendMessage(JsonDocument& doc) {
  String payload;
  serializeJson(doc, payload);
  payload += kSeparator;
  return sendRaw(payload);
}

bool SignalRHub::sendRaw(const String& payload) {
  if (!m_ws.sendTXT(payload.c_str(), payload.length())) return false;
  m_lastSent = millis();
  return true;
}

void SignalRHub::dropConnection(const char* motivo) {
  if (m_state == State::Idle || m_state == State::Backoff) return;  // ja caiu

  const bool estavaConectado = (m_state == State::Connected);
  // Antes do disconnect(): ele dispara WStype_DISCONNECTED de volta aqui,
  // e com o estado ainda em Connected o callback saia em duplicata.
  setState(State::Backoff);
  m_ws.disconnect();
  m_rxBuffer = "";
  m_pending.clear();

  // Uma conexao que chegou a vigorar nao e sintoma de servidor fora do ar:
  // reinicia o backoff para reconectar rapido.
  if (estavaConectado) m_backoffMs = kBackoffMinMs;
  else m_backoffMs = min(m_backoffMs * 2, kBackoffMaxMs);

  log_w("conexao encerrada: %s (proxima tentativa em %lums)", motivo, (unsigned long)m_backoffMs);
  if (m_onError) m_onError(motivo);
  if (estavaConectado && m_onDisconnected) m_onDisconnected();
}

void SignalRHub::falhou(const char* motivo, uint32_t backoffMs) {
  if (backoffMs) m_backoffMs = backoffMs;
  else m_backoffMs = min(m_backoffMs * 2, kBackoffMaxMs);

  setState(State::Backoff);
  log_w("%s (proxima tentativa em %lums)", motivo, (unsigned long)m_backoffMs);
  if (m_onError) m_onError(motivo);
}

bool SignalRHub::parseWsUrl(const String& url, String& host, uint16_t& port, String& path, bool& secure) {
  String u = url;
  if (u.startsWith("wss://")) { secure = true;  u = u.substring(6); }
  else if (u.startsWith("ws://")) { secure = false; u = u.substring(5); }
  else return false;

  const int slash = u.indexOf('/');
  const String authority = (slash < 0) ? u : u.substring(0, slash);
  path = (slash < 0) ? "/" : u.substring(slash);

  const int colon = authority.indexOf(':');
  if (colon < 0) {
    host = authority;
    port = secure ? 443 : 80;
  } else {
    host = authority.substring(0, colon);
    port = static_cast<uint16_t>(authority.substring(colon + 1).toInt());
  }
  return host.length() > 0 && port > 0;
}
