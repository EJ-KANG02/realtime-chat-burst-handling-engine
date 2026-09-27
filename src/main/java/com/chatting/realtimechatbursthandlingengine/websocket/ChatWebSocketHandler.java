package com.chatting.realtimechatbursthandlingengine.websocket;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

public class ChatWebSocketHandler extends TextWebSocketHandler {
    private final SessionRegistry sessionRegistry;

    public ChatWebSocketHandler(SessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session){
        sessionRegistry.register(session);
    }

    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message){
        sessionRegistry.broadcast(message);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status){
        sessionRegistry.unregister(session);
    }

}
