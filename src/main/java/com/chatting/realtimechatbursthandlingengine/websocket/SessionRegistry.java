package com.chatting.realtimechatbursthandlingengine.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SessionRegistry {
    private static final Logger log = LoggerFactory.getLogger(SessionRegistry.class);
    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();
    public void register(WebSocketSession session){
        sessions.add(session);
        log.info("session registered: {} (total={})", session.getId(), sessions.size());
    }

    public void unregister(WebSocketSession session){
        sessions.remove(session);
        log.info("session unregistered: {} (total={})", session.getId(), sessions.size());
    }

    public void broadcast(TextMessage message){
        for (WebSocketSession session : sessions) {
            try {
                session.sendMessage(message);
            } catch (IOException e) {
                log.warn("failed to send message to session {}: {}", session.getId(), e.getMessage());
            }
        }
    }
}
