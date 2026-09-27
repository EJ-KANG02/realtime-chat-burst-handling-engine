package com.chatting.realtimechatbursthandlingengine.websocket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {
    @Bean
    public SessionRegistry sessionRegistry() {
        return new SessionRegistry();
    }
    @Bean
    public ChatWebSocketHandler chatWebSocketHandler(SessionRegistry sessionRegistry) {
        return new ChatWebSocketHandler(sessionRegistry);
    }
    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(chatWebSocketHandler(sessionRegistry()),"/chat")
                .setAllowedOrigins("*");
    }
}
