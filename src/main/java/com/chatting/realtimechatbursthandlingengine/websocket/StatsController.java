package com.chatting.realtimechatbursthandlingengine.websocket;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatsController {

    private final SessionRegistry sessionRegistry;

    public StatsController(SessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    @GetMapping("/stats")
    public Stats stats() {
        return new Stats(
                sessionRegistry.getSessionCount(),
                sessionRegistry.getDroppedCount(),
                sessionRegistry.getQueueCapacity()
        );
    }

    public record Stats(int sessions, long dropped, int queueCapacity) {}
}
