package com.chatting.realtimechatbursthandlingengine.websocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.*;

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
            broadcastExecutor.execute(() -> {
                try {
                    synchronized (session) {
                        session.sendMessage(message);
                    }
                } catch (IOException e) {
                    log.warn("failed to send message to session {}: {}", session.getId(), e.getMessage());
                }
            });
        }
    }


    private final java.util.concurrent.atomic.AtomicLong droppedCount = new java.util.concurrent.atomic.AtomicLong();

    // k6 부하 테스트(VUS=50, MSG_INTERVAL_MS=10ms)로 20/50/100/500 비교 측정 후 결정.
    // 50: latency avg 30ms/p95 109ms (체감상 즉시) vs drop rate ~40%
    // 100: latency avg 495ms/p95 1.25s (체감상 지연) vs drop rate ~28% - 개선 폭 대비 latency 손해가 커서 기각
    private static final int QUEUE_CAPACITY = Integer.getInteger("queue.capacity", 50);

    private final ExecutorService broadcastExecutor = new ThreadPoolExecutor(
            64,
            64,
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(QUEUE_CAPACITY),
            (r, executor) -> {
                droppedCount.incrementAndGet();
                new ThreadPoolExecutor.DiscardOldestPolicy().rejectedExecution(r, executor);
            }
    );

    public long getDroppedCount() {
        return droppedCount.get();
    }

    public int getSessionCount() {
        return sessions.size();
    }

    public int getQueueCapacity() {
        return QUEUE_CAPACITY;
    }
}
