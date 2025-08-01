package com.dev.unearlogconsumer.monitor;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/health")
@RequiredArgsConstructor
public class HealthCheckController {

    private final RedisTemplate<String, String> redisTemplate;
    private static final String STREAM_KEY = "stream:user_action_logs";

    @GetMapping
    public ResponseEntity<?> health() {
        try {
            // 단순 pong
            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "message", "Consumer is running"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                    "status", "DOWN",
                    "error", e.getMessage()
            ));
        }
    }
}