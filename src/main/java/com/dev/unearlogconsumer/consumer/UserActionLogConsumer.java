package com.dev.unearlogconsumer.consumer;

import com.dev.unearlogconsumer.domain.UserActionLog;
import com.dev.unearlogconsumer.domain.UserActionLogRepository;
import io.lettuce.core.RedisCommandExecutionException;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Slf4j
@Component
@RequiredArgsConstructor
public class UserActionLogConsumer {

    private static final String STREAM_KEY = "stream:user_action_logs";
    private static final String GROUP_NAME = "log-consumer-group";
    private static final String CONSUMER_NAME = "consumer-1";

    private final RedisTemplate<String, String> redisTemplate;
    private final UserActionLogRepository userActionLogRepository;

    @PostConstruct
    public void initGroup() {
        try {
            // Stream 존재 여부 확인
            if (!redisTemplate.hasKey(STREAM_KEY)) {
                redisTemplate.opsForStream().add(STREAM_KEY, Map.of("init", "true"));
            }

            // Consumer Group 존재 여부 확인 후 생성
            if (!isConsumerGroupExists()) {
                redisTemplate.opsForStream().createGroup(STREAM_KEY, ReadOffset.from("0"), GROUP_NAME);
                log.info("컨슈머 그룹 생성됨: {}", GROUP_NAME);
            } else {
                log.info("기존 컨슈머 그룹 사용: {}", GROUP_NAME);
            }
        } catch (Exception e) {
            log.error("컨슈머 그룹 초기화 실패", e);
            throw e;
        }
    }

    private boolean isConsumerGroupExists() {
        try {
            StreamInfo.XInfoGroups groups = redisTemplate.opsForStream().groups(STREAM_KEY);
            return groups.stream().anyMatch(group -> GROUP_NAME.equals(group.groupName()));
        } catch (Exception e) {
            return false;
        }
    }


    // 1일 1회 배치 스케쥴링
//    @Scheduled(cron = "0 0 0 * * *") // 매일 자정 실행
//    public void consumeBatch() {
//        List<UserActionLog> logsToSave = new ArrayList<>();
//
//        try {
//            while (true) {
//                List<MapRecord<String, Object, Object>> records =
//                        redisTemplate.opsForStream().read(Consumer.from(GROUP_NAME, CONSUMER_NAME),
//                                StreamReadOptions.empty().count(100),
//                                StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));
//
//                if (records == null || records.isEmpty()) break;
//
//                for (MapRecord<String, Object, Object> record : records) {
//                    Map<Object, Object> value = record.getValue();
//                    try {
//                        if (!value.containsKey("userId") || !value.containsKey("actionType") || !value.containsKey("timestamp")) {
//                            log.warn("필드 누락된 레코드: {}", value);
//                            continue;
//                        }
//
//                        UserActionLog logEntity = UserActionLog.builder()
//                                .userId(Long.parseLong((String) value.get("userId")))
//                                .actionType((String) value.get("actionType"))
//                                .metadata((String) value.get("metadata"))
//                                .createAt(Instant.ofEpochMilli(Long.parseLong((String) value.get("timestamp")))
//                                        .atZone(ZoneId.of("Asia/Seoul"))
//                                        .toLocalDateTime())
//                                .build();
//
//                        logsToSave.add(logEntity);
//
//                        redisTemplate.opsForStream()
//                                .acknowledge(STREAM_KEY, GROUP_NAME, record.getId());
//
//                        redisTemplate.opsForStream()
//                                .delete(STREAM_KEY, record.getId());
//
//
//                    } catch (Exception e) {
//                        log.warn("레코드 파싱 오류: {}, {}", value, e.getMessage());
//                    }
//                }
//            }
//
//            if (!logsToSave.isEmpty()) {
//                userActionLogRepository.saveAll(logsToSave); // batch insert
//                log.info("총 {}건 로그 저장 완료", logsToSave.size());
//            }
//
//        } catch (Exception e) {
//            log.error("배치 로그 소비 중 오류 발생", e);
//        }
//    }


    // 로그 수집시 바로 소비 ( 테스트용 )
    @Scheduled(fixedDelay = 1000) // 1초마다 pulling
    public void consume() {
        try {
            List<MapRecord<String, Object, Object>> records = redisTemplate.opsForStream()
                    .read(Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(50).block(Duration.ofSeconds(5)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));

            List<UserActionLog> batch = new ArrayList<>();
            List<RecordId> ackIds = new ArrayList<>();

            for (MapRecord<String, Object, Object> record : records) {
                Map<Object, Object> value = record.getValue();

                try {
                    if (!value.containsKey("userId") || !value.containsKey("actionType") || !value.containsKey("timestamp")) {
                        log.warn("필수 필드 누락. 레코드: {}", value);
                        continue;
                    }

                    UserActionLog logEntity = UserActionLog.builder()
                            .userId(Long.parseLong((String) value.get("userId")))
                            .actionType((String) value.get("actionType"))
                            .metadata((String) value.get("metadata"))
                            .createAt(Instant.ofEpochMilli(Long.parseLong((String) value.get("timestamp")))
                                    .atZone(ZoneId.of("Asia/Seoul"))
                                    .toLocalDateTime())
                            .build();

                    batch.add(logEntity);
                    ackIds.add(record.getId());

                } catch (Exception e) {
                    log.warn("레코드 파싱 실패. 레코드: {}, 오류: {}", value, e.getMessage());
                }
            }

            if (!batch.isEmpty()) {
                userActionLogRepository.saveAll(batch);
                log.info("총 {}개 로그 저장 완료", batch.size());

                // Redis에서 ack 처리하여 stream에서 제거
                redisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, ackIds.toArray(new RecordId[0]));
            }

        } catch (Exception e) {
            log.error("Redis 로그 소비 중 오류 발생", e);
        }
    }

}


