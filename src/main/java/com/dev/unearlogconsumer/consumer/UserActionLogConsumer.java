package com.dev.unearlogconsumer.consumer;

import com.dev.unearlogconsumer.domain.UserActionLog;
import com.dev.unearlogconsumer.domain.UserActionLogRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import java.time.Instant;
import java.time.LocalDateTime;
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

            if (!redisTemplate.hasKey(STREAM_KEY)) {
                redisTemplate.opsForStream().add(STREAM_KEY, Map.of("init", "true"));
            }

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

    // 20분 스케쥴링
    @Scheduled(fixedDelay = 3 * 60 * 1000)
    public void consumeBatch() {
        StopWatch stopWatch = new StopWatch(); // 스톱워치 생성
        stopWatch.start(); // 측정 시작

        int hour = LocalDateTime.now(ZoneId.of("Asia/Seoul")).getHour();
        int maxProcess = getMaxProcessByHour(hour);

        List<UserActionLog> logsToSave = new ArrayList<>();
        List<RecordId> ackIds = new ArrayList<>();
        List<RecordId> delIds = new ArrayList<>();
        int totalProcessed = 0;

        try {
            while (totalProcessed < maxProcess) {
                List<MapRecord<String, Object, Object>> records =
                        redisTemplate.opsForStream().read(Consumer.from(GROUP_NAME, CONSUMER_NAME),
                                StreamReadOptions.empty().count(100),
                                StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed()));

                if (records == null || records.isEmpty()) break;

                for (MapRecord<String, Object, Object> record : records) {
                    if (totalProcessed >= maxProcess) break;

                    Map<Object, Object> value = record.getValue();
                    try {
                        if (!value.containsKey("userId") || !value.containsKey("actionType") || !value.containsKey("timestamp")) {
                            log.warn("필드 누락된 레코드: {}", value);
                            continue;
                        }

                        UserActionLog logEntity = UserActionLog.builder()
                                .userId(Long.parseLong((String) value.get("userId")))
                                .actionType((String) value.get("actionType"))
                                .screen((String) value.get("screen"))
                                .metadata((String) value.get("metadata"))
                                .createAt(Instant.ofEpochMilli(Long.parseLong((String) value.get("timestamp")))
                                        .atZone(ZoneId.of("Asia/Seoul"))
                                        .toLocalDateTime())
                                .build();

                        logsToSave.add(logEntity);
                        ackIds.add(record.getId());
                        delIds.add(record.getId());
                        totalProcessed++;

                    } catch (Exception e) {
                        log.warn("레코드 파싱 오류: {}, {}", value, e.getMessage());
                    }
                }
            }

            if (!logsToSave.isEmpty()) {
                userActionLogRepository.saveAll(logsToSave);
                log.info("총 {}건 로그 저장 완료 ({}시 기준 최대 {})", logsToSave.size(), hour, maxProcess);

                redisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, ackIds.toArray(new RecordId[0]));
                redisTemplate.opsForStream().delete(STREAM_KEY, delIds.toArray(new RecordId[0]));
            } else {
                log.info("처리할 로그 없음 ({}시 기준 최대 {})", hour, maxProcess);
            }

        } catch (Exception e) {
            log.error("배치 로그 소비 중 오류 발생", e);
        } finally {
            stopWatch.stop(); // 측정 종료
            long totalTimeMillis = stopWatch.getTotalTimeMillis();

            if (totalProcessed > 0) {
                double throughput = (double) totalProcessed / totalTimeMillis * 1000;
                log.info("===== 배치 작업 성능 측정 =====");
                log.info("총 처리 건수: {}", totalProcessed);
                log.info("총 소요 시간: {} ms", totalTimeMillis);
                log.info("초당 처리량: {:.2f} records/sec", throughput);
                log.info("============================");
            }
        }
    }

    private int getMaxProcessByHour(int hour) {
        if (hour >= 0 && hour < 6) return 100;
        if (hour == 12) return 1500;
        if (hour >= 20 && hour < 22) return 2000;
        return 500;
    }

}


