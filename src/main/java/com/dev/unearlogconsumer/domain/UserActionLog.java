package com.dev.unearlogconsumer.domain;


import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "user_action_logs")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@SequenceGenerator(
        name = "USER_ACTION_LOG_SEQ_GENERATOR",
        sequenceName = "USER_ACTION_LOG_SEQ",
        initialValue = 1,
        allocationSize = 50
)
public class UserActionLog {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE,
            generator = "USER_ACTION_LOG_SEQ_GENERATOR")
    private Long userActionLogId;

    private Long userId;

    private String actionType;

    private String screen;

    @Column(columnDefinition = "TEXT")
    private String metadata;

    private LocalDateTime createAt;
}