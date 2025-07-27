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
public class UserActionLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long userActionLogId;

    private Long userId;

    private String actionType;

    private String screen;

    @Column(columnDefinition = "TEXT")
    private String metadata;

    private LocalDateTime createAt;
}