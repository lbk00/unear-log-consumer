package com.dev.unearlogconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class UnearLogConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(UnearLogConsumerApplication.class, args);
    }

}
