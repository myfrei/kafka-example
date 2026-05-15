package com.kafka.demo.case1.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Case1ProducerApplication {
    public static void main(String[] args) {
        SpringApplication.run(Case1ProducerApplication.class, args);
    }
}
