package com.kafka.producer.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/producer")
public class KafkaPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping
    public void publishData(@RequestParam String data) {
        // code logic
        CompletableFuture<SendResult<String, String>> orderCreation = kafkaTemplate.send("order_creation", data);
        orderCreation.whenCompleteAsync((result, ex) -> {
            if (ex == null) {
                log.info("Data Published SUccessfully to kafka : {}", result.getProducerRecord().value());
            } else {
                log.error("Error while publishing data : ", ex);
            }
        });
    }

}
