package com.kafka.reactSub.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.web.bind.annotation.*;

import com.kafka.reactSub.service.KafkaService;

import lombok.RequiredArgsConstructor;
import reactor.core.Disposable;

@RestController
@RequestMapping("/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaService kafkaService;

    @GetMapping
    public void consumeMessage(){
        int count = 20;
        CountDownLatch latch = new CountDownLatch(count);
        Disposable disposable = kafkaService.consume(latch);
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        disposable.dispose();
    }
}