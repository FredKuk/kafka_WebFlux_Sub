package com.kafka.reactSub.service;

import reactor.core.Disposable;
import java.util.concurrent.CountDownLatch;

public interface KafkaService {

    public Disposable consume(CountDownLatch latch);
}
