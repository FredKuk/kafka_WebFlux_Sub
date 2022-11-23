package com.kafka.reactSub.service;

import java.util.concurrent.CountDownLatch;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.core.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@RequiredArgsConstructor
public class KafkaServiceImpl implements KafkaService{
    
    private static final Logger log = LoggerFactory.getLogger(KafkaServiceImpl.class.getName());
    private final ReceiverOptions<String, Object> receiverOptions;

    @Override
    public Disposable consume(CountDownLatch latch){
        Flux<ReceiverRecord<String, Object>> kafkaFlux = KafkaReceiver.create(receiverOptions).receive()
                    .doOnError(e -> {
                        log.debug("############     Kafka read error    ##################");
                        consume(latch);     // 에러 발생 시, consumer가 종료되고 재시작할 방법이 없기 때문에 error시 재시작
                    });
        return kafkaFlux
        .onErrorResume(e -> {
            log.debug("############     onErrorResume   ##################");
            return Flux.empty();
        })
        .doOnCancel(() -> {
            log.debug("###########     Disconnected By Client   ##########");
        })   // client 종료 시, ping으로 인지하고 cancel signal을 받음;
        .subscribe(record -> {
            ReceiverOffset offset = record.receiverOffset();
            System.out.printf("#############    Received message : topic-partition= %s  offset= %d  key= %s  value= %s   ##############",
                    offset.topicPartition(),
                    offset.offset(),
                    record.key(),
                    record.value());
            offset.acknowledge();
            latch.countDown();
        });
    }
}
