package com.codrite.springkafkaws;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.Trace;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Component
@EnableKafka
public class MessageFacade {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConcurrentMap<String, String> map;

    @Autowired
    public MessageFacade(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.map = new ConcurrentHashMap<>();
    }

    @KafkaListener(topics = {"Test"})
    @Trace(operationName = "Listen")
    public void listen(ConsumerRecord<String, String> record) {
        TraceContext.putCorrelation("UUID", record.key());
        map.put(record.key(), record.value());
    }

    @Trace(operationName = "Publish")
    public void publish(String uuid, String ts) {
        TraceContext.putCorrelation("UUID", uuid);
        ListenableFuture<SendResult<String, String>> result = kafkaTemplate.send("Test", uuid, ts);
        result.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable throwable) {
                log.error(throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> onSuccessMessage) {
                log.info(onSuccessMessage.getProducerRecord().key());
            }
        });

    }

    @Trace(operationName = "Consume")
    public String consume(String uuid) {
        TraceContext.putCorrelation("UUID", uuid);
        return Optional.ofNullable(map.remove(uuid)).orElse("No Value Found");
    }

}
