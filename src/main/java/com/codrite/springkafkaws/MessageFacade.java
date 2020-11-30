package com.codrite.springkafkaws;

import datadog.trace.api.Trace;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

    private KafkaTemplate<String, String> kafkaTemplate;
    private ConcurrentMap<String, String> map;

    @Autowired
    public MessageFacade(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.map = new ConcurrentHashMap<>();
    }

    @KafkaListener(topics = {"Test"})
    @Trace(operationName = "Listen")
    public void listen(ConsumerRecord<String, String> record) {
        Span span = GlobalTracer.get().activeSpan();
        try {
            span.setTag("UUID", record.key());
            map.put(record.key(), record.value());
        } finally {
            span.finish();
        }
    }

    @Trace(operationName = "Publish")
    public void publish(String uuid, String ts) {
        Span span = GlobalTracer.get().activeSpan();
        try {
            span.setTag("UUID", uuid);
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
        } finally {
            span.finish();
        }
    }

    @Trace(operationName = "Consume")
    public String consume(String uuid) {
        Span span = GlobalTracer.get().activeSpan();
        try {
            span.setTag("UUID", uuid);
            return Optional.ofNullable(map.remove(uuid)).orElse("No Value Found");
        } finally {
            span.finish();
        }
    }

}
