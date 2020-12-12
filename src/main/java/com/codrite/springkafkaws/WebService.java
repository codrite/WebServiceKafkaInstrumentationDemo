package com.codrite.springkafkaws;

import com.timgroup.statsd.Event;
import com.timgroup.statsd.StatsDClient;
import datadog.trace.api.Trace;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/message")
@Slf4j
public class WebService {

    private final MessageFacade messageFacade;
    private final StatsDClient statsDClient;

    @Autowired
    public WebService(MessageFacade messageFacade, StatsDClient statsDClient) {
        this.messageFacade = messageFacade;
        this.statsDClient = statsDClient;
    }

    @PostMapping
    @Trace(operationName = "WebServiceCreate")
    public String create() {
        LocalDateTime start = LocalDateTime.now();
        Span span = GlobalTracer.get().activeSpan();
        try {
            String uuid = UUID.randomUUID().toString();
            statsDClient.recordEvent(createNewUUIDEvent(uuid));
            String ts = System.currentTimeMillis() + "";
            span.setTag("UUID", uuid);
            messageFacade.publish(uuid, ts);
            long exec = Duration.between(start, LocalDateTime.now()).toMillis();
            log.info("Time taken to process : {}", exec);
            statsDClient.recordExecutionTime("newRequestTimeToCreate", exec);
            statsDClient.gauge("newRequestTimeToCreateGauge", exec);
            statsDClient.incrementCounter("newRequest");
            return uuid;
        } finally {
            span.finish();
        }

    }

    Event createNewUUIDEvent(String uuid) {
        return Event.builder()
                    .withAlertType(Event.AlertType.INFO)
                    .withText("Received " + uuid + " on " + LocalDateTime.now())
                    .withTitle("UUID Tracking")
                    .build();
    }

    Event createNewEvent(String message) {
        return Event.builder()
                    .withAlertType(Event.AlertType.INFO)
                    .withText(message)
                    .withTitle("Consumption Usage")
                    .build();
    }

    @GetMapping("/{uuid}")
    @Trace(operationName = "WebServiceGet")
    public String get(@PathVariable("uuid") String uuid) {
        Span span = GlobalTracer.get().activeSpan();
        statsDClient.recordEvent(createNewEvent("Web service consumption tracking"));
        try {
            span.setTag("UUID", uuid);
            statsDClient.decrementCounter("newRequest");
            track();
            return messageFacade.consume(uuid);
        } finally {
            span.finish();
        }
    }

    SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
    Counter counter = Counter.builder("Invocations")
                             .register(simpleMeterRegistry);
    public String track() {
        counter.increment();
        return ""+counter.count();
    }

}
