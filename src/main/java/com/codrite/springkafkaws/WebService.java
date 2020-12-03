package com.codrite.springkafkaws;

import com.timgroup.statsd.StatsDClient;
import datadog.trace.api.Trace;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/message")
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
            String ts = System.currentTimeMillis() + "";
            span.setTag("UUID", uuid);
            messageFacade.publish(uuid, ts);
            long exec = Duration.between(LocalDateTime.now(), start).toNanos();
            statsDClient.recordExecutionTime("newRequestTimeToCreate", exec);
            statsDClient.incrementCounter("newRequest");
            return uuid;
        } finally {
            span.finish();
        }

    }

    @GetMapping("/{uuid}")
    @Trace(operationName = "WebServiceGet")
    public String get(@PathVariable("uuid") String uuid) {
        Span span = GlobalTracer.get().activeSpan();
        try {
            span.setTag("UUID", uuid);
            statsDClient.decrementCounter("newRequest");
            return messageFacade.consume(uuid);
        } finally {
            span.finish();
        }
    }

}
