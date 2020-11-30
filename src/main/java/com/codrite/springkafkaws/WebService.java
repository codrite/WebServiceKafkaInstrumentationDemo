package com.codrite.springkafkaws;

import datadog.trace.api.Trace;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/message")
public class WebService {

    private MessageFacade messageFacade;

    @Autowired
    public WebService(MessageFacade messageFacade) {
        this.messageFacade = messageFacade;
    }

    @PostMapping
    @Trace(operationName = "WebServiceCreate")
    public String create() {
        Span span = GlobalTracer.get().activeSpan();
        try {
            String uuid = UUID.randomUUID().toString();
            String ts = System.currentTimeMillis() + "";
            span.setTag("UUID", uuid);
            messageFacade.publish(uuid, ts);
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
            return messageFacade.consume(uuid);
        } finally {
            span.finish();
        }
    }

}
