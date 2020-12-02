package com.codrite.springkafkaws;

import org.apache.skywalking.apm.toolkit.trace.ActiveSpan;
import org.apache.skywalking.apm.toolkit.trace.Tag;
import org.apache.skywalking.apm.toolkit.trace.Trace;
import org.apache.skywalking.apm.toolkit.trace.TraceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/message")
public class WebService {

    private final MessageFacade messageFacade;

    @Autowired
    public WebService(MessageFacade messageFacade) {
        this.messageFacade = messageFacade;
    }

    @PostMapping
    @Trace(operationName = "WebServiceCreate")
    public String create() {
        String uuid = UUID.randomUUID().toString();
        String ts = System.currentTimeMillis() + "";
        TraceContext.putCorrelation("UUID", uuid);
        messageFacade.publish(uuid, ts);
        return uuid;
    }

    @GetMapping("/{uuid}")
    @Trace(operationName = "WebServiceGet")
    @Tag(key = "UUID", value = "arg[0]")
    public String get(@PathVariable("uuid") String uuid) {
        TraceContext.putCorrelation("UUID", uuid);
        return messageFacade.consume(uuid);
    }

}
