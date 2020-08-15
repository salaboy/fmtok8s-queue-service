package com.salaboy.queue.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salaboy.cloudevents.helper.CloudEventsHelper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.format.EventFormat;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import io.zeebe.cloudevents.ZeebeCloudEventsHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

@SpringBootApplication
@RestController
@Slf4j
public class QueueServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueueServiceApplication.class, args);
    }

    @Value("${ZEEBE_CLOUD_EVENTS_ROUTER:http://zeebe-cloud-events-router}")
    private String ZEEBE_CLOUD_EVENTS_ROUTER;

    @Value("${FRONT_END:http://customer-waiting-room-app.default.svc.cluster.local}")
    private String FRONT_END;


    private ObjectMapper objectMapper = new ObjectMapper();

    private LinkedList<QueueSession> queue = new LinkedList<>();

    @PostConstruct
    public void initQueue() {
        log.info("> Queue Init!");
        new Thread("queue") {
            public void run() {
                while (true) {
                    if (!queue.isEmpty()) {
                        QueueSession session = queue.pop();
                        log.info("You are next: " + session);
                        String data = "";
                        try {
                            data = objectMapper.writeValueAsString("{ \"sessionId\" : \"" + session.getSessionId() + "\" }");
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        log.info("Data going into the Cloud Event Builder ");
                        CloudEventBuilder cloudEventBuilder = CloudEventBuilder.v03()
                                .withId(UUID.randomUUID().toString())
                                .withTime(ZonedDateTime.now())
                                .withType("Queue.CustomerExited")
                                .withSource(URI.create("queue-service.default.svc.cluster.local"))
                                .withData(data.getBytes())
                                .withDataContentType("application/json")
                                .withSubject(session.getSessionId());



                        CloudEvent zeebeCloudEvent = ZeebeCloudEventsHelper
                                .buildZeebeCloudEvent(cloudEventBuilder)
                                .withCorrelationKey(session.getSessionId()).build();



                        logCloudEvent(zeebeCloudEvent);
                        WebClient webClient = WebClient.builder().baseUrl(ZEEBE_CLOUD_EVENTS_ROUTER).filter(logRequest()).build();

                        WebClient.ResponseSpec postCloudEvent = CloudEventsHelper.createPostCloudEvent(webClient, "/message", zeebeCloudEvent);

                        postCloudEvent.bodyToMono(String.class).doOnError(t -> t.printStackTrace())
                                .doOnSuccess(s -> System.out.println("Result -> " + s)).subscribe();


                        webClient = WebClient.builder().baseUrl(FRONT_END).filter(logRequest()).build();

                        postCloudEvent = CloudEventsHelper.createPostCloudEvent(webClient, "/api/", zeebeCloudEvent);

                        postCloudEvent.bodyToMono(String.class).doOnError(t -> t.printStackTrace())
                                .doOnSuccess(s -> System.out.println("Result -> " + s)).subscribe();


                        log.info("Queue Size: " + queue.size());
                    } else {
                        log.info("The Queue is empty!");
                    }
                    try {
                        Thread.sleep(20 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    private void logCloudEvent(CloudEvent cloudEvent) {
        EventFormat format = EventFormatProvider
                .getInstance()
                .resolveFormat(JsonFormat.CONTENT_TYPE);

        log.info("Cloud Event: " + new String(format.serialize(cloudEvent)));

    }


    private static ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            log.info("Request: " + clientRequest.method() + " - " + clientRequest.url());
            clientRequest.headers().forEach((name, values) -> values.forEach(value -> log.info(name + "=" + value)));
            return Mono.just(clientRequest);
        });
    }

    @PostMapping(value = "/join")
    public String joinQueueForTicket(@RequestHeader HttpHeaders headers, @RequestBody Object event) throws IOException {
        CloudEvent cloudEvent = ZeebeCloudEventsHelper.parseZeebeCloudEventFromRequest(headers, event);
        logCloudEvent(cloudEvent);
        if (!cloudEvent.getType().equals("Queue.CustomerJoined")) {
            throw new IllegalStateException("Wrong Cloud Event Type, expected: 'Tickets.CustomerQueueJoined' and got: " + cloudEvent.getType());
        }
        QueueSession session = objectMapper.readValue(new String(cloudEvent.getData()), QueueSession.class);

        if (!alreadyInQueue(session.getSessionId())) {
            log.info("> New Customer in Queue: " + session);

            queue.add(session);
        }
        return session.toString();
    }



    @PostMapping(value = "/abandon")
    public void abandonQueue(@RequestHeader HttpHeaders headers, @RequestBody Object event) throws JsonProcessingException {
        CloudEvent cloudEvent = ZeebeCloudEventsHelper.parseZeebeCloudEventFromRequest(headers, event);
        logCloudEvent(cloudEvent);
        if (!cloudEvent.getType().equals("Queue.CustomerAbandoned")) {
            throw new IllegalStateException("Wrong Cloud Event Type, expected: 'Queue.CustomerAbandoned' and got: " + cloudEvent.getType());
        }
        log.info("> Customer abandoned the Queue: " + event);
        QueueSession session = objectMapper.readValue(new String(cloudEvent.getData()), QueueSession.class);
        if(session != null && queue.contains(session)) {
            queue.remove(session);
            log.info("Session Removed: " + session.getSessionId());
        }else{
            log.info("Session not removed: " +session);
        }

    }




    @GetMapping("/")
    public List<QueueSession> getQueuedSessions() {
        return queue;
    }

    @GetMapping("/{id}")
    public QueuePosition getMyPositionInQueue(@PathVariable("id") String sessionId) {
        Iterator<QueueSession> iterator = queue.iterator();
        int position = 0;
        while (iterator.hasNext()) {
            if (!iterator.next().getSessionId().equals(sessionId)) {
                position++;
            } else {
                return new QueuePosition(position, queue.size(), position * 2);
            }
        }
        return new QueuePosition(-1, queue.size(), queue.size() * 2);
    }

    private boolean alreadyInQueue(String sessionId) {
        Iterator<QueueSession> iterator = queue.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getSessionId().equals(sessionId)) {
                return true;
            }
        }
        return false;

    }


}
