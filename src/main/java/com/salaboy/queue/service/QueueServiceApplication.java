package com.salaboy.queue.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salaboy.cloudevents.helper.CloudEventsHelper;
import io.cloudevents.CloudEvent;
import io.cloudevents.v03.CloudEventBuilder;
import io.zeebe.cloudevents.*;
import io.cloudevents.json.Json;
import io.cloudevents.v03.AttributesImpl;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@SpringBootApplication
@RestController
@Slf4j
public class QueueServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueueServiceApplication.class, args);
    }

    @Value("${ZEEBE_CLOUD_EVENTS_ROUTER:http://zeebe-cloud-events-router}")
    private String ZEEBE_CLOUD_EVENTS_ROUTER;

    @Value("${FRONT_END:http://customer-waiting-room-app}")
    private String FRONT_END;

    @Value("${EVENTS_SINK:http://localhost:8080}")
    private String EVENT_SINK;

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

                        CloudEventBuilder<String> cloudEventBuilder = CloudEventBuilder.<String>builder()
                                .withId(UUID.randomUUID().toString())
                                .withTime(ZonedDateTime.now())
                                .withType("Queue.CustomerExited")
                                .withSource(URI.create("queue.service.default"))
                                .withData("{\"sessionId\" : " + "\"" + session.getSessionId() + "\"}")
                                .withDatacontenttype("application/json")
                                .withSubject(session.getSessionId());


                        CloudEvent<AttributesImpl, String> zeebeCloudEvent = ZeebeCloudEventsHelper
                                .buildZeebeCloudEvent(cloudEventBuilder)
                                .withCorrelationKey(session.getSessionId()).build();

                        String cloudEventJson = Json.encode(zeebeCloudEvent);
                        log.info("Before sending Cloud Event: " + cloudEventJson);
                        WebClient webClient = WebClient.builder().baseUrl(ZEEBE_CLOUD_EVENTS_ROUTER).filter(logRequest()).build();

                        WebClient.ResponseSpec postCloudEvent = CloudEventsHelper.createPostCloudEvent(webClient, "/message", zeebeCloudEvent);

                        postCloudEvent.bodyToMono(String.class).doOnError(t -> t.printStackTrace())
                                .doOnSuccess(s -> System.out.println("Result -> " + s)).subscribe();


                        webClient = WebClient.builder().baseUrl(FRONT_END).filter(logRequest()).build();

                        postCloudEvent = CloudEventsHelper.createPostCloudEvent(webClient, "/api/", zeebeCloudEvent);

                        postCloudEvent.bodyToMono(String.class).doOnError(t -> t.printStackTrace())
                                .doOnSuccess(s -> System.out.println("Result -> " + s)).subscribe();



                        log.info("Queue Size: " + queue.size());
                    }else{
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


    private static ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(clientRequest -> {
            log.info("Request: " + clientRequest.method() + " - " + clientRequest.url());
            clientRequest.headers().forEach((name, values) -> values.forEach(value -> log.info(name + "=" + value)));
            return Mono.just(clientRequest);
        });
    }

    @PostMapping(value = "/join")
    public String joinQueueForTicket(@RequestHeader Map<String, String> headers, @RequestBody Object event) throws JsonProcessingException {
        log.info(event.toString());
        CloudEvent<AttributesImpl, String> cloudEvent = ZeebeCloudEventsHelper.parseZeebeCloudEventFromRequest(headers, event);
        if(!cloudEvent.getAttributes().getType().equals("Queue.CustomerJoined")){
            throw new IllegalStateException("Wrong Cloud Event Type, expected: 'Tickets.CustomerQueueJoined' and got: " + cloudEvent.getAttributes().getType() );
        }
        QueueSession session = objectMapper.readValue(cloudEvent.getData().get(), QueueSession.class);

        if(!alreadyInQueue(session.getSessionId())) {
            session.setClientId(UUID.randomUUID().toString());
            log.info("> New Customer in Queue: " + session);

            queue.add(session);
        }
        return session.toString();
    }

    @PostMapping(value = "/exit", consumes = MediaType.APPLICATION_JSON_VALUE)
    public void exitQueue(@RequestHeader Map<String, String> headers, @RequestBody Object event) {
        log.info(event.toString());
        CloudEvent<AttributesImpl, String> cloudEvent = ZeebeCloudEventsHelper.parseZeebeCloudEventFromRequest(headers, event);
        if(!cloudEvent.getAttributes().getType().equals("Queue.CustomerExited")){
            throw new IllegalStateException("Wrong Cloud Event Type, expected: 'Tickets.CustomerQueueExited' and got: " + cloudEvent.getAttributes().getType() );
        }
        log.info("> Customer exited the Queue: " + event);
        queue.remove(event);
    }


    @GetMapping("/")
    public int getQueueSize() {
        return queue.size();
    }

    @GetMapping("/{id}")
    public int getMyPositionInQueue(@PathVariable("id") String sessionId) {
        Iterator<QueueSession> iterator = queue.iterator();
        int position = 0;
        while(iterator.hasNext()){
            if(!iterator.next().getSessionId().equals(sessionId)){
                position++;
            }else{
                return position;
            }
        }
        return -1;
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