package com.maks.kafkaretry;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumer {

    public static final String MAIN_LISTENER = "mainListener";
    public static final String RETRY_LISTENER_1 = "retryListener-1";
    public static final String RETRY_LISTENER_2 = "retryListener-2";
    public static final String DLT_LISTENER = "dltListener";
    public static final String EXAMPLE_EVENTS_TOPIC = "example-events";
    public static final String EXAMPLE_EVENTS_RETRY_1_TOPIC = "example-events.retry-1";
    public static final String EXAMPLE_EVENTS_RETRY_2_TOPIC = "example-events.retry-2";
    public static final String EXAMPLE_EVENTS_DLT_TOPIC = "example-events.dlt";
    public static final String KAFKA_LISTENER_CONTAINER_FACTORY = "kafkaListenerContainerFactory";
    public static final int NACK_TIME = 1000;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;

    private final Map<Integer, Long> retries = Map.of(1, 1000L, 2, 2000L, 3, 4000L);

    @KafkaListener(
            topics = EXAMPLE_EVENTS_TOPIC,
            containerFactory = KAFKA_LISTENER_CONTAINER_FACTORY,
            id = MAIN_LISTENER
    )
    public void precessMessage(@Payload String message,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.GROUP_ID) String groupId,
                               Acknowledgment ack) {
        log.info("Processing started " + message);
        try {
            callRestService();
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Processing started " + message);
            Integer nextRetry = 1;
            String destinationTopic = calculateTopic(topic, ex, nextRetry);
            sendToRetry(message, ex, destinationTopic, String.valueOf(nextRetry));
        }
        log.info("Processing finished " + message);
    }


    @KafkaListener(
            topics = EXAMPLE_EVENTS_RETRY_1_TOPIC,
            containerFactory = KAFKA_LISTENER_CONTAINER_FACTORY,
            id = RETRY_LISTENER_1
    )
    public void precessMessageRetry1(@Payload String message,
                                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                     @Header(KafkaHeaders.GROUP_ID) String groupId,
                                     @Header("retry") byte[] retryBytes,
                                     Acknowledgment ack) {
        precessMessageRetry(message, topic, retryBytes, ack, RETRY_LISTENER_1);
    }

    @KafkaListener(
            topics = EXAMPLE_EVENTS_RETRY_2_TOPIC,
            containerFactory = KAFKA_LISTENER_CONTAINER_FACTORY,
            id = RETRY_LISTENER_2
    )
    public void precessMessageRetry2(@Payload String message,
                                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                     @Header(KafkaHeaders.GROUP_ID) String groupId,
                                     @Header("retry") byte[] retryBytes,
                                     Acknowledgment ack
    ) {
        precessMessageRetry(message, topic, retryBytes, ack, RETRY_LISTENER_2);
    }

    private void precessMessageRetry(String message,
                                     String topic,
                                     byte[] retryBytes,
                                     Acknowledgment ack,
                                     String retryListener1
    ) {
        log.info("Processing started " + message);
        int retry = Integer.parseInt(new String(retryBytes, StandardCharsets.UTF_8));
        try {
            Long delayMillis = retries.get(retry);
            if (delayMillis != null) {
                ack.nack(NACK_TIME);
                sleepConsumer(delayMillis, retryListener1);
                return;
            }
            callRestService();
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Processing started " + message);
            Integer nextRetry = retry + 1;
            String destinationTopic = calculateTopic(topic, ex, nextRetry);
            sendToRetry(message, ex, destinationTopic, String.valueOf(nextRetry));
        }
        log.info("Processing finished " + message);
    }

    @KafkaListener(
            topics = EXAMPLE_EVENTS_DLT_TOPIC,
            containerFactory = KAFKA_LISTENER_CONTAINER_FACTORY,
            id = DLT_LISTENER
    )
    public void precessMessageRetryDlt(@Payload String message,
                                       Acknowledgment ack) {
        log.info("Processing started " + message);
        ack.acknowledge();
    }

    private void callRestService() {
        throw new RestClientException("error");
    }

    private void sleepConsumer(Long delayMillis, String retryListener) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(retryListener);
        if (Objects.nonNull(listenerContainer)) {
            listenerContainer.pause();
            Instant retryAt = Instant.now().plus(delayMillis, ChronoUnit.MILLIS);
            threadPoolTaskScheduler.schedule(listenerContainer::resume, retryAt);
        }
    }

    private String calculateTopic(String topic, Exception ex, Integer nextRetry) {
        String destinationTopic;
        if (nextRetry == 1 && !isFatalException(ex)) {
            destinationTopic = "example-events.retry-1";
            log.info("RETRY sending due to exception to topic = {}", destinationTopic);
        } else if (nextRetry == 2 && !isFatalException(ex)) {
            destinationTopic = "example-events.retry-2";
            log.info("RETRY sending due to exception to topic = {}", destinationTopic);
        } else {
            destinationTopic = "example-events.dlt";
            log.error("DLT sending due to exception to topic = {}", destinationTopic);
        }
        return destinationTopic;
    }

    // TODO: change if you don't want to retry in case if some errors
    private boolean isFatalException(Exception ex) {
        return false;
    }

    private void sendToRetry(String message, Exception ex, String destinationTopic, String nextRetry) {
        ProducerRecord<String, String> record = new ProducerRecord<>(destinationTopic, message);
        record.headers().add("retry", nextRetry.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }
}
