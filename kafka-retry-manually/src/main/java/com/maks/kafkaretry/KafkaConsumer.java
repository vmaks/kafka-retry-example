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

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final ThreadPoolTaskScheduler threadPoolTaskScheduler;

    private final Map<Integer, Long> retries = Map.of(1, 1000L, 2, 2000L, 3, 4000L);

    @KafkaListener(topics = "example-events", containerFactory = "kafkaListenerContainerFactory", id = "mainListener")
    public void precessMessage(@Payload String message,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.GROUP_ID) String groupId,
                               Acknowledgment ack) {
        log.info("Processing started " + message);
        try {
            callRestService();
        } catch (Exception ex) {
            log.error("Processing started " + message);
            Integer nextRetry = 1;
            String destinationTopic = calculateTopic(topic, ex, nextRetry);
            sendToRetry(message, ex, destinationTopic, String.valueOf(nextRetry));
        }
        ack.acknowledge();
        log.info("Processing finished " + message);
    }


    @KafkaListener(topics = "example-events.retry", containerFactory = "kafkaListenerContainerFactory", id = "retryListener")
    public void precessMessageRetry(@Payload String message,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                    @Header(KafkaHeaders.GROUP_ID) String groupId,
                                    @Header("retry") byte[] retryBytes,
                                    Acknowledgment ack) {
        log.info("Processing started " + message);
        int retry = Integer.parseInt(new String(retryBytes, StandardCharsets.UTF_8));
        try {
            Long delayMillis = retries.get(retry);
            if (delayMillis != null) {
                sleepConsumer(delayMillis);
            }
            callRestService();
        } catch (Exception ex) {
            log.error("Processing started " + message);
            Integer nextRetry = retry + 1;
            String destinationTopic = calculateTopic(topic, ex, nextRetry);
            sendToRetry(message, ex, destinationTopic, String.valueOf(nextRetry));
        }
        ack.acknowledge();
        log.info("Processing finished " + message);
    }

    @KafkaListener(topics = "example-events.dlt", containerFactory = "kafkaListenerContainerFactory", id = "dltListener")
    public void precessMessageRetry(@Payload String message,
                                    Acknowledgment ack) {
        log.info("Processing started " + message);
        ack.acknowledge();
    }

    private void callRestService() {
        throw new RestClientException("error");
    }

    private void sleepConsumer(Long delayMillis) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer("retryListener");
        listenerContainer.stop();
        Instant retryAt = Instant.now().plus(delayMillis, ChronoUnit.MILLIS);
        threadPoolTaskScheduler.schedule(listenerContainer::start, retryAt);
    }

    private String calculateTopic(String topic, Exception ex, Integer nextRetry) {
        String destinationTopic;
        if ((nextRetry > 2) || isFatalException(ex)) {
            // TODO:
            destinationTopic = "example-events.dlt";
            log.error("DLT sending due to exception to topic = {}", destinationTopic);
        } else {
            destinationTopic = "example-events.retry";
            log.info("RETRY sending due to exception to topic = {}", destinationTopic);
        }
        return destinationTopic;
    }

    // TODO:
    private boolean isFatalException(Exception ex) {
        return false;
    }

    private void sendToRetry(String message, Exception ex, String destinationTopic, String nextRetry) {
        ProducerRecord<String, String> record = new ProducerRecord<>(destinationTopic, message);
        record.headers().add("retry", nextRetry.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }
}
