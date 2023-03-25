package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class LibraryEventProducer {

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    String topic="library-events";

    public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key=libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);

        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key,value,ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });
        /**
         * Spring boot 3
         * // Use the KafkaTemplate to send a message using the sendDefault() method
         * CompletableFuture<SendResult<String, String>> completableFuture = kafkaTemplate.sendDefault("Hello, World!");
         *
         * // Add a callback to the CompletableFuture to print the response when the future is complete
         * completableFuture.thenAccept(result -> {
         *     System.out.println("Message sent successfully! Offset: " + result.getRecordMetadata().offset());
         * }).exceptionally(ex -> {
         *     System.err.println("Error sending message: " + ex.getMessage());
         *     return null;
         * });
         */
    }

    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException {

        Integer key=libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> sendResult;
        try {
            sendResult = kafkaTemplate.sendDefault(key, value).get();
        } catch (InterruptedException|ExecutionException e) {
            log.error("InterruptedException/ExecutionException sending message and exception is {}",e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Exception sending message and exception is {}",e.getMessage());
            throw e;
        }
        return sendResult;

    }

    public ListenableFuture<SendResult<Integer, String>> sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key=libraryEvent.getLibraryEventId();
        String value=objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer, String> producerRecord = buildProdcerRecord(key, value, topic);
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key,value,ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key,value,result);
            }
        });
        return listenableFuture;
    }

    private ProducerRecord<Integer,String> buildProdcerRecord(Integer key, String value, String topic) {
        List<Header> recordHeaders = Arrays.asList(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending message and exception is {}",ex.getMessage());
        try {
            throw ex;
        } catch (Throwable e) {
            log.error("Error is on failure: {}",e);
        }
    }


    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent Successfully for the key: {} and the value: {} , partition is: {} ",key,value,result.getRecordMetadata().partition());
    }
}
