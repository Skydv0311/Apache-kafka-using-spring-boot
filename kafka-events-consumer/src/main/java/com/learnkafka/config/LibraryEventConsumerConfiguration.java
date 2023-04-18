package com.learnkafka.config;

import com.learnkafka.service.FailureService;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfiguration {

    public static final  String RETRY = "RETRY";
    public static final  String DEAD = "DEAD";
    public static final String SUCCESS = "SUCCESS";

    @Autowired
    KafkaTemplate template;

    @Autowired
    FailureService failureService;
    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;

    public DeadLetterPublishingRecoverer publishingRecoverer(){

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (r, e) -> {
                    if (e.getCause() instanceof RecoverableDataAccessException) {
                        System.out.println("Inside recovery");
                        return new TopicPartition(retryTopic, r.partition());
                    }
                    else {System.out.println("Inside dead letter");
                        return new TopicPartition(dltTopic, r.partition());
                    }
                });

        return recoverer;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (consumerRecord, e) -> {
        log.error("Exception in consumerRecordRecoverer : {} ", e.getMessage(), e);
        var record = (ConsumerRecord<Integer, String>)consumerRecord;
        if (e.getCause() instanceof RecoverableDataAccessException) {
            //recovery logic
            log.info("Inside Recovery");
            failureService.saveFailedRecord(record, e, RETRY);
        }
        else {
            // non-recovery logic
            log.info("Inside Non-Recovery");
            failureService.saveFailedRecord(record, e, DEAD);
        }
    };
    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
//        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    private DefaultErrorHandler errorHandler() {
        var exceptionsToIgnoreList = Arrays.asList(IllegalArgumentException.class);

        var exceptionsToRetryList = Arrays.asList(RecoverableDataAccessException.class);

//        var fixedBackOff = new FixedBackOff(1000L, 2);

        var expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1_000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2_000L);

        var errorHandler = new DefaultErrorHandler(
//                publishingRecoverer(),
                consumerRecordRecoverer,
                // fixedBackOff
                expBackOff
        );

//        exceptionsToIgnoreList.forEach(errorHandler::addNotRetryableExceptions);
        exceptionsToRetryList.forEach(errorHandler::addRetryableExceptions);

        errorHandler
                .setRetryListeners(((record, ex, deliveryAttempt) -> {
                    log.info("Failed Record in Retry Listener, Exception : {} , deliveryAttempt : {} "
                            ,ex.getMessage(), deliveryAttempt);
                }));

        return  errorHandler;
    }

}
