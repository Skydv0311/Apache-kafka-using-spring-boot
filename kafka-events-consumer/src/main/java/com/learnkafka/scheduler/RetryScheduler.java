package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventConsumerConfiguration;
import com.learnkafka.domain.FailureRecord;
import com.learnkafka.repository.FailureRecordRepository;
import com.learnkafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetryScheduler {

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @Autowired
    LibraryEventService libraryEventService;

    @Scheduled(fixedRate = 10000)
    public  void retryFailedRecord(){
        log.info("Retrying Failed Record started");
        failureRecordRepository.findAllByStatus(LibraryEventConsumerConfiguration.RETRY)
                .forEach(failureRecord -> {
                    log.info("Retry Failed Record: {}",failureRecord);
                    var consumerRecord=buildConsumerRecord(failureRecord);
                    try {
                        libraryEventService.saveLibraryEventDetails(consumerRecord);
                        failureRecord.setStatus(LibraryEventConsumerConfiguration.SUCCESS);
                    } catch (Exception e) {
                        log.error("Error in retryFailedRecord: {}",e.getMessage(),e);
                    }
                });
    }

    private ConsumerRecord<Integer,String> buildConsumerRecord(FailureRecord failureRecord) {
        return  new ConsumerRecord<>(failureRecord.getTopic(),
                failureRecord.getPartition(),
                failureRecord.getOffset_value(),
                failureRecord.getDkey(),
                failureRecord.getErrorRecord());
    }
}
