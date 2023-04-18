package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    public void saveLibraryEventDetails(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent=objectMapper.readValue(consumerRecord.value(),LibraryEvent.class);
        log.info("libraryEvent: {} "+libraryEvent);

        if(libraryEvent!=null && (libraryEvent.getLibraryEventId()!=null && libraryEvent.getLibraryEventId() == 999)){
            System.out.println("Inside exception");
            throw new RecoverableDataAccessException("Temporary Network Issue");
        }
        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                save(libraryEvent);
                break;
            case UPDATE:
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("Invalid Record");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }
        Optional<LibraryEvent> OptionalLibraryEvent = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
        if(!OptionalLibraryEvent.isPresent()){
            throw new IllegalArgumentException("Event Id is Invalid");
        }
        log.info("Successfully Validated the libraryEvent");
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);
        log.info("successfully persisted the library event {} ",libraryEvent);
    }
}
