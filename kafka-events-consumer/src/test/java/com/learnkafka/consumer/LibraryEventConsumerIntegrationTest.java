package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.repository.LibraryEventRepository;
import com.learnkafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @SpyBean
    LibraryEventConsumer libraryEventConsumerSpy;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for(MessageListenerContainer messageListenerContainer: kafkaListenerEndpointRegistry.getAllListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer,embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishThePostLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = " {\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch=new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,times(1)).saveLibraryEventDetails(isA(ConsumerRecord.class));

        List<LibraryEvent> libraryEventsList= (List<LibraryEvent>) libraryEventRepository.findAll();

        assert libraryEventsList.size()==1;
        libraryEventsList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId()!=null;
            assertEquals(456,libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishTheUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent object = objectMapper.readValue(json, LibraryEvent.class);
        object.getBook().setLibraryEvent(object);
        libraryEventRepository.save(object);

        Book book=Book.builder()
                .bookId(456).bookName("Kafka using spring boot2.x")
                .bookAuthor("shubham")
                .build();
        object.setBook(book);
        object.setLibraryEventType(LibraryEventType.UPDATE);
        String updatedRecord=objectMapper.writeValueAsString(object);

        kafkaTemplate.sendDefault(updatedRecord).get();

        CountDownLatch countDownLatch=new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,times(1)).saveLibraryEventDetails(isA(ConsumerRecord.class));

        LibraryEvent persistedlibraryEvents = libraryEventRepository.findById(object.getLibraryEventId()).get();
        assertEquals("Kafka using spring boot2.x",persistedlibraryEvents.getBook().getBookName());
    }
}
