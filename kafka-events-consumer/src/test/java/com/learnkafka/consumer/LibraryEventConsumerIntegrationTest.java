package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.repository.FailureRecordRepository;
import com.learnkafka.repository.LibraryEventRepository;
import com.learnkafka.service.LibraryEventService;
import lombok.var;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events","library-events.RETRY","library-events.DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
        , "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}","retryListener.startup:false"})
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

    private Consumer<Integer, String> consumer;

    @Autowired
    private FailureRecordRepository failureRecordRepository;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String dltTopic;

    @BeforeEach
    void setUp() {
        var container= kafkaListenerEndpointRegistry.getAllListenerContainers().stream().filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(),"library-events-listener-group"))
                .collect(Collectors.toList());
        ContainerTestUtils.waitForAssignment(container.get(0),embeddedKafkaBroker.getPartitionsPerTopic());

//        for(MessageListenerContainer messageListenerContainer: kafkaListenerEndpointRegistry.getAllListenerContainers()){
//            ContainerTestUtils.waitForAssignment(messageListenerContainer,embeddedKafkaBroker.getPartitionsPerTopic());
//        }
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void publishThePostLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = " {\"libraryEventId\":23,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
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

    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).saveLibraryEventDetails(isA(ConsumerRecord.class));

        Map<String,Object> configs=new HashMap<>(KafkaTestUtils.consumerProps("group2","true",embeddedKafkaBroker));
        consumer=new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(),new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer,dltTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, dltTopic);

        System.out.println("consumerRecord is : "+consumerRecord);

        assertEquals(json,consumerRecord.value());
    }

    @Test
    void publishUpdateLibraryEvent_999_LibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given

        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(3)).saveLibraryEventDetails(isA(ConsumerRecord.class));

        Map<String,Object> configs=new HashMap<>(KafkaTestUtils.consumerProps("group1","true",embeddedKafkaBroker));
        consumer=new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(),new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer,retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);

        System.out.println("consumerRecord is : "+consumerRecord);

        assertEquals(json,consumerRecord.value());

    }


    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent_failurerecord() throws JsonProcessingException, ExecutionException, InterruptedException {
        //given

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(json).get();

        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        //then
        verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).saveLibraryEventDetails(isA(ConsumerRecord.class));

        var count  = failureRecordRepository.count();
        assertEquals(1, count);

        failureRecordRepository.findAll()
                .forEach(failureRecord -> {
                    System.out.println("failureRecord : " + failureRecord);
                });

    }


}
