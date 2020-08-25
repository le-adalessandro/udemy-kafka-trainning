/**
 *
 */
package com.learnkafka.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;

/**
 * @author Leandro
 *
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}" })
@TestMethodOrder(OrderAnnotation.class)
public class LibraryEventsConsumerIntegrationTest {

	@Autowired
	EmbeddedKafkaBroker embeddedKafkaBroker;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	KafkaListenerEndpointRegistry endpointRegistry;

	@SpyBean
	LibraryEventsConsumer libraryEventsConsumerSpy;

	@SpyBean
	LibraryEventsService libraryEventsServiceSpy;

	@Autowired
	LibraryEventsRepository libraryEventsRepository;

	@Autowired
	ObjectMapper mapper;

	@BeforeEach
	public void setup() {

		for (final MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
		}
	}

	@AfterEach
	public void tearDown() {
		libraryEventsRepository.deleteAll();
	}

	@SuppressWarnings("unchecked")
	@Test
	@Order(1)
	void publishNewLibraryEvent() throws Exception {

		final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka com Spring Boot\",\"bookAuthor\":\"Me!!\"}}";

		kafkaTemplate.sendDefault(json).get();

		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		final List<LibraryEvent> libraryEventsList = (List<LibraryEvent>) libraryEventsRepository.findAll();
		assert libraryEventsList.size() == 1;
		libraryEventsList.forEach(libraryEvent -> {
			assert libraryEvent.getLibraryEventId() != null;
			assertEquals(123, libraryEvent.getBook().getBookId());
		});

	}

	@SuppressWarnings("unchecked")
	@Test
	@Order(2)
	void publishUpdateLibraryEvent() throws Exception {

		final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka com Spring Boot\",\"bookAuthor\":\"Me!!\"}}";
		final LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);

		final Book updatedBook = Book.builder().bookId(123).bookName("Kafka com Spring Boot 2.x").bookAuthor("Me||")
				.build();
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		final String updatedJson = mapper.writeValueAsString(libraryEvent);

		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		final LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
				.get();
		assertEquals("Kafka com Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	@Order(3)
	void publishUpdateLibraryEventWithRandomLibraryEventId() throws Exception {

		final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka com Spring Boot\",\"bookAuthor\":\"Me!!\"}}";
		final LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);

		final Book updatedBook = Book.builder().bookId(123).bookName("Kafka com Spring Boot 2.x").bookAuthor("Me||")
				.build();
		libraryEvent.setLibraryEventId(2000);
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		final String updatedJson = mapper.writeValueAsString(libraryEvent);

		final ConsumerRecord[] consumerRecord = new ConsumerRecord[1];
		final IllegalArgumentException[] assertThrows = new IllegalArgumentException[1];
		Mockito.doAnswer(invocation -> {
			consumerRecord[0] = invocation.getArgument(0);
			assertThrows[0] = assertThrows(IllegalArgumentException.class, () -> {
				invocation.callRealMethod();
			});
			return null;
		}).when(libraryEventsServiceSpy).processLibraryEvent(isA(ConsumerRecord.class));

		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		assertEquals("Not a validLibrary Event", assertThrows[0].getMessage());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	@Order(4)
	void publishUpdateLibraryEventWithNullLibraryEventId() throws Exception {

		final String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka com Spring Boot\",\"bookAuthor\":\"Me!!\"}}";
		final LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);

		final Book updatedBook = Book.builder().bookId(123).bookName("Kafka com Spring Boot 2.x").bookAuthor("Me||")
				.build();
		libraryEvent.setLibraryEventId(null);
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEvent.setBook(updatedBook);
		final String updatedJson = mapper.writeValueAsString(libraryEvent);

		final ConsumerRecord[] consumerRecord = new ConsumerRecord[1];
		final IllegalArgumentException[] assertThrows = new IllegalArgumentException[1];
		Mockito.doAnswer(invocation -> {
			consumerRecord[0] = invocation.getArgument(0);
			assertThrows[0] = assertThrows(IllegalArgumentException.class, () -> {
				invocation.callRealMethod();
			});
			return null;
		}).when(libraryEventsServiceSpy).processLibraryEvent(isA(ConsumerRecord.class));

		kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		assertEquals("Library Event Id is missing", assertThrows[0].getMessage());
	}

	// Solucao do instrutor
	@SuppressWarnings("unchecked")
	@Test
	@Order(5)
	void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		final Integer libraryEventId = 123;
		final String json = "{\"libraryEventId\":" + libraryEventId
				+ ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		System.out.println(json);
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		// when
		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

		final Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
		assertFalse(libraryEventOptional.isPresent());
	}

	@SuppressWarnings("unchecked")
	@Test
	@Order(6)
	void publishModifyLibraryEvent_Null_LibraryEventId()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		final Integer libraryEventId = null;
		final String json = "{\"libraryEventId\":" + libraryEventId
				+ ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		// when
		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	@Order(7)
	void publishModifyLibraryEvent_000_LibraryEventId()
			throws JsonProcessingException, InterruptedException, ExecutionException {
		// given
		final Integer libraryEventId = 000;
		final String json = "{\"libraryEventId\":" + libraryEventId
				+ ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
		kafkaTemplate.sendDefault(libraryEventId, json).get();
		// when
		final CountDownLatch latch = new CountDownLatch(1);
		latch.await(3, TimeUnit.SECONDS);

		verify(libraryEventsConsumerSpy, times(4)).onMessage(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
		verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));
	}
}
