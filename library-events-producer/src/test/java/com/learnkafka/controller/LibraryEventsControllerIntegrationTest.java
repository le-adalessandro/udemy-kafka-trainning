package com.learnkafka.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = { "library-events" }, partitions = 3)
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
"spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" })
public class LibraryEventsControllerIntegrationTest {

	@Autowired
	TestRestTemplate restTemplate;

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	private Consumer<Integer, String> consumer;

	@BeforeEach
	public void setup() {
		final Map<String, Object> configs = new HashMap<>(
				KafkaTestUtils.consumerProps("group1", "true", embeddedKafka));
		consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
				.createConsumer();
		embeddedKafka.consumeFromAllEmbeddedTopics(consumer);
	}

	@AfterEach
	public void tearDown() {
		consumer.close();
	}

	@Test
	@Timeout(5) // wait for 5s and fails if run out of time
	void postLibraryEvent() throws InterruptedException {

		final Book book = Book.builder().bookId(123).bookAuthor("Me!!").bookName("Kafka com Spring Boot").build();

		final LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		final HttpHeaders headers = new HttpHeaders();
		headers.set("content-type", MediaType.APPLICATION_JSON.toString());

		final HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent, headers);

		final ResponseEntity<LibraryEvent> responseEntity = restTemplate.exchange("/v1/libraryevent",
				HttpMethod.POST,
				request, LibraryEvent.class);

		assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

		final ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer,
				"library-events");
		Thread.sleep(3000); // wait for the message

		final String expectedRecord = "{\"libraryEvntId\":null,\"book\":{\"bookId\":123,\"bookName\":\"Kafka com Spring Boot\",\"bookAuthor\":\"Me!!\"},\"libraryEventType\":\"NEW\"}";
		final String value = consumerRecord.value();
		assertEquals(expectedRecord, value);
	}

}
