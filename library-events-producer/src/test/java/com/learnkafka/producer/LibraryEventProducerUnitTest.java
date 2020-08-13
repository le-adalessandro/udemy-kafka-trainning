package com.learnkafka.producer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;

@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {

	@Mock
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Spy
	ObjectMapper objectMapper = new ObjectMapper();

	@InjectMocks
	LibraryEventProducer eventProducer;

	@SuppressWarnings("unchecked")
	@Test
	void sendLibraryEventWithTopic_failure() throws Exception {

		final Book book = Book.builder().bookId(123).bookAuthor("Me!!").bookName("Kafka com Spring Boot").build();
		final LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		final SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<>();
		future.setException(new RuntimeException("Exception Calling Kafka"));

		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

		assertThrows(Exception.class, () -> eventProducer.sendLibraryEventWithTopic(libraryEvent).get());
	}

	@Test
	void sendLibraryEventWithTopic_success() throws Exception {

		final TopicPartition topicPartition = new TopicPartition("library-events", 1);
		final RecordMetadata recordMetadata = new RecordMetadata(topicPartition, 1, 1, 342, System.currentTimeMillis(),
				1, 2);

		final Book book = Book.builder().bookId(123).bookAuthor("Me!!").bookName("Kafka com Spring Boot").build();
		final LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		final String record = objectMapper.writeValueAsString(libraryEvent);
		final ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events",
				libraryEvent.getLibraryEventId(), record);

		final SendResult<Integer, String> sendResultMock = new SendResult<>(producerRecord, recordMetadata);
		final SettableListenableFuture<SendResult<Integer, String>> future = new SettableListenableFuture<>();
		future.set(sendResultMock);

		when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

		final ListenableFuture<SendResult<Integer, String>> listenableFuture = eventProducer
				.sendLibraryEventWithTopic(libraryEvent);

		final SendResult<Integer, String> sendResult = listenableFuture.get();

		assert sendResult.getRecordMetadata().partition() == 1;
	}
}
