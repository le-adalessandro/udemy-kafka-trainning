/**
 *
 */
package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Leandro
 *
 */
@Component
@Slf4j
public class LibraryEventProducer {

	private static final String TOPIC = "library-events";

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private ObjectMapper objectMapper;

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {

		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);
		final ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}
		});
	}

	// sendLibraryEvent_Approach2
	public ListenableFuture<SendResult<Integer, String>> sendLibraryEventWithTopic(LibraryEvent libraryEvent)
			throws JsonProcessingException {

		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);

		final ProducerRecord<Integer, String> producerRecord =  buildProducerRecord(key, value, TOPIC);

		final ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);
			}
		});

		return listenableFuture;
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic2) {

		final List<Header> recordHheaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(TOPIC, null, key, value, recordHheaders);
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent) throws Exception {

		final Integer key = libraryEvent.getLibraryEventId();
		final String value = objectMapper.writeValueAsString(libraryEvent);

		SendResult<Integer,String> sendResult ;

		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get();

		} catch (InterruptedException | ExecutionException e) {
			log.error("Error sending the message and the exception is: {}", e.getMessage());
			throw e;
		} catch (final Exception e) {
			log.error("Error sending the message and the exception is: {}", e.getMessage());
			throw e;
		}

		return sendResult;
	}

	private void handleFailure(Integer key, String value, Throwable ex) {

		log.error("Error sending the message and the exception is: {}", ex.getMessage());

		try {
			throw ex;
		} catch (final Throwable throwable) {
			log.error("Error in onFailure: {}", throwable.getMessage());
		}
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

		log.info("Message sent successfully for the key: {} and the value is {}, partirion is {}", key, value,
				result.getRecordMetadata().partition());
	}
}
