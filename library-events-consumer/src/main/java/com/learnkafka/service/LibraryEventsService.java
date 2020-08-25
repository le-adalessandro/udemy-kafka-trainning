/**
 *
 */
package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Leandro
 *
 */
@Service
@Slf4j
public class LibraryEventsService {

	@Autowired
	private LibraryEventsRepository repository;

	@Autowired
	ObjectMapper mapper;

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
			throws JsonMappingException, JsonProcessingException {

		final LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("libraryEvent: {}", libraryEvent);

		if (libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 000) {
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
			log.info("Invalid Library Event Type");
		}
	}

	private void validate(LibraryEvent libraryEvent) {

		Optional<LibraryEvent> searchResult = null;
		try {
			if (libraryEvent.getLibraryEventId() == null) {
				throw new IllegalArgumentException("Library Event Id is missing");
			}

			searchResult = repository.findById(libraryEvent.getLibraryEventId());

			if (!searchResult.isPresent()) {
				throw new IllegalArgumentException("Not a validLibrary Event");
			}
		} catch (final IllegalArgumentException e) {
			log.error("Validation error for the Library Event {}: ", libraryEvent);
			throw e;
		}

		log.info("Validation is successfull for the Library Event {}: ", searchResult.get());
	}

	private void save(LibraryEvent libraryEvent) {

		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repository.save(libraryEvent);
		log.info("Successfully Persisted the Library Event {}", libraryEvent);
	}

	public void handleRecovery(ConsumerRecord<Integer, String> record) {

		final Integer key = record.key();
		final String data = record.value();

		final ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, data);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, data, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, data, ex);
			}
		});
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
