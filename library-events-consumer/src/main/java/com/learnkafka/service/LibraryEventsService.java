/**
 *
 */
package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
			throws JsonMappingException, JsonProcessingException {

		final LibraryEvent libraryEvent = mapper.readValue(consumerRecord.value(), LibraryEvent.class);

		log.info("libraryEvent: {}", libraryEvent);

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

		if(libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("Library Event Id is missing");
		}

		final Optional<LibraryEvent> searchResult = repository.findById(libraryEvent.getLibraryEventId());

		if (!searchResult.isPresent()) {
			throw new IllegalArgumentException("Not a validLibrary Event");
		}

		log.info("Validation is successfull for the Library Event {}: ", searchResult.get());
	}

	private void save(LibraryEvent libraryEvent) {

		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		repository.save(libraryEvent);
		log.info("Successfully Persisted the Library Event {}", libraryEvent);
	}
}
