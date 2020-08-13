/**
 *
 */
package com.learnkafka.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Leandro
 *
 */
@Slf4j
@RestController
public class LibraryEventsController {

	@Autowired
	LibraryEventProducer libraryEventProducer;

	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEventAssyncDefault(
			@RequestBody @Valid LibraryEvent libraryEvent)
					throws Exception {

		log.info("before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEvent(libraryEvent); // assynchornous
		log.info("after sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PostMapping("/v2/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent) throws Exception {

		log.info("before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		final SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		log.info("SendResult is {}", sendResult.toString());
		log.info("after sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PostMapping("/v3/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEventAssyncTopic(@RequestBody LibraryEvent libraryEvent)
			throws Exception {

		log.info("before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEventWithTopic(libraryEvent);
		log.info("after sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}

	@PutMapping("/v1/libraryevent")
	public ResponseEntity<?> putLibraryEventAssyncDefault(
			@RequestBody @Valid LibraryEvent libraryEvent)
					throws Exception {

		if(libraryEvent.getLibraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
		}

		log.info("before sendLibraryEvent");
		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEventWithTopic(libraryEvent); // assynchornous
		log.info("after sendLibraryEvent");
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}

}
