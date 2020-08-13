/**
 *
 */
package com.learnkafka.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;

/**
 * @author Leandro
 *
 */
@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

	@Autowired
	MockMvc mockMvc;

	@MockBean
	LibraryEventProducer libraryEventProducer;

	ObjectMapper objectMapper = new ObjectMapper();

	@Test
	public void postLibraryEvent() throws Exception {

		final Book book = Book.builder().bookId(123).bookAuthor("Me!!").bookName("Kafka com Spring Boot").build();

		final LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

		final String json = objectMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventWithTopic(isA(LibraryEvent.class))).thenReturn(null);

		final MockHttpServletRequestBuilder request = post("/v1/libraryevent").content(json)
				.contentType(MediaType.APPLICATION_JSON);
		mockMvc.perform(request).andExpect(status().isCreated());
	}

	@Test
	public void postLibraryEventNullBook() throws Exception {

		final LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(null).build();

		final String json = objectMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventWithTopic(isA(LibraryEvent.class))).thenReturn(null);

		final MockHttpServletRequestBuilder request = post("/v1/libraryevent").content(json)
				.contentType(MediaType.APPLICATION_JSON);

		final String expectedErrorMessage = "book - must not be null";
		mockMvc.perform(request).andExpect(status().is4xxClientError())
		.andExpect(content().string(expectedErrorMessage));
	}

	@Test
	public void postLibraryEventEmptyBok() throws Exception {

		final LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(new Book()).build();

		final String json = objectMapper.writeValueAsString(libraryEvent);

		when(libraryEventProducer.sendLibraryEventWithTopic(isA(LibraryEvent.class))).thenReturn(null);

		final MockHttpServletRequestBuilder request = post("/v1/libraryevent").content(json)
				.contentType(MediaType.APPLICATION_JSON);

		final String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null, book.bookName - must not be blank";
		mockMvc.perform(request).andExpect(status().is4xxClientError())
		.andExpect(content().string(expectedErrorMessage));
	}
}
