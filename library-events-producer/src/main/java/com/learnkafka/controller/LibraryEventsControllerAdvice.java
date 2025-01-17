/**
 *
 */
package com.learnkafka.controller;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Leandro
 *
 */
@ControllerAdvice
@Slf4j
public class LibraryEventsControllerAdvice {

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<?> handleRequestBody(MethodArgumentNotValidException ex) {

		final List<FieldError> erroList = ex.getBindingResult().getFieldErrors();

		final String errorMessage = erroList.stream()
				.map(fieldError -> fieldError.getField() + " - " + fieldError.getDefaultMessage()).sorted()
				.collect(Collectors.joining(", "));

		log.info("errorMessage: {}", errorMessage);

		return new ResponseEntity<>(errorMessage, HttpStatus.BAD_REQUEST);
	}
}
