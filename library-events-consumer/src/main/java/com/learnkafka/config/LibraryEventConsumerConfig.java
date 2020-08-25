package com.learnkafka.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import com.learnkafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventConsumerConfig {

	@Autowired
	LibraryEventsService libraryEventsService;

	@Bean
	ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<Object, Object> kafkaConsumerFactory) {

		final ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		configurer.configure(factory, kafkaConsumerFactory);
		factory.setConcurrency(3); // util quando a aplicacao nao roda em conteiners. para aplicacoes na cloud, nao
		// eh necessario

		//		factory.getContainerProperties().setAckMode(AckMode.MANUAL);

		// tratamento de erros
		factory.setErrorHandler((thrownException, data) -> {
			log.info("Exception in consumerConfig is {} and the record is {}", thrownException.getMessage(), data);
		});

		// reprocessar mensagens
		factory.setRetryTemplate(retryTemplate());
		factory.setRecoveryCallback(context -> {
			if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
				log.info("Inside the recoverable logic");
//				Arrays.asList(context.attributeNames()).forEach(attributeName -> {
//					log.info("Attribute name is {}: ", attributeName);
//					log.info("Attribute value is {}: ", context.getAttribute(attributeName));
//				});

				final ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
				libraryEventsService.handleRecovery(consumerRecord);
			} else {

				log.info("Inside the non recoverable logic");
				throw new RuntimeException(context.getLastThrowable().getMessage());
			}

			return null;
		});

		return factory;
	}

	private RetryTemplate retryTemplate() {

		final FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);

		final RetryTemplate retryTemplate = new RetryTemplate();
		retryTemplate.setRetryPolicy(simpleRetryPolicy());
		retryTemplate.setBackOffPolicy(backOffPolicy);

		return retryTemplate;
	}

	private RetryPolicy simpleRetryPolicy() {

//		final SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
//		simpleRetryPolicy.setMaxAttempts(3);

		final Map<Class<? extends Throwable>, Boolean> retryableExceptions = new HashMap<>();
		retryableExceptions.put(IllegalArgumentException.class, false); // esta exception nao sera reprocessada
		retryableExceptions.put(RecoverableDataAccessException.class, true); // esta exception sera reprocessada
		final SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy(3, retryableExceptions, true);

		return simpleRetryPolicy;
	}

}
