/**
 *
 */
package com.learnkafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import lombok.extern.slf4j.Slf4j;

/**
 * Para que este bean funciona, eh necessario comentar a anotacao @Component da
 * classe LibraryEventsConsumerManualOffset e descomentar a mesma anotacao nesta
 * classe Tambem eh necessario retirar o comentario na classe
 * LibraryEventConsumerConfig
 *
 * @author Leandro
 *
 */
//@Component
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

	@Override
	@KafkaListener(topics = { "library-events" })
	public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {

		log.info("ConsumerRecord: {}", consumerRecord);
		acknowledgment.acknowledge();
	}
}
