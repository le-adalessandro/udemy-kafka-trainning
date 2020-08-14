/**
 *
 */
package com.learnkafka.jpa;

import org.springframework.data.repository.CrudRepository;

import com.learnkafka.entity.LibraryEvent;

/**
 * @author Leandro
 *
 */
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {

}
