/**
 *
 */
package com.learnkafka.domain;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Leandro
 *
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class Book {

	@NotNull
	private Integer bookId;
	@NotBlank
	private String bookName;
	@NotBlank
	private String bookAuthor;

}
