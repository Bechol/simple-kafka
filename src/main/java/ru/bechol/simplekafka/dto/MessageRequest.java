package ru.bechol.simplekafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record MessageRequest(
		@NotNull Integer key,
		@NotBlank String value
) {
}
