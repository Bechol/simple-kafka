package ru.bechol.simplekafka.dto;

import jakarta.validation.constraints.NotBlank;

public record MessageRequest(
		@NotBlank String key,
		@NotBlank String value
) {
}
