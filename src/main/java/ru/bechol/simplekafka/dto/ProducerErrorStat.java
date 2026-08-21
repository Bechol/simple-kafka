package ru.bechol.simplekafka.dto;

public record ProducerErrorStat(
		String type,
		long count
) {
}
