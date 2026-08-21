package ru.bechol.simplekafka.dto;

import java.util.List;

public record ProducerStats(
		String producer,
		long total,
		long success,
		long failed,
		List<ProducerErrorStat> errors
) {
}
