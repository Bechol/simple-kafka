package ru.bechol.simplekafka.resource;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.bechol.simplekafka.dto.ProducerStats;
import ru.bechol.simplekafka.metrics.ProducerMetrics;

@RestController
@RequestMapping("/api/producers")
@RequiredArgsConstructor
public class ProducerController {

	private final ProducerMetrics producerMetrics;

	@GetMapping
	public ProducerStats stats() {
		return producerMetrics.snapshot();
	}
}
