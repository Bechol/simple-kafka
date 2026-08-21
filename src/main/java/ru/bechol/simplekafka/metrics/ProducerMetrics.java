package ru.bechol.simplekafka.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.bechol.simplekafka.config.AppProperties;
import ru.bechol.simplekafka.dto.ProducerErrorStat;
import ru.bechol.simplekafka.dto.ProducerStats;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class ProducerMetrics {

	private final AppProperties appProperties;
	private final MeterRegistry meterRegistry;
	private final AtomicLong total = new AtomicLong();
	private final AtomicLong success = new AtomicLong();
	private final AtomicLong failed = new AtomicLong();
	private final Map<String, AtomicLong> errorsByType = new ConcurrentHashMap<>();
	private final MultiGauge errorsGauge;

	@Value("${app.kafka.producer-metrics-reset-zone:Europe/Moscow}")
	private String resetZone;

	public ProducerMetrics(AppProperties appProperties, MeterRegistry meterRegistry) {
		this.appProperties = appProperties;
		this.meterRegistry = meterRegistry;
		this.errorsGauge = MultiGauge.builder("kafka.producer.errors")
				.description("Producer send errors by exception type since last daily reset")
				.baseUnit("messages")
				.register(meterRegistry);
	}

	@PostConstruct
	void registerGauges() {
		Tags tags = producerTags();
		Gauge.builder("kafka.producer.sent.all", total, AtomicLong::get)
				.description("Total producer send attempts since last daily reset")
				.tags(tags)
				.register(meterRegistry);
		Gauge.builder("kafka.producer.sent.success", success, AtomicLong::get)
				.description("Successful producer sends since last daily reset")
				.tags(tags)
				.register(meterRegistry);
		Gauge.builder("kafka.producer.sent.failed", failed, AtomicLong::get)
				.description("Failed producer sends since last daily reset")
				.tags(tags)
				.register(meterRegistry);
		refreshErrorGauges();
	}

	public void recordAttempt() {
		total.incrementAndGet();
	}

	public void recordSuccess() {
		success.incrementAndGet();
	}

	public void recordFailure(Throwable throwable) {
		failed.incrementAndGet();
		String type = resolveErrorType(throwable);
		errorsByType.computeIfAbsent(type, ignored -> new AtomicLong()).incrementAndGet();
		refreshErrorGauges();
	}

	public ProducerStats snapshot() {
		List<ProducerErrorStat> errors = errorsByType.entrySet().stream()
				.map(entry -> new ProducerErrorStat(entry.getKey(), entry.getValue().get()))
				.sorted(Comparator.comparingLong(ProducerErrorStat::count).reversed()
						.thenComparing(ProducerErrorStat::type))
				.toList();
		return new ProducerStats(
				appProperties.producerName(),
				total.get(),
				success.get(),
				failed.get(),
				errors);
	}

	@Scheduled(cron = "${app.kafka.producer-metrics-reset-cron:0 0 0 * * *}",
			zone = "${app.kafka.producer-metrics-reset-zone:Europe/Moscow}")
	public void resetDaily() {
		total.set(0);
		success.set(0);
		failed.set(0);
		errorsByType.clear();
		refreshErrorGauges();
		log.info("Producer metrics reset for producer={} (zone={})", appProperties.producerName(), resetZone);
	}

	private void refreshErrorGauges() {
		List<MultiGauge.Row<?>> rows = errorsByType.entrySet().stream()
				.<MultiGauge.Row<?>>map(entry -> MultiGauge.Row.of(
						producerTags().and("error", entry.getKey()),
						entry.getValue().get()))
				.toList();
		errorsGauge.register(rows, true);
	}

	private Tags producerTags() {
		return Tags.of(
				"producer", appProperties.producerName(),
				"topic", appProperties.topic());
	}

	private static String resolveErrorType(Throwable throwable) {
		Throwable current = throwable;
		while (current.getCause() != null && current.getCause() != current) {
			current = current.getCause();
		}
		return current.getClass().getSimpleName();
	}
}
