package ru.bechol.simplekafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.bechol.simplekafka.config.AppProperties;
import ru.bechol.simplekafka.metrics.ProducerMetrics;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final AppProperties appProperties;
	private final ProducerMetrics producerMetrics;

	public CompletableFuture<SendResult<String, String>> send(String key, String value) {
		log.info("Sending message producer={}, topic={}, key={}, value={}",
				appProperties.producerName(), appProperties.topic(), key, value);
		producerMetrics.recordAttempt();
		try {
			return kafkaTemplate.send(appProperties.topic(), key, value)
					.whenComplete((result, ex) -> {
						if (ex != null) {
							onFailure(key, ex);
						} else {
							producerMetrics.recordSuccess();
							log.info("Sent message producer={}, key={} to partition={} offset={}",
									appProperties.producerName(),
									key,
									result.getRecordMetadata().partition(),
									result.getRecordMetadata().offset());
						}
					});
		} catch (RuntimeException ex) {
			onFailure(key, ex);
			return CompletableFuture.failedFuture(ex);
		}
	}

	private void onFailure(String key, Throwable ex) {
		producerMetrics.recordFailure(ex);
		log.error("Failed to send message producer={}, key={}",
				appProperties.producerName(), key, ex);
	}
}
