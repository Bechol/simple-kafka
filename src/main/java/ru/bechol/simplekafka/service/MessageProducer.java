package ru.bechol.simplekafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.bechol.simplekafka.config.AppProperties;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

	private final KafkaTemplate<Integer, String> kafkaTemplate;
	private final AppProperties appProperties;

	public CompletableFuture<SendResult<Integer, String>> send(Integer key, String value) {
		log.info("Sending message to topic={}, key={}, value={}", appProperties.topic(), key, value);
		return kafkaTemplate.send(appProperties.topic(), key, value)
				.whenComplete((result, ex) -> {
					if (ex != null) {
						log.error("Failed to send message key={}", key, ex);
					} else {
						log.info("Sent message key={} to partition={} offset={}",
								key,
								result.getRecordMetadata().partition(),
								result.getRecordMetadata().offset());
					}
				});
	}
}
