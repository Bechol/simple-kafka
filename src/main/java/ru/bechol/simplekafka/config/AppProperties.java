package ru.bechol.simplekafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public record AppProperties(
		String topic,
		int groupCount,
		String groupIdPrefix,
		long metricsRefreshMs
) {
}
