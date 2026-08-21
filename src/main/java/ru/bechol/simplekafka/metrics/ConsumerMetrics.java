package ru.bechol.simplekafka.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import jakarta.annotation.PostConstruct;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.bechol.simplekafka.config.AppProperties;
import ru.bechol.simplekafka.dto.ConsumerGroupStatus;
import ru.bechol.simplekafka.dto.PartitionOffset;
import ru.bechol.simplekafka.listener.ConsumerGroupManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ConsumerMetrics {

	private final MeterRegistry meterRegistry;
	private final ConsumerGroupManager consumerGroupManager;
	private final AppProperties appProperties;
	private final Map<String, AtomicLong> runningByGroup = new ConcurrentHashMap<>();
	private final Map<String, AtomicLong> totalLagByGroup = new ConcurrentHashMap<>();
	private final MultiGauge offsetGauge;
	private final MultiGauge endOffsetGauge;
	private final MultiGauge lagGauge;

	public ConsumerMetrics(
			MeterRegistry meterRegistry,
			ConsumerGroupManager consumerGroupManager,
			AppProperties appProperties) {
		this.meterRegistry = meterRegistry;
		this.consumerGroupManager = consumerGroupManager;
		this.appProperties = appProperties;
		this.offsetGauge = MultiGauge.builder("kafka.consumer.offset")
				.description("Committed consumer offset")
				.baseUnit("offsets")
				.register(meterRegistry);
		this.endOffsetGauge = MultiGauge.builder("kafka.consumer.end.offset")
				.description("Log end offset")
				.baseUnit("offsets")
				.register(meterRegistry);
		this.lagGauge = MultiGauge.builder("kafka.consumer.lag")
				.description("Consumer lag (end offset - committed offset)")
				.baseUnit("messages")
				.register(meterRegistry);
	}

	@PostConstruct
	void registerGroupGauges() {
		for (int i = 1; i <= appProperties.groupCount(); i++) {
			String groupId = appProperties.groupIdPrefix() + "-" + i;
			AtomicLong running = new AtomicLong(0);
			AtomicLong totalLag = new AtomicLong(0);
			runningByGroup.put(groupId, running);
			totalLagByGroup.put(groupId, totalLag);

			Gauge.builder("kafka.consumer.running", running, AtomicLong::get)
					.description("1 if consumer group container is running, otherwise 0")
					.tag("groupId", groupId)
					.tag("topic", appProperties.topic())
					.register(meterRegistry);

			Gauge.builder("kafka.consumer.lag.total", totalLag, AtomicLong::get)
					.description("Total lag across all partitions for consumer group")
					.tag("groupId", groupId)
					.tag("topic", appProperties.topic())
					.register(meterRegistry);
		}
	}

	@Scheduled(fixedDelayString = "${app.kafka.metrics-refresh-ms:5000}")
	public void refresh() {
		List<ConsumerGroupStatus> statuses = consumerGroupManager.list();
		List<MultiGauge.Row<?>> offsetRows = new ArrayList<>();
		List<MultiGauge.Row<?>> endOffsetRows = new ArrayList<>();
		List<MultiGauge.Row<?>> lagRows = new ArrayList<>();

		for (ConsumerGroupStatus status : statuses) {
			AtomicLong running = runningByGroup.get(status.groupId());
			AtomicLong totalLag = totalLagByGroup.get(status.groupId());
			if (running != null) {
				running.set(status.running() ? 1 : 0);
			}
			if (totalLag != null) {
				totalLag.set(status.totalLag());
			}

			for (PartitionOffset partition : status.partitions()) {
				Tags tags = Tags.of(
						"groupId", status.groupId(),
						"topic", appProperties.topic(),
						"partition", String.valueOf(partition.partition()));
				offsetRows.add(MultiGauge.Row.of(tags, nullToZero(partition.offset())));
				endOffsetRows.add(MultiGauge.Row.of(tags, nullToZero(partition.endOffset())));
				lagRows.add(MultiGauge.Row.of(tags, nullToZero(partition.lag())));
			}
		}

		offsetGauge.register(offsetRows, true);
		endOffsetGauge.register(endOffsetRows, true);
		lagGauge.register(lagRows, true);
	}

	private static double nullToZero(Long value) {
		return value != null ? value : 0d;
	}
}
