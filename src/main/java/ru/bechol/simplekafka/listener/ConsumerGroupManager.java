package ru.bechol.simplekafka.listener;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import ru.bechol.simplekafka.config.AppProperties;
import ru.bechol.simplekafka.dto.ConsumerGroupStatus;
import ru.bechol.simplekafka.dto.PartitionOffset;
import ru.bechol.simplekafka.service.ConsumerLagService;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerGroupManager implements ApplicationRunner {

	private final ConcurrentKafkaListenerContainerFactory<String, String> messageListenerFactory;
	private final AppProperties appProperties;
	private final ConsumerLagService consumerLagService;
	private final Map<String, ConcurrentMessageListenerContainer<String, String>> containers = new LinkedHashMap<>();

	@Override
	public void run(ApplicationArguments args) {
		for (int i = 1; i <= appProperties.groupCount(); i++) {
			String groupId = appProperties.groupIdPrefix() + "-" + i;
			containers.put(groupId, createAndStart(groupId));
		}
	}

	public List<ConsumerGroupStatus> list() {
		return containers.keySet().stream()
				.map(this::status)
				.toList();
	}

	public ConsumerGroupStatus status(String groupId) {
		var container = requireContainer(groupId);
		List<PartitionOffset> partitions = consumerLagService.offsetsForGroup(groupId);
		long totalLag = partitions.stream()
				.map(PartitionOffset::lag)
				.filter(lag -> lag != null)
				.mapToLong(Long::longValue)
				.sum();
		return new ConsumerGroupStatus(groupId, container.isRunning(), totalLag, partitions);
	}

	public ConsumerGroupStatus stop(String groupId) {
		var container = requireContainer(groupId);
		if (!container.isRunning()) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "Consumer group already stopped: " + groupId);
		}
		container.stop();
		log.info("Stopped consumer group={}", groupId);
		return status(groupId);
	}

	public ConsumerGroupStatus start(String groupId) {
		var container = requireContainer(groupId);
		if (container.isRunning()) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "Consumer group already running: " + groupId);
		}
		container.start();
		log.info("Started consumer group={}", groupId);
		return status(groupId);
	}

	@PreDestroy
	public void stopAll() {
		containers.values().forEach(ConcurrentMessageListenerContainer::stop);
	}

	private ConcurrentMessageListenerContainer<String, String> createAndStart(String groupId) {
		var container = messageListenerFactory.createContainer(appProperties.topic());
		container.setBeanName("kafka-consumer-" + groupId);
		container.setConcurrency(1);
		container.getContainerProperties().setGroupId(groupId);
		container.setupMessageListener((MessageListener<String, String>) record ->
				log.info("Received message: group={}, topic={}, partition={}, offset={}, key={}, value={}",
						groupId,
						record.topic(),
						record.partition(),
						record.offset(),
						record.key(),
						record.value()));
		container.start();
		log.info("Started consumer group={} with 1 consumer on topic={}", groupId, appProperties.topic());
		return container;
	}

	private ConcurrentMessageListenerContainer<String, String> requireContainer(String groupId) {
		var container = containers.get(groupId);
		if (container == null) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown consumer group: " + groupId);
		}
		return container;
	}
}
