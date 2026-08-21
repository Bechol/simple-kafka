package ru.bechol.simplekafka.listener;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;
import ru.bechol.simplekafka.config.AppProperties;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerGroupStarter implements ApplicationRunner {

	private final ConcurrentKafkaListenerContainerFactory<String, String> messageListenerFactory;
	private final AppProperties appProperties;
	private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();

	@Override
	public void run(ApplicationArguments args) {
		for (int i = 1; i <= appProperties.groupCount(); i++) {
			String groupId = appProperties.groupIdPrefix() + "-" + i;
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
			containers.add(container);
			log.info("Started consumer group={} with 1 consumer on topic={}", groupId, appProperties.topic());
		}
	}

	@PreDestroy
	public void stop() {
		containers.forEach(ConcurrentMessageListenerContainer::stop);
	}
}
