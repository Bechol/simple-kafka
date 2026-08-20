package ru.bechol.simplekafka.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static ru.bechol.simplekafka.config.KafkaConfig.MESSAGE_LISTENER_FACTORY;

@Slf4j
@Component
public class MessageListener {

	@KafkaListener(
			topics = "${app.kafka.topic}",
			containerFactory = MESSAGE_LISTENER_FACTORY
	)
	public void onMessage(ConsumerRecord<Integer, String> record) {
		log.info("Received message: topic={}, partition={}, offset={}, key={}, value={}",
				record.topic(),
				record.partition(),
				record.offset(),
				record.key(),
				record.value());
	}
}
