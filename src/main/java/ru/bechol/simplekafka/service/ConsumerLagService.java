package ru.bechol.simplekafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import ru.bechol.simplekafka.config.AppProperties;
import ru.bechol.simplekafka.dto.PartitionOffset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerLagService {

	private final AdminClient adminClient;
	private final AppProperties appProperties;

	public List<PartitionOffset> offsetsForGroup(String groupId) {
		String topic = appProperties.topic();
		try {
			TopicDescription description = adminClient.describeTopics(List.of(topic))
					.allTopicNames()
					.get()
					.get(topic);
			if (description == null) {
				return List.of();
			}

			List<TopicPartition> partitions = description.partitions().stream()
					.map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
					.toList();

			Map<TopicPartition, OffsetAndMetadata> committed = adminClient
					.listConsumerGroupOffsets(groupId)
					.partitionsToOffsetAndMetadata()
					.get();

			Map<TopicPartition, OffsetSpec> endOffsetRequest = new HashMap<>();
			partitions.forEach(tp -> endOffsetRequest.put(tp, OffsetSpec.latest()));
			Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
					adminClient.listOffsets(endOffsetRequest).all().get();

			List<PartitionOffset> result = new ArrayList<>();
			for (TopicPartition tp : partitions) {
				Long endOffset = endOffsets.containsKey(tp) ? endOffsets.get(tp).offset() : null;
				OffsetAndMetadata committedMeta = committed.get(tp);
				Long offset = committedMeta != null ? committedMeta.offset() : null;
				Long lag = (endOffset != null && offset != null) ? Math.max(0, endOffset - offset) : endOffset;
				result.add(new PartitionOffset(tp.partition(), offset, endOffset, lag));
			}
			return result;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			log.warn("Interrupted while fetching offsets for group={}", groupId, e);
			return List.of();
		} catch (ExecutionException e) {
			log.warn("Failed to fetch offsets for group={}: {}", groupId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
			return List.of();
		}
	}
}
