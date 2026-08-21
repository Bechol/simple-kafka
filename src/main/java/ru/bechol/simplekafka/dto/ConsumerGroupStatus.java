package ru.bechol.simplekafka.dto;

import java.util.List;

public record ConsumerGroupStatus(
		String groupId,
		boolean running,
		long totalLag,
		List<PartitionOffset> partitions
) {
}
