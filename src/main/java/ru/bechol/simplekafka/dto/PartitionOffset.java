package ru.bechol.simplekafka.dto;

public record PartitionOffset(
		int partition,
		Long offset,
		Long endOffset,
		Long lag
) {
}
