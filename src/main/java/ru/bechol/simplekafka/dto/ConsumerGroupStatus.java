package ru.bechol.simplekafka.dto;

public record ConsumerGroupStatus(
		String groupId,
		boolean running
) {
}
