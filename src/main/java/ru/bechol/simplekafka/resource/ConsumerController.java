package ru.bechol.simplekafka.resource;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.bechol.simplekafka.dto.ConsumerGroupStatus;
import ru.bechol.simplekafka.listener.ConsumerGroupManager;

import java.util.List;

@RestController
@RequestMapping("/api/consumers")
@RequiredArgsConstructor
public class ConsumerController {

	private final ConsumerGroupManager consumerGroupManager;

	@GetMapping
	public List<ConsumerGroupStatus> list() {
		return consumerGroupManager.list();
	}

	@PostMapping("/{groupId}/stop")
	public ConsumerGroupStatus stop(@PathVariable String groupId) {
		return consumerGroupManager.stop(groupId);
	}

	@PostMapping("/{groupId}/start")
	public ConsumerGroupStatus start(@PathVariable String groupId) {
		return consumerGroupManager.start(groupId);
	}
}
