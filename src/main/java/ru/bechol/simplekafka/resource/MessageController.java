package ru.bechol.simplekafka.resource;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import ru.bechol.simplekafka.dto.MessageRequest;
import ru.bechol.simplekafka.service.MessageProducer;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

	private final MessageProducer messageProducer;

	@PostMapping
	@ResponseStatus(HttpStatus.ACCEPTED)
	public void send(@Valid @RequestBody MessageRequest request) {
		messageProducer.send(request.key(), request.value());
	}
}
