package ru.bechol.simplekafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.bechol.simplekafka.config.AppProperties;

@EnableConfigurationProperties(AppProperties.class)
@SpringBootApplication
public class SimpleKafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SimpleKafkaApplication.class, args);
	}
}
