package com.lspb.transcoding;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;

@SpringBootApplication
@EnableKafkaRetryTopic
public class TranscodingApplication {

	public static void main(String[] args) {
		SpringApplication.run(TranscodingApplication.class, args);
	}

}
