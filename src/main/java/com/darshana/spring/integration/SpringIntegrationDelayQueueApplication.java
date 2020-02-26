package com.darshana.spring.integration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.integration.config.EnableIntegration;

@EnableIntegration
@SpringBootApplication
public class SpringIntegrationDelayQueueApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringIntegrationDelayQueueApplication.class, args);
	}


}
