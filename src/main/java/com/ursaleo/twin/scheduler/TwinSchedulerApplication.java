package com.ursaleo.twin.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TwinSchedulerApplication {

	public static void main(String[] args) {
		SpringApplication.run(TwinSchedulerApplication.class, args);
	}

}
