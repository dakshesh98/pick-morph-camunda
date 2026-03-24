package com.temporallearn.spring_temporal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class SpringCamundaApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCamundaApplication.class, args);
	}

}
