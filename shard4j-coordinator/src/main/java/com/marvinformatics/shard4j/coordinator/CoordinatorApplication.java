package com.marvinformatics.shard4j.coordinator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(CoordinatorSettings.class)
public class CoordinatorApplication {

  public static void main(String[] args) {
    SpringApplication.run(CoordinatorApplication.class, args);
  }
}
