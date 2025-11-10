package com.synclab.miloclientgateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MiloClientGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(MiloClientGatewayApplication.class, args);
    }

}
