package com.synclab.miloclientgateway.mes;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "mes.kafka")
public class KafkaProducerProperties {

    /**
     * Kafka bootstrap servers list, e.g. localhost:9092
     */
    private String bootstrapServers;

    /**
     * Kafka topic that should receive machine telemetry.
     */
    private String topic;

    /**
     * Optional Kafka client id for this gateway.
     */
    private String clientId = "milo-client-gateway";

    /**
     * Kafka producer acknowledgement policy.
     */
    private String acks = "1";

    public void validate() {
        if (!StringUtils.hasText(bootstrapServers)) {
            throw new IllegalStateException("Kafka bootstrapServers must be configured (mes.kafka.bootstrap-servers).");
        }
        if (!StringUtils.hasText(topic)) {
            throw new IllegalStateException("Kafka topic must be configured (mes.kafka.topic).");
        }
    }
}
