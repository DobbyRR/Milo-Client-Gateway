package com.synclab.miloclientgateway.mes;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "mes.kafka-bridge")
public class KafkaBridgeProperties {

    /**
     * Base URL of the Kafka Bridge REST endpoint, e.g. http://localhost:8083
     */
    private String baseUrl;

    /**
     * Kafka topic that should receive machine telemetry.
     */
    private String topic;

    public URI topicUri() {
        if (!StringUtils.hasText(baseUrl)) {
            throw new IllegalStateException("Kafka Bridge baseUrl must be configured (mes.kafka-bridge.base-url).");
        }
        if (!StringUtils.hasText(topic)) {
            throw new IllegalStateException("Kafka Bridge topic must be configured (mes.kafka-bridge.topic).");
        }
        return UriComponentsBuilder
                .fromUriString(baseUrl)
                .pathSegment("topics")
                .pathSegment(topic)
                .build()
                .toUri();
    }
}
