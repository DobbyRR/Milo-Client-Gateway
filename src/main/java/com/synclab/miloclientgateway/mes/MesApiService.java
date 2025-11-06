package com.synclab.miloclientgateway.mes;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class MesApiService {

    private final RestTemplate restTemplate;
    private final KafkaBridgeProperties kafkaBridgeProperties;

    public MesApiService(KafkaBridgeProperties kafkaBridgeProperties,
                         RestTemplateBuilder restTemplateBuilder) {
        this.kafkaBridgeProperties = kafkaBridgeProperties;
        this.restTemplate = restTemplateBuilder
                .setConnectTimeout(Duration.ofSeconds(5))
                .setReadTimeout(Duration.ofSeconds(5))
                .build();
    }

    /**
     * Push Milo OPC data into MES through Kafka Bridge in JSON format.
     *
     * @param machineName ex) "Machine1"
     * @param tagName     ex) "Temperature"
     * @param value       ex) 24.5
     */
    public void sendMachineData(String machineName, String tagName, Object value) {
        Map<String, Object> payload = buildPayload(machineName, tagName, value);
        Map<String, Object> requestBody = Map.of(
                "records", List.of(Map.of("value", payload))
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(
                    kafkaBridgeProperties.topicUri(),
                    request,
                    String.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("Published telemetry via Kafka Bridge → {}.{} = {}", machineName, tagName, value);
            } else {
                log.warn("Kafka Bridge responded with status {} and body {}", response.getStatusCode(), response.getBody());
            }
        } catch (RestClientException e) {
            log.error("Failed to send data to Kafka Bridge: {}", e.getMessage(), e);
        }
    }

    private Map<String, Object> buildPayload(String machineName, String tagName, Object value) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("machine", machineName);
        payload.put("tag", tagName);
        payload.put("value", value);
        payload.put("timestamp", System.currentTimeMillis());
        return payload;
    }
}
