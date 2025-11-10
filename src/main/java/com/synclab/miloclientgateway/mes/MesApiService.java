package com.synclab.miloclientgateway.mes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

@Slf4j
@Service
public class MesApiService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final KafkaProducerProperties kafkaProperties;
    private final MesPipelineProperties pipelineProperties;
    private final ObjectMapper objectMapper;

    public MesApiService(KafkaProducerProperties kafkaProperties,
                         MesPipelineProperties pipelineProperties,
                         ObjectMapper objectMapper,
                         KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.pipelineProperties = pipelineProperties;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
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

        Optional<Map<String, Object>> filteredPayload = applyFiltering(payload);
        if (filteredPayload.isEmpty()) {
            log.debug("Dropped telemetry {}.{} due to filtering", machineName, tagName);
            return;
        }

        Map<String, Object> requestBody = Map.of(
                "records", List.of(Map.of("value", filteredPayload.get()))
        );

        byte[] requestPayload = serialize(requestBody);
        requestPayload = applyCompressionIfNecessary(requestPayload);

        kafkaTemplate
                .send(kafkaProperties.getTopic(), buildRecordKey(machineName, tagName), requestPayload)
                .completable()
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to send data to Kafka topic {}: {}", kafkaProperties.getTopic(), throwable.getMessage(), throwable);
                    } else if (result != null) {
                        log.info("Published telemetry to Kafka → {}.{} partition={} offset={}",
                                machineName, tagName, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    }
                });
    }

    private String buildRecordKey(String machineName, String tagName) {
        return machineName + "." + tagName;
    }

    private Map<String, Object> buildPayload(String machineName, String tagName, Object value) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("machine", machineName);
        payload.put("tag", tagName);
        payload.put("value", value);
        payload.put("timestamp", System.currentTimeMillis());
        return payload;
    }

    private Optional<Map<String, Object>> applyFiltering(Map<String, Object> payload) {
        MesPipelineProperties.FilterProperties filter = pipelineProperties.getFilter();
        if (!filter.isEnabled()) {
            return Optional.of(payload);
        }

        if (!filter.getAllowedMachines().isEmpty()) {
            Object machine = payload.get("machine");
            if (machine == null || !filter.getAllowedMachines().contains(machine.toString())) {
                return Optional.empty();
            }
        }

        Object tag = payload.get("tag");
        if (!filter.getAllowedTags().isEmpty()) {
            if (tag == null || !filter.getAllowedTags().contains(tag.toString())) {
                return Optional.empty();
            }
        }

        if (tag != null && (!filter.getAllowedTagSuffixes().isEmpty() || !filter.getAllowedTagKeywords().isEmpty())) {
            String tagValue = tag.toString();
            boolean suffixMatch = filter.getAllowedTagSuffixes().isEmpty() ||
                    filter.getAllowedTagSuffixes().stream().anyMatch(tagValue::endsWith);
            boolean keywordMatch = filter.getAllowedTagKeywords().isEmpty() ||
                    filter.getAllowedTagKeywords().stream().anyMatch(tagValue::contains);

            if (!(suffixMatch && keywordMatch)) {
                return Optional.empty();
            }
        } else if (tag == null && (!filter.getAllowedTagSuffixes().isEmpty() || !filter.getAllowedTagKeywords().isEmpty())) {
            return Optional.empty();
        }

        Map<String, Object> filteredPayload;
        if (!filter.getIncludeFields().isEmpty()) {
            filteredPayload = new LinkedHashMap<>();
            Set<String> includeFields = Set.copyOf(filter.getIncludeFields());
            payload.forEach((key, value) -> {
                if (includeFields.contains(key)) {
                    filteredPayload.put(key, value);
                }
            });
        } else {
            filteredPayload = new LinkedHashMap<>(payload);
        }

        if (filter.isDropNullValues()) {
            filteredPayload.values().removeIf(value -> value == null);
        }

        if (filteredPayload.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(filteredPayload);
    }

    private byte[] serialize(Map<String, Object> requestBody) {
        try {
            return objectMapper.writeValueAsBytes(requestBody);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize MES payload", e);
        }
    }

    private byte[] applyCompressionIfNecessary(byte[] payload) {
        MesPipelineProperties.CompressionProperties compression = pipelineProperties.getCompression();
        if (!compression.isEnabled()) {
            return payload;
        }

        if (compression.getAlgorithm() == MesPipelineProperties.CompressionAlgorithm.GZIP) {
            return gzip(payload);
        }

        throw new IllegalStateException("Unsupported compression algorithm: " + compression.getAlgorithm());
    }

    private byte[] gzip(byte[] payload) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzipOutputStream = new GZIPOutputStream(baos)) {
            gzipOutputStream.write(payload);
            gzipOutputStream.finish();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to gzip MES payload", e);
        }
    }
}
