package com.synclab.miloclientgateway.mes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final Set<String> NG_TAGS = Set.of("order_ng_qty", "order_ng_type", "order_ng_name");
    private static final String NG_EVENT_TAG = "order_ng_event";
    private static final String EQUIPMENT_ID_TAG = "equipment_id";

    private final Map<String, AggregateBucket> energyAggregationBuckets = new ConcurrentHashMap<>();
    private final Map<String, NgEventState> ngEventStates = new ConcurrentHashMap<>();
    private final Map<String, String> equipmentIdByMachine = new ConcurrentHashMap<>();

    public void sendMachineData(String machineName, String tagName, Object value) {
        Map<String, Object> payload = buildPayload(machineName, tagName, value);

        Optional<Map<String, Object>> filteredPayloadOpt = applyFiltering(payload);
        if (filteredPayloadOpt.isEmpty()) {
            log.debug("Dropped telemetry {}.{} due to filtering", machineName, tagName);
            return;
        }
        Map<String, Object> filteredPayload = filteredPayloadOpt.get();

        if (isEquipmentIdTag(tagName)) {
            Object equipmentId = filteredPayload.get("value");
            if (equipmentId != null && !equipmentId.toString().isBlank()) {
                equipmentIdByMachine.put(machineName, equipmentId.toString());
            }
        }

        if (handleNgTelemetry(machineName, tagName, filteredPayload)) {
            return;
        }

        if (shouldAggregate(tagName)
                && pipelineProperties.getEnergyAggregation().isEnabled()) {
            bufferEnergyUsage(machineName, tagName, filteredPayload);
            return;
        }

        sendPayload(machineName, tagName, filteredPayload);
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

    private boolean shouldAggregate(String tagName) {
        return tagName != null && tagName.endsWith("energy_usage");
    }

    private void bufferEnergyUsage(String machineName, String tagName, Map<String, Object> payload) {
        Object value = payload.get("value");
        if (!(value instanceof Number number)) {
            log.debug("Cannot aggregate non-numeric energy_usage for {}.{}", machineName, tagName);
            sendPayload(machineName, tagName, payload);
            return;
        }
        String key = buildRecordKey(machineName, tagName);
        energyAggregationBuckets.compute(key, (k, bucket) -> {
            if (bucket == null) {
                bucket = new AggregateBucket(machineName, tagName);
            }
            bucket.add(number.doubleValue());
            return bucket;
        });
    }

    @Scheduled(fixedRateString = "${mes.pipeline.energy-aggregation.window-ms:10000}")
    public void flushEnergyAggregation() {
        MesPipelineProperties.EnergyAggregationProperties aggregation = pipelineProperties.getEnergyAggregation();
        if (!aggregation.isEnabled()) {
            if (!energyAggregationBuckets.isEmpty()) {
                energyAggregationBuckets.clear();
            }
            return;
        }

        Map<String, AggregateBucket> snapshot = new HashMap<>(energyAggregationBuckets);
        energyAggregationBuckets.clear();

        snapshot.values().forEach(bucket -> {
            if (bucket.count == 0) {
                return;
            }
            double average = bucket.sum / bucket.count;
            Map<String, Object> aggregatedPayload = buildPayload(bucket.machineName, bucket.tagName, average);
            aggregatedPayload.put("aggregated", true);
            aggregatedPayload.put("sampleCount", bucket.count);
            aggregatedPayload.put("windowMs", aggregation.getWindowMs());
            sendPayload(bucket.machineName, bucket.tagName, aggregatedPayload);
        });
    }

    private void sendPayload(String machineName, String tagName, Map<String, Object> payload) {
        Map<String, Object> body = Map.of(
                "records", List.of(Map.of("value", payload))
        );

        byte[] requestPayload = serialize(body);
        requestPayload = applyCompressionIfNecessary(requestPayload);

        kafkaTemplate
                .send(kafkaProperties.getTopic(), buildRecordKey(machineName, tagName), requestPayload)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to send data to Kafka topic {}: {}", kafkaProperties.getTopic(), throwable.getMessage(), throwable);
                    } else if (result != null) {
                        Object value = payload.containsKey("value") ? payload.get("value") : payload;
                        if (shouldEmitValueLog(tagName)) {
                            log.info("Kafka upload [{}] value={}", tagName, value);
                        } else if (log.isDebugEnabled()) {
                            log.debug("Published telemetry → {}.{} partition={} offset={}",
                                    machineName, tagName, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                        }
                    }
                });
    }

    private boolean shouldEmitValueLog(String tagName) {
        if (tagName == null) {
            return false;
        }
        return tagName.endsWith("energy_usage")
                || NG_TAGS.contains(tagName.toLowerCase())
                || NG_EVENT_TAG.equalsIgnoreCase(tagName);
    }

    private boolean isEquipmentIdTag(String tagName) {
        return tagName != null && EQUIPMENT_ID_TAG.equalsIgnoreCase(tagName);
    }

    private String resolveEquipmentId(String machineName) {
        return equipmentIdByMachine.getOrDefault(machineName, machineName);
    }

    private boolean handleNgTelemetry(String machineName, String tagName, Map<String, Object> payload) {
        if (tagName == null) {
            return false;
        }
        String normalized = tagName.toLowerCase();
        if (!NG_TAGS.contains(normalized)) {
            return false;
        }
        Object value = payload.get("value");
        if (value == null) {
            return true;
        }

        NgEventState state = ngEventStates.computeIfAbsent(machineName, key -> new NgEventState());

        synchronized (state) {
            switch (normalized) {
                case "order_ng_qty" -> state.setNgQty(toInteger(value));
                case "order_ng_type" -> state.setNgType(toInteger(value));
                case "order_ng_name" -> state.setNgName(value.toString());
            }

            if (state.isComplete()) {
                emitNgEvent(machineName, state);
            }
        }
        return true;
    }

    private void emitNgEvent(String machineName, NgEventState state) {
        if (!state.canUpload()) {
            log.debug("Skipping NG event for {} due to empty name or non-positive qty (name={}, qty={})",
                    machineName, state.getNgName(), state.getNgQty());
            return;
        }

        Map<String, Object> ngPayload = new LinkedHashMap<>();
        ngPayload.put("equipmentId", resolveEquipmentId(machineName));
        ngPayload.put("ng_type", state.getNgType());
        ngPayload.put("ng_name", state.getNgName());
        ngPayload.put("ng_qty", state.getNgQty());

        sendPayload(machineName, NG_EVENT_TAG, ngPayload);
    }

    private Integer toInteger(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            log.debug("Unable to parse NG numeric value: {}", value);
            return null;
        }
    }

    private static final class AggregateBucket {
        private final String machineName;
        private final String tagName;
        private double sum;
        private long count;

        private AggregateBucket(String machineName, String tagName) {
            this.machineName = machineName;
            this.tagName = tagName;
        }

        private void add(double value) {
            this.sum += value;
            this.count++;
        }
    }

    private static final class NgEventState {
        private Integer ngQty;
        private Integer ngType;
        private String ngName;

        private void setNgQty(Integer ngQty) {
            this.ngQty = ngQty;
        }

        private void setNgType(Integer ngType) {
            this.ngType = ngType;
        }

        private void setNgName(String ngName) {
            this.ngName = ngName;
        }

        private Integer getNgQty() {
            return ngQty;
        }

        private Integer getNgType() {
            return ngType;
        }

        private String getNgName() {
            return ngName;
        }

        private boolean isComplete() {
            return ngQty != null && ngType != null && ngName != null;
        }

        private boolean canUpload() {
            return ngName != null && !ngName.isBlank()
                    && ngQty != null && ngQty > 0
                    && ngType != null;
        }
    }
}
