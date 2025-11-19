package com.synclab.miloclientgateway.opcua;

import com.synclab.miloclientgateway.mes.MesApiService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.client.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.NodeClass;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class MiloOpcClient {

    private static final String ENDPOINT = "opc.tcp://192.168.0.23:4840/milo";
    private static final double DEFAULT_SAMPLING_INTERVAL = 1000.0;

    private final AtomicLong clientHandleSeq = new AtomicLong(1);
    private final Map<String, NodeId> nodeLookup = new ConcurrentHashMap<>();

    private OpcUaClient client;

    @Autowired
    private MesApiService mesApiService;

    private record NodeTarget(NodeId nodeId, String group, String tag) {}

    @PostConstruct
    public void connect() {
        try {
            client = OpcUaClient.create(ENDPOINT);
            client.connect().get();
            log.info("Connected to Milo Server at {}", ENDPOINT);

            startUpSnapshotAndSubscribe();
        } catch (Exception e) {
            log.error("OPC UA connection failed: {}", e.getMessage(), e);
        }
    }

    private void startUpSnapshotAndSubscribe() throws Exception {
        UaNode machinesFolder = findMachinesFolder();

        List<NodeTarget> targets = new ArrayList<>();
        for (UaNode child : client.getAddressSpace().browseNodes(machinesFolder.getNodeId())) {
            if (child.getNodeClass() == NodeClass.Object) {
                collectGroupNodes(child, new ArrayList<>(), targets);
            }
        }

        if (targets.isEmpty()) {
            log.warn("No telemetry nodes discovered under Machines folder.");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Discovered OPC nodes: {}", nodeLookup.keySet());
        }

        List<NodeId> nodeIds = targets.stream().map(NodeTarget::nodeId).toList();
        List<DataValue> values = client.readValues(0, TimestampsToReturn.Both, nodeIds).get();
        for (int i = 0; i < targets.size(); i++) {
            NodeTarget target = targets.get(i);
            Object payload = extractVariant(values.get(i));
            mesApiService.sendMachineData(target.group(), target.tag(), payload);
            if (isPayloadTag(target.tag())) {
                log.info("SNAPSHOT PAYLOAD {}.{} = {}", target.group(), target.tag(), payload);
            } else if (isItemCodeTag(target.tag())) {
                log.info("SNAPSHOT ITEM CODE {}.{} = {}", target.group(), target.tag(), payload);
            } else {
                log.debug("SNAPSHOT {}.{} = {}", target.group(), target.tag(), payload);
            }
        }

        UaSubscription sub = client.getSubscriptionManager().createSubscription(DEFAULT_SAMPLING_INTERVAL).get();
        for (NodeTarget target : targets) {
            subscribeNode(sub, target);
        }
    }

    private UaNode findMachinesFolder() throws Exception {
        return client.getAddressSpace()
                .browseNodes(Identifiers.ObjectsFolder)
                .stream()
                .filter(node -> "Machines".equals(node.getBrowseName().getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Machines folder not found in namespace."));
    }

    private void collectGroupNodes(UaNode currentNode,
                                   List<String> parentPath,
                                   List<NodeTarget> targets) throws Exception {
        if (currentNode.getNodeClass() != NodeClass.Object) {
            return;
        }

        List<String> currentPath = new ArrayList<>(parentPath);
        String nodeName = currentNode.getBrowseName().getName();
        String normalizedName = normalizeGroupName(nodeName);
        if (!normalizedName.isEmpty()) {
            currentPath.add(normalizedName);
        }
        String group = String.join(".", currentPath);

        for (UaNode child : client.getAddressSpace().browseNodes(currentNode.getNodeId())) {
            if (child instanceof UaVariableNode variable) {
                String tag = normalizeTag(group, variable.getBrowseName().getName());
                registerNode(group, tag, variable.getNodeId());
                if (!"command".equalsIgnoreCase(tag)) {
                    targets.add(new NodeTarget(variable.getNodeId(), group, tag));
                }
            } else if (child.getNodeClass() == NodeClass.Object) {
                collectGroupNodes(child, currentPath, targets);
            }
        }
    }

    private String normalizeTag(String group, String browseName) {
        String trimmed = stripRepeatedPrefix(browseName, group + ".");
        String[] parts = group.split("\\.");
        if (parts.length > 0) {
            String last = parts[parts.length - 1];
            trimmed = stripRepeatedPrefix(trimmed, last + ".");
        }
        return trimLeadingDot(trimmed);
    }

    private String normalizeGroupName(String browseName) {
        if (browseName == null) {
            return "";
        }
        int idx = browseName.lastIndexOf('.');
        return idx >= 0 ? browseName.substring(idx + 1) : browseName;
    }

    private String stripRepeatedPrefix(String value, String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return value;
        }
        String result = value;
        while (result.startsWith(prefix)) {
            result = result.substring(prefix.length());
        }
        return result;
    }

    private String trimLeadingDot(String value) {
        String trimmed = value;
        if (trimmed.startsWith(".")) {
            trimmed = trimmed.substring(1);
        }
        return trimmed;
    }

    private void registerNode(String group, String tag, NodeId nodeId) {
        String key = buildKey(group, tag);
        NodeId previous = nodeLookup.put(key, nodeId);
        if (previous != null && !previous.equals(nodeId)) {
            log.debug("Node mapping for {} replaced ({} -> {}).", key, previous, nodeId);
        } else {
            log.debug("Registered node {}.{} ({})", group, tag, nodeId);
        }
    }

    private String buildKey(String group, String tag) {
        return group + "|" + tag;
    }

    private void subscribeNode(UaSubscription sub, NodeTarget target) {
        String label = target.group() + "." + target.tag();
        try {
            UInteger clientHandle = Unsigned.uint(clientHandleSeq.getAndIncrement());
            ReadValueId rvid = new ReadValueId(target.nodeId(), AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
            MonitoringParameters params = new MonitoringParameters(
                    clientHandle,
                    DEFAULT_SAMPLING_INTERVAL,
                    null,
                    Unsigned.uint(10),
                    true
            );
            MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(rvid, MonitoringMode.Reporting, params);

            UaSubscription.ItemCreationCallback callback = (item, id) ->
                    item.setValueConsumer((it, value) -> {
                        Object payload = extractVariant(value);
                        log.debug("MONITOR {} -> {} (status={}, sourceTs={}, serverTs={})",
                                label,
                                payload,
                                value != null ? value.getStatusCode() : null,
                                value != null ? value.getSourceTime() : null,
                                value != null ? value.getServerTime() : null);

                        if (payload != null && isPayloadTag(target.tag())) {
                            log.info("ALARM PAYLOAD {} = {}", label, payload);
                        } else if (payload != null && isItemCodeTag(target.tag())) {
                            log.info("ITEM CODE UPDATE {} = {}", label, payload);
                        }

                        if (payload != null) {
                            mesApiService.sendMachineData(target.group(), target.tag(), payload);
                        }
                    });

            List<UaMonitoredItem> items = sub.createMonitoredItems(TimestampsToReturn.Both, List.of(request), callback).get();
            for (UaMonitoredItem item : items) {
                log.debug("Subscribed {} (status={})", label, item.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Subscription failed for {}: {}", label, e.getMessage(), e);
        }
    }

    private Object extractVariant(DataValue value) {
        if (value == null || value.getValue() == null) {
            return null;
        }
        Variant variant = value.getValue();
        if (variant.isNull()) {
            return null;
        }
        return coerceVariantValue(variant.getValue());
    }

    private Object coerceVariantValue(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof ByteString byteString) {
            if (byteString.isNull()) {
                return null;
            }
            return decodeBytes(byteString.bytes());
        }
        if (raw instanceof byte[] bytes) {
            return decodeBytes(bytes);
        }
        if (raw instanceof Variant[] variants) {
            List<Object> decoded = new ArrayList<>(variants.length);
            for (Variant variant : variants) {
                decoded.add(variant == null ? null : coerceVariantValue(variant.getValue()));
            }
            return decoded;
        }
        return raw;
    }

    private Object decodeBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return isLikelyText(bytes) ? new String(bytes, StandardCharsets.UTF_8) : bytes;
    }

    private boolean isLikelyText(byte[] bytes) {
        int sample = Math.min(bytes.length, 32);
        for (int i = 0; i < sample; i++) {
            int b = bytes[i] & 0xFF;
            if (b == 0) {
                return false;
            }
            if (b < 0x09 || (b > 0x0A && b < 0x20)) {
                return false;
            }
        }
        return true;
    }

    private boolean isPayloadTag(String tag) {
        return tag != null && tag.toLowerCase().contains("payload");
    }

    private boolean isItemCodeTag(String tag) {
        if (tag == null) {
            return false;
        }
        String normalized = tag.toLowerCase();
        return normalized.contains("item_code") || normalized.contains("itemcode");
    }

    public boolean sendLineCommand(String lineName,
                                   String action,
                                   String orderNo,
                                   Integer targetQty,
                                   Integer ppm,
                                   String itemCode) {
        if (lineName == null || lineName.isBlank() || action == null || action.isBlank()) {
            log.warn("Line command requires 'line' and 'action'. line={}, action={}", lineName, action);
            return false;
        }

        String normalizedAction = action.trim().toUpperCase();
        String command;

        switch (normalizedAction) {
            case "START" -> {
                if (orderNo == null || orderNo.isBlank() || targetQty == null) {
                    log.warn("START command requires orderNo and targetQty (line={}).", lineName);
                    return false;
                }
                if (itemCode == null || itemCode.isBlank()) {
                    log.warn("START command requires itemCode (line={}, orderNo={}).", lineName, orderNo);
                    return false;
                }
                if (ppm != null && ppm > 0) {
                    command = String.format("START:%s:%d:%s:%d", orderNo, targetQty, itemCode, ppm);
                } else {
                    command = String.format("START:%s:%d:%s", orderNo, targetQty, itemCode);
                }
                log.info("Preparing START command for {}: orderNo={}, targetQty={}, itemCode={}, ppm={}",
                        lineName, orderNo, targetQty, itemCode, ppm);
            }
            case "ACK", "STOP", "RESET" -> command = normalizedAction;
            default -> {
                log.warn("Unsupported line action '{}' for line {}.", action, lineName);
                return false;
            }
        }

        return writeValue(lineName, "command", command);
    }

    public boolean sendMachineCommand(String lineName, String machineName, String actionPayload) {
        if (machineName == null || machineName.isBlank()) {
            log.warn("Machine command requires machine name.");
            return false;
        }
        String group = (lineName != null && !lineName.isBlank())
                ? lineName + "." + machineName
                : machineName;
        return writeValue(group, "command", actionPayload);
    }

    public boolean writeValue(String group, String tag, Object newValue) {
        String key = buildKey(group, tag);
        NodeId nodeId = nodeLookup.get(key);
        if (nodeId == null) {
            log.warn("Node {}.{} not found in lookup table.", group, tag);
            return false;
        }

        try {
            DataValue value = new DataValue(new Variant(newValue));
            log.info("OPC WRITE REQUEST {}.{} = {}", group, tag, newValue);
            client.writeValue(nodeId, value).get();
            log.info("Wrote {}.{} = {}", group, tag, newValue);
            return true;
        } catch (Exception e) {
            log.error("Write failed for {}.{}: {}", group, tag, e.getMessage(), e);
            return false;
        }
    }

    public boolean writeValue(String nodePath, Object newValue) {
        if (nodePath == null) {
            return false;
        }
        int idx = nodePath.lastIndexOf('.');
        if (idx < 0) {
            log.warn("Invalid node path '{}'. Expected format group.tag.", nodePath);
            return false;
        }
        String group = nodePath.substring(0, idx);
        String tag = nodePath.substring(idx + 1);
        return writeValue(group, tag, newValue);
    }

    public CompletableFuture<OpcUaClient> disconnect() {
        return client != null ? client.disconnect() : CompletableFuture.completedFuture(null);
    }
}
