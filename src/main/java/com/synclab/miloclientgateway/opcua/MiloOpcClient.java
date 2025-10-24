package com.synclab.miloclientgateway.opcua;

import com.synclab.miloclientgateway.mes.MesApiService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
public class MiloOpcClient {

    private OpcUaClient client;
    private static final String ENDPOINT = "opc.tcp://192.168.0.18:4840/milo";

    @Autowired
    private MesApiService mesApiService;

    @PostConstruct
    public void connect() {
        try {
            client = OpcUaClient.create(ENDPOINT);
            client.connect().get();
            log.info("✅ Connected to Milo Server at {}", ENDPOINT);

            // 전체 Machine 자동 구독 시작
            browseAndSubscribeAll();

        } catch (Exception e) {
            log.error(" OPC UA connection failed: {}", e.getMessage(), e);
        }
    }

    /** 모든 Machine 노드 탐색 및 구독 */
    private void browseAndSubscribeAll() throws Exception {
        // ns0 ObjectsFolder에서 "Machines" 찾기
        var roots = client.getAddressSpace().browseNodes(Identifiers.ObjectsFolder);
        UaNode machinesFolder = roots.stream()
                .filter(n -> "Machines".equals(n.getBrowseName().getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Machines folder not found"));

        UaSubscription sub = client.getSubscriptionManager().createSubscription(1000.0).get();

        // Machines 하위 설비 폴더 순회
        for (UaNode machine : client.getAddressSpace().browseNodes(machinesFolder.getNodeId())) {
            log.info("📂 Machine: {}", machine.getBrowseName().getName());

            // 설비 하위 변수 노드 순회
            for (UaNode var : client.getAddressSpace().browseNodes(machine.getNodeId())) {
                NodeId nodeId = var.getNodeId();                 // ✅ 실제 NodeId 사용
                String fullName = machine.getBrowseName().getName() + "." + var.getBrowseName().getName();
                subscribeNode(sub, nodeId, fullName);            // ✅ 재사용 구독
            }
        }
    }

    /** 개별 노드 구독 */
    private void subscribeNode(UaSubscription sub, NodeId nodeId, String fullNodeName) {
        try {
            UInteger clientHandle = Unsigned.uint(Math.abs(fullNodeName.hashCode()));
            ReadValueId rvid = new ReadValueId(nodeId, AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
            MonitoringParameters params = new MonitoringParameters(clientHandle, 1000.0, null, Unsigned.uint(10), true);
            MonitoredItemCreateRequest req = new MonitoredItemCreateRequest(rvid, MonitoringMode.Reporting, params);

            UaSubscription.ItemCreationCallback cb = (item, id) ->
                    item.setValueConsumer((it, value) -> {
                        if (value == null || value.getValue() == null) return;
                        Object v = value.getValue().getValue();
                        log.info("[{}] {} = {}", ENDPOINT, fullNodeName, v);
                        String[] parts = fullNodeName.split("\\.");
                        if (parts.length == 2) mesApiService.sendMachineData(parts[0], parts[1], v);
                    });

            sub.createMonitoredItems(TimestampsToReturn.Both, List.of(req), cb).get();
            log.info("📡 Subscribed {}", fullNodeName);
        } catch (Exception e) {
            log.error("Subscription failed {}: {}", fullNodeName, e.getMessage(), e);
        }
    }


    /** 단일 노드 쓰기 (MES → Milo) */
    public boolean writeValue(String nodeName, Object newValue) {
        try {
            NodeId nodeId = new NodeId(2, nodeName);
            DataValue value = new DataValue(new Variant(newValue));
            client.writeValue(nodeId, value).get();
            log.info(" Wrote {} = {}", nodeName, newValue);
            return true;
        } catch (Exception e) {
            log.error("Write failed: {}", e.getMessage());
            return false;
        }
    }

    public CompletableFuture<OpcUaClient> disconnect() {
        return client != null
                ? client.disconnect()
                : CompletableFuture.completedFuture(null);
    }
}
