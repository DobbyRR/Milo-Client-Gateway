package com.synclab.miloclientgateway.opcua;

import com.synclab.miloclientgateway.mes.MesApiService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.client.nodes.UaVariableNode;
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
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class MiloOpcClient {

    private OpcUaClient client;
    private static final String ENDPOINT = "opc.tcp://192.168.0.38:4840/milo";
    private final AtomicLong clientHandleSeq = new AtomicLong(1);

    @Autowired
    private MesApiService mesApiService;

    @PostConstruct
    public void connect() {
        try {
            client = OpcUaClient.create(ENDPOINT);
            client.connect().get();
            log.info(" Connected to Milo Server at {}", ENDPOINT);

            // 전체 Machine 자동 구독 시작
//            browseAndSubscribeAll();
            startUpSnapshotAndSubscribe();   // 스냅샷 + 구독

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

    private void startUpSnapshotAndSubscribe() throws Exception {
        // 0) Machines 폴더 찾기
        var roots = client.getAddressSpace().browseNodes(Identifiers.ObjectsFolder);
        UaNode machinesFolder = roots.stream()
                .filter(n -> "Machines".equals(n.getBrowseName().getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Machines folder not found"));

        // 1) Machines 하위 변수 노드 수집
        record VarRef(String machine, UaNode var) {}
        java.util.List<VarRef> vars = new java.util.ArrayList<>();
        for (UaNode machine : client.getAddressSpace().browseNodes(machinesFolder.getNodeId())) {
            String mName = machine.getBrowseName().getName();
            for (UaNode var : client.getAddressSpace().browseNodes(machine.getNodeId())) {
                if (var instanceof UaVariableNode) vars.add(new VarRef(mName, var));
            }
        }

        // 2) 스냅샷 일괄 읽기
        java.util.List<NodeId> nodeIds = vars.stream().map(v -> v.var().getNodeId()).toList();
        java.util.List<DataValue> values =
                client.readValues(0, TimestampsToReturn.Both, nodeIds).get();

        // 3) MES로 1회 전송 + 로그
        for (int i = 0; i < vars.size(); i++) {
            VarRef ref = vars.get(i);
            Object v = values.get(i).getValue() != null ? values.get(i).getValue().getValue() : null;
            mesApiService.sendMachineData(ref.machine(), ref.var().getBrowseName().getName(), v);
            log.info("SNAPSHOT {}.{} = {}", ref.machine(), ref.var().getBrowseName().getName(), v);
        }

        // 4) 변경 구독 생성
        UaSubscription sub = client.getSubscriptionManager().createSubscription(1000.0).get();
        for (VarRef ref : vars) {
            String fullName = ref.machine() + "." + ref.var().getBrowseName().getName();
            subscribeNode(sub, ref.var().getNodeId(), fullName);
        }
    }

    /** 개별 노드 구독 */
    private void subscribeNode(UaSubscription sub, NodeId nodeId, String fullNodeName) {
        try {
            UInteger clientHandle = Unsigned.uint(clientHandleSeq.getAndIncrement());
            ReadValueId rvid = new ReadValueId(nodeId, AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
            MonitoringParameters params = new MonitoringParameters(clientHandle, 1000.0, null, Unsigned.uint(10), true);
            MonitoredItemCreateRequest req = new MonitoredItemCreateRequest(rvid, MonitoringMode.Reporting, params);

            UaSubscription.ItemCreationCallback cb = (item, id) ->
                    item.setValueConsumer((it, value) -> {
                        log.info(
                                "MONITOR {} -> {} (status={}, sourceTs={}, serverTs={})",
                                fullNodeName,
                                value != null ? value.getValue() : null,
                                value != null ? value.getStatusCode() : null,
                                value != null ? value.getSourceTime() : null,
                                value != null ? value.getServerTime() : null
                        );

                        if (value == null || value.getValue() == null) {
                            return;
                        }

                        Object v = value.getValue().getValue();
                        String[] parts = fullNodeName.split("\\.");
                        if (parts.length == 2) {
                            mesApiService.sendMachineData(parts[0], parts[1], v);
                        }
                    });

            List<UaMonitoredItem> items = sub.createMonitoredItems(TimestampsToReturn.Both, List.of(req), cb).get();
            for (UaMonitoredItem item : items) {
                log.info("📡 Subscribed {} (status={})", fullNodeName, item.getStatusCode());
            }
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
