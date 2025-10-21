package com.synclab.miloclientgateway.controller;

import com.synclab.miloclientgateway.opcua.MiloOpcClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("api/v1/machines")
@RequiredArgsConstructor
public class CmdController {

    private final MiloOpcClient miloClient;

    /**
     * MES → Milo 제어 명령 API
     * 예시 요청:
     * POST /api/v1/machines/command
     * {
     *   "machine": "Machine1",
     *   "tag": "Command",
     *   "value": "START"
     * }
     */
    @PostMapping("/command")
    public ResponseEntity<?> sendCommand(@RequestBody Map<String, Object> req) {
        String machine = (String) req.get("machine");
        String tag = (String) req.get("tag");
        Object value = req.get("value");

        String nodeName = machine + "." + tag;
        log.info("📩 Command received → {} = {}", nodeName, value);

        boolean ok = miloClient.writeValue(nodeName, value);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "node", nodeName,
                    "value", value
            ));
        } else {
            return ResponseEntity.internalServerError().body(Map.of(
                    "status", "failed",
                    "node", nodeName
            ));
        }
    }
}
