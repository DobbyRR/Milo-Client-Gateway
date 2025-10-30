package com.synclab.miloclientgateway.controller;

import com.synclab.miloclientgateway.opcua.MiloOpcClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

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
        String line = stringValue(req.get("line"));
        String machine = stringValue(req.get("machine"));
        String action = stringValue(req.get("action"));
        String tag = Optional.ofNullable(stringValue(req.get("tag")))
                .orElse("command");

        // 1) Line-level command (recommended flow)
        if (line != null && action != null && machine == null && "command".equalsIgnoreCase(tag)) {
            String orderNo = stringValue(req.get("orderNo"));
            Integer targetQty = intValue(req.get("targetQty"));
            Integer ppm = intValue(req.get("ppm"));

            boolean ok = miloClient.sendLineCommand(line, action, orderNo, targetQty, ppm);
            if (ok) {
                return ResponseEntity.ok(Map.of(
                        "status", "success",
                        "line", line,
                        "action", action,
                        "orderNo", orderNo,
                        "targetQty", targetQty
                ));
            }
            return ResponseEntity.internalServerError().body(Map.of(
                    "status", "failed",
                    "reason", "line command rejected",
                    "line", line,
                    "action", action
            ));
        }

        // 2) Machine-level or direct node write (fallback for diagnostics)
        Object value = req.getOrDefault("value", action);
        if (value == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "failed",
                    "reason", "value or action must be provided"
            ));
        }

        String nodePath;
        if (machine != null) {
            if (line != null) {
                nodePath = line + "." + machine + "." + tag;
            } else {
                nodePath = machine + "." + tag;
            }
        } else if (line != null) {
            nodePath = line + "." + tag;
        } else {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "failed",
                    "reason", "machine or line must be specified"
            ));
        }

        log.info("Command write → {} = {}", nodePath, value);
        boolean ok = miloClient.writeValue(nodePath, value);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "success",
                    "node", nodePath,
                    "value", value
            ));
        }
        return ResponseEntity.internalServerError().body(Map.of(
                "status", "failed",
                "node", nodePath
        ));
    }

    // -------- 테스트용 라인 지령 엔드포인트 (MES 미연동 시 수동 동작 확인) --------

    @PostMapping("/command/test/start")
    public ResponseEntity<?> triggerTestStart() {
        String orderNo = "TEST-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        boolean ok = miloClient.sendLineCommand("Line01", "START", orderNo, 100, null);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "started",
                    "orderNo", orderNo,
                    "targetQty", 100
            ));
        }
        return ResponseEntity.internalServerError().body(Map.of(
                "status", "failed",
                "action", "START"
        ));
    }

    @PostMapping("/command/test/ack")
    public ResponseEntity<?> triggerTestAck() {
        boolean ok = miloClient.sendLineCommand("Line01", "ACK", null, null, null);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "acked"
            ));
        }
        return ResponseEntity.internalServerError().body(Map.of(
                "status", "failed",
                "action", "ACK"
        ));
    }

    @PostMapping("/command/test/stop")
    public ResponseEntity<?> triggerTestStop() {
        boolean ok = miloClient.sendLineCommand("Line01", "STOP", null, null, null);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "stopped"
            ));
        }
        return ResponseEntity.internalServerError().body(Map.of(
                "status", "failed",
                "action", "STOP"
        ));
    }

    private String stringValue(Object value) {
        if (value instanceof String s) {
            return s.trim().isEmpty() ? null : s.trim();
        }
        return null;
    }

    private Integer intValue(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String s && !s.isBlank()) {
            try {
                return Integer.parseInt(s.trim());
            } catch (NumberFormatException ignore) {
                log.warn("Unable to parse integer from '{}'", s);
            }
        }
        return null;
    }
}
