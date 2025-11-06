package com.synclab.miloclientgateway.controller;

import com.synclab.miloclientgateway.opcua.MiloOpcClient;
import com.synclab.miloclientgateway.security.HmacVerifier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class CmdController {

    private final MiloOpcClient miloClient;

    @Value("${mes.auth.secret-key:}")
    private String mesSecretKey;

    /**
     * MES → Milo 제어 명령 API
     * 예시 요청:
     * POST /api/v1/orders/cmd?factoryCode=F001&lineCode=Line01
     * Authorization: Bearer {Base64(HMAC-SHA256(factory|line|action|orderNo|targetQty|itemCode|ppm))}
     *   "action": "START",
     *   "orderNo": "ORDER-12345",
     *   "targetQty": 150,
     *   "itemCode": "PRODUCT-001",
     *   "ppm": 120
     * }
     */
    @PostMapping("/orders/cmd")
    public ResponseEntity<?> sendCommand(@RequestHeader(value = "Authorization", required = false) String authorization,
                                         @RequestParam("factoryCode") String factoryCode,
                                         @RequestParam("lineCode") String lineCode,
                                         @RequestBody Map<String, Object> req) {
        String tokenHeader = stringValue(authorization);
        if (tokenHeader == null || tokenHeader.length() <= 7 || !tokenHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                    "status", "failed",
                    "reason", "Authorization header with Bearer token is required"
            ));
        }

        String factory = stringValue(factoryCode);
        String line = stringValue(lineCode);
        String action = stringValue(req.get("action"));
        String orderNo = stringValue(req.get("orderNo"));
        Integer targetQty = intValue(req.get("targetQty"));
        Integer ppm = intValue(req.get("ppm"));
        String itemCode = stringValue(req.get("itemCode"));
        String tag = Optional.ofNullable(stringValue(req.get("tag")))
                .orElse("command");

        if (mesSecretKey == null || mesSecretKey.isBlank()) {
            log.warn("MES authorization failed: missing shared secret");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                    "status", "failed",
                    "reason", "authorization secret misconfigured"
            ));
        }

        if (factory == null || line == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "failed",
                    "reason", "factoryCode and lineCode are required"
            ));
        }
        if (action == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "status", "failed",
                    "reason", "action is required"
            ));
        }

        String signaturePayload = buildSignaturePayload(factory, line, action, orderNo, targetQty, itemCode, ppm);
        String providedToken = tokenHeader.substring(7).trim();
        if (!HmacVerifier.verify(mesSecretKey, signaturePayload, providedToken)) {
            log.warn("MES authorization failed: signature mismatch");
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                    "status", "failed",
                    "reason", "invalid authorization token"
            ));
        }

        log.info("MES command received: factory={}, line={}, action={}, orderNo={}, targetQty={}, itemCode={}, ppm={}, rawBody={}",
                factory, line, action, orderNo, targetQty, itemCode, ppm, req);

        // 1) Line-level command (recommended flow)
        if (line != null && action != null && "command".equalsIgnoreCase(tag)) {
            if (itemCode == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "failed",
                        "reason", "itemCode is required"
                ));
            }
            if (orderNo == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "failed",
                        "reason", "orderNo is required for line commands"
                ));
            }
            if (targetQty == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "status", "failed",
                        "reason", "targetQty is required for line commands"
                ));
            }
            boolean ok = miloClient.sendLineCommand(line, action, orderNo, targetQty, ppm);
            if (ok) {
                return ResponseEntity.ok(Map.of(
                        "status", "success",
                        "factoryCode", factory,
                        "line", line,
                        "action", action,
                        "orderNo", orderNo,
                        "targetQty", targetQty,
                        "itemCode", itemCode
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
        String machine = stringValue(req.get("machine"));
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

    @PostMapping("/machines/command/test/start")
    public ResponseEntity<?> triggerTestStart() {
        String orderNo = "TEST-" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        boolean ok = miloClient.sendLineCommand("Line01", "START", orderNo, 40, null);
        if (ok) {
            return ResponseEntity.ok(Map.of(
                    "status", "started",
                    "orderNo", orderNo,
                    "targetQty", 40
            ));
        }
        return ResponseEntity.internalServerError().body(Map.of(
                "status", "failed",
                "action", "START"
        ));
    }

    @PostMapping("/machines/command/test/ack")
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

    @PostMapping("/machines/command/test/stop")
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

    private String buildSignaturePayload(String factory,
                                         String line,
                                         String action,
                                         String orderNo,
                                         Integer targetQty,
                                         String itemCode,
                                         Integer ppm) {
        return String.join("|",
                signatureComponent(factory),
                signatureComponent(line),
                signatureComponent(action),
                signatureComponent(orderNo),
                signatureComponent(targetQty),
                signatureComponent(itemCode),
                signatureComponent(ppm)
        );
    }

    private String signatureComponent(Object value) {
        return value == null ? "" : value.toString().trim();
    }
}
