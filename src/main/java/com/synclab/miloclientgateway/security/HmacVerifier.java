package com.synclab.miloclientgateway.security;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Utility for computing and verifying HMAC-based signatures shared between MES and Milo-Client.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HmacVerifier {

    private static final String HMAC_SHA256 = "HmacSHA256";

    public static boolean verify(String secretKey, String payload, String providedSignature) {
        if (secretKey == null || secretKey.isBlank()) {
            log.warn("HMAC verification skipped: empty secret key");
            return false;
        }
        if (payload == null || providedSignature == null || providedSignature.isBlank()) {
            log.debug("HMAC verification failed: missing payload or signature");
            return false;
        }
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), HMAC_SHA256));
            byte[] digest = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
            String expectedSignature = Base64.getEncoder().encodeToString(digest);

            return constantTimeEquals(expectedSignature, providedSignature.trim());
        } catch (Exception e) {
            log.error("HMAC verification error: {}", e.getMessage(), e);
            return false;
        }
    }

    private static boolean constantTimeEquals(String left, String right) {
        byte[] leftBytes = left.getBytes(StandardCharsets.UTF_8);
        byte[] rightBytes = right.getBytes(StandardCharsets.UTF_8);
        if (leftBytes.length != rightBytes.length) {
            return false;
        }

        int result = 0;
        for (int i = 0; i < leftBytes.length; i++) {
            result |= leftBytes[i] ^ rightBytes[i];
        }
        return result == 0;
    }
}

