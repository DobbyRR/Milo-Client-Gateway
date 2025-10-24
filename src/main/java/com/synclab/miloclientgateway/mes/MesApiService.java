package com.synclab.miloclientgateway.mes;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpHeaders;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class MesApiService {

    private final RestTemplate restTemplate = new RestTemplate();

    // MES 수집 서버의 엔드포인트 URL
    private static final String MES_API_URL = "http://localhost:8081/api/v1/machines/data";

    /**
     * MiloClient → MES 데이터 전달
     *
     * @param machineName ex) "Machine1"
     * @param tagName     ex) "Temperature"
     * @param value       ex) 24.5
     */
    public void sendMachineData(String machineName, String tagName, Object value) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("machine", machineName);
            payload.put("tag", tagName);
            payload.put("value", value);
            payload.put("timestamp", System.currentTimeMillis());

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(MES_API_URL, request, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info(" Sent to MES → {}.{} = {}", machineName, tagName, value);
            } else {
                log.warn(" MES responded with status: {}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error(" Failed to send data to MES: {}", e.getMessage());
        }
    }
}
