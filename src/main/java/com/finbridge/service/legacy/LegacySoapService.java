package com.finbridge.service.legacy;

import com.finbridge.soap.IntegrationSoapRequest;
import com.finbridge.soap.IntegrationSoapResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LegacySoapService {

    public IntegrationSoapResponse process(IntegrationSoapRequest request) {
        log.info("[SOAP Legacy] 요청 처리: requestId={}", request.getRequestId());

        IntegrationSoapResponse response = new IntegrationSoapResponse();
        response.setStatus("SUCCESS");
        response.setMessage("레거시 시스템 처리 완료: " + request.getRequestId());
        return response;
    }
}
