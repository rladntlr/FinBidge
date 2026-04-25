package com.finbridge.soap;

import com.finbridge.service.legacy.LegacySoapService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

@Endpoint
@Slf4j
@RequiredArgsConstructor
public class SoapEndpoint {

    private static final String NAMESPACE_URI = "http://finbridge.com/soap";
    private final LegacySoapService legacySoapService;

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "IntegrationRequest")
    @ResponsePayload
    public IntegrationSoapResponse processIntegration(@RequestPayload IntegrationSoapRequest request) {
        log.info("[SOAP Endpoint] 요청 수신: requestId={}", request.getRequestId());

        IntegrationSoapResponse response = legacySoapService.process(request);
        log.info("[SOAP Endpoint] 처리 완료: requestId={}", request.getRequestId());
        return response;
    }
}
