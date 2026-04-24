package com.finbridge.soap;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ws.server.endpoint.annotation.Endpoint;
import org.springframework.ws.server.endpoint.annotation.PayloadRoot;
import org.springframework.ws.server.endpoint.annotation.RequestPayload;
import org.springframework.ws.server.endpoint.annotation.ResponsePayload;

@Endpoint
@Slf4j
public class SoapEndpoint {

    private static final String NAMESPACE_URI = "http://finbridge.com/soap";

    @PayloadRoot(namespace = NAMESPACE_URI, localPart = "IntegrationRequest")
    @ResponsePayload
    public IntegrationSoapResponse processIntegration(@RequestPayload IntegrationSoapRequest request) {
        log.info("[SOAP Endpoint] 요청 수신: requestId={}", request.getRequestId());

        IntegrationSoapResponse response = new IntegrationSoapResponse();
        response.setStatus("SUCCESS");
        response.setMessage("레거시 시스템 처리 완료: " + request.getRequestId());

        log.info("[SOAP Endpoint] 처리 완료: requestId={}", request.getRequestId());
        return response;
    }
}
