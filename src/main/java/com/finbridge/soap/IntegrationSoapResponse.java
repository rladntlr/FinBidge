package com.finbridge.soap;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;

@Data
@XmlRootElement(name = "IntegrationResponse", namespace = "http://finbridge.com/soap")
@XmlAccessorType(XmlAccessType.FIELD)
public class IntegrationSoapResponse {

    private String status;
    private String message;
}
