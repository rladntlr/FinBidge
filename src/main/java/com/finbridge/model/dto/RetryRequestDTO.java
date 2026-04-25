package com.finbridge.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RetryRequestDTO {

    // 비워두면 원본 요청에서 실패/타임아웃된 프로토콜만 재처리한다.
    private List<String> protocols;
}
