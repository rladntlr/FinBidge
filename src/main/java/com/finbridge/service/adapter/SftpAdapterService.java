package com.finbridge.service.adapter;

import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.entity.ProtocolResult;
import com.finbridge.model.enums.ProtocolType;
import com.finbridge.model.enums.ResultStatus;
import com.finbridge.repository.ProtocolResultRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class SftpAdapterService implements ProtocolAdapter {

    private final ProtocolResultRepository protocolResultRepository;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        long startTime = System.currentTimeMillis();
        String filename = "finbridge-" + requestId + ".json";
        log.info("[SFTP] {} - 파일 업로드 시작: {}", requestId, filename);

        try {
            // JSch SFTP 업로드 모의 (500ms 네트워크 지연)
            Thread.sleep(500);

            // 로컬 /tmp에 파일 저장
            Path path = Paths.get("/tmp/" + filename);
            Files.writeString(path, payload.toString());

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[SFTP] {} - 업로드 성공: {} ({}ms)", requestId, path, executionTimeMs);

            saveResult(requestId, ResultStatus.SUCCESS, "200",
                    "파일 업로드 완료: " + filename, executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "파일 업로드 완료: " + filename, executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[SFTP] {} - 실패: {}", requestId, e.getMessage());

            saveResult(requestId, ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.FAILED, "500",
                    e.getMessage(), executionTimeMs);
        }
    }

    private void saveResult(String requestId, ResultStatus status,
                            String code, String message, Long executionTimeMs) {
        ProtocolResult result = new ProtocolResult();
        result.setRequestId(requestId);
        result.setProtocol(ProtocolType.SFTP);
        result.setStatus(status);
        result.setResponseCode(code);
        result.setResponseMessage(message);
        result.setExecutionTimeMs(executionTimeMs);
        protocolResultRepository.save(result);
    }
}
