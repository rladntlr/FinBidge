package com.finbridge.service.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finbridge.model.dto.AdapterExecutionConfig;
import com.finbridge.model.dto.ProtocolResultDTO;
import com.finbridge.model.enums.ResultStatus;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

@Service
@Slf4j
@RequiredArgsConstructor
public class SftpAdapterService implements ProtocolAdapter {

    private final ObjectMapper objectMapper;

    @Value("${sftp.host}")
    private String host;

    @Value("${sftp.port}")
    private int port;

    @Value("${sftp.username}")
    private String username;

    @Value("${sftp.password}")
    private String password;

    @Value("${sftp.upload-dir}")
    private String uploadDir;

    @Value("${sftp.strict-host-key-checking:yes}")
    private String strictHostKeyChecking;

    @Value("${sftp.known-hosts:}")
    private String knownHosts;

    @Override
    public ProtocolResultDTO execute(String requestId, Map<String, Object> payload) {
        return execute(requestId, payload, null);
    }

    @Override
    public ProtocolResultDTO execute(
            String requestId,
            Map<String, Object> payload,
            AdapterExecutionConfig config
    ) {
        long startTime = System.currentTimeMillis();
        String filename = "finbridge-" + requestId + ".json";
        String targetUploadDir = endpointOrDefault(config, uploadDir);
        int timeoutMs = timeoutOrDefault(config, 10_000);
        log.info("[SFTP] {} - 파일 업로드 시작: {}, uploadDir={}, timeoutMs={}",
                requestId, filename, targetUploadDir, timeoutMs);

        Session session = null;
        ChannelSftp channel = null;

        try {
            String json = objectMapper.writeValueAsString(payload);
            byte[] content = json.getBytes(StandardCharsets.UTF_8);

            JSch jsch = new JSch();
            if ("yes".equalsIgnoreCase(strictHostKeyChecking)) {
                if (knownHosts == null || knownHosts.isBlank()) {
                    throw new IllegalStateException("sftp.known-hosts 설정이 필요합니다.");
                }
                jsch.setKnownHosts(knownHosts);
            }

            session = jsch.getSession(username, host, port);
            session.setPassword(password);

            Properties sessionConfig = new Properties();
            sessionConfig.put("StrictHostKeyChecking", strictHostKeyChecking);
            session.setConfig(sessionConfig);
            session.connect(timeoutMs);

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(timeoutMs);
            channel.put(new ByteArrayInputStream(content), targetUploadDir + "/" + filename);

            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.info("[SFTP] {} - 업로드 성공: {} ({}ms)", requestId, filename, executionTimeMs);

            return new ProtocolResultDTO(ResultStatus.SUCCESS, "200",
                    "파일 업로드 완료: " + targetUploadDir + "/" + filename, executionTimeMs);

        } catch (Exception e) {
            long executionTimeMs = System.currentTimeMillis() - startTime;
            log.error("[SFTP] {} - 실패: {}", requestId, e.getMessage());

            return new ProtocolResultDTO(ResultStatus.FAILED, "500", e.getMessage(), executionTimeMs);

        } finally {
            if (channel != null && channel.isConnected()) channel.disconnect();
            if (session != null && session.isConnected()) session.disconnect();
        }
    }

    private String endpointOrDefault(AdapterExecutionConfig config, String defaultEndpoint) {
        return config != null && config.getEndpoint() != null && !config.getEndpoint().isBlank()
                ? config.getEndpoint()
                : defaultEndpoint;
    }

    private int timeoutOrDefault(AdapterExecutionConfig config, int defaultTimeoutMs) {
        return config != null && config.getTimeoutMs() != null && config.getTimeoutMs() > 0
                ? config.getTimeoutMs()
                : defaultTimeoutMs;
    }
}
