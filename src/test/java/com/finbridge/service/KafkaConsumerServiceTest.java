package com.finbridge.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * KafkaConsumerService 단위 테스트.
 *
 * - KafkaConsumerService는 외부 의존성 없이 순수 로직만 가지므로 Mock 불필요.
 * - consume()은 유효하지 않은 메시지에서 IllegalArgumentException을 던진다.
 * - consumeDlt()는 내부에서 예외를 catch하므로 어떤 입력에도 throw하지 않는다.
 */
class KafkaConsumerServiceTest {

    private KafkaConsumerService kafkaConsumerService;

    @BeforeEach
    void setUp() {
        kafkaConsumerService = new KafkaConsumerService();
    }

    // =========================================================================
    // consume()
    // =========================================================================

    @Test
    @DisplayName("유효한 메시지('requestId|payload')는 예외 없이 처리된다")
    void consume_validMessage_doesNotThrow() {
        assertThatCode(() -> kafkaConsumerService.consume("req-001|{\"key\":\"value\"}"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("파이프(|)가 없는 메시지는 IllegalArgumentException을 던진다")
    void consume_noPipe_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> kafkaConsumerService.consume("invalidmessage"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("형식이 올바르지 않습니다");
    }

    @Test
    @DisplayName("null 메시지는 IllegalArgumentException을 던진다")
    void consume_nullMessage_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> kafkaConsumerService.consume(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("비어 있습니다");
    }

    @Test
    @DisplayName("공백만 있는 메시지는 IllegalArgumentException을 던진다")
    void consume_blankMessage_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> kafkaConsumerService.consume("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("비어 있습니다");
    }

    @Test
    @DisplayName("requestId가 빈 문자열인 메시지('|payload')는 IllegalArgumentException을 던진다")
    void consume_blankRequestId_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> kafkaConsumerService.consume("|some-payload"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requestId를 파싱할 수 없습니다");
    }

    @Test
    @DisplayName("payload 안에 파이프가 추가로 있어도 requestId는 첫 세그먼트만 사용한다")
    void consume_payloadContainsPipes_onlyFirstSegmentIsRequestId() {
        // "req-abc|part1|part2|part3" → requestId = "req-abc" (split limit=2 덕분)
        assertThatCode(() -> kafkaConsumerService.consume("req-abc|part1|part2|part3"))
                .doesNotThrowAnyException();
    }

    // =========================================================================
    // consumeDlt()
    // =========================================================================

    @Test
    @DisplayName("DLT 메시지가 유효하면 예외 없이 처리된다")
    void consumeDlt_validMessage_doesNotThrow() {
        assertThatCode(() -> kafkaConsumerService.consumeDlt("req-dlt|{\"error\":\"timeout\"}"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("DLT 메시지가 형식 오류여도 내부 catch 처리로 예외가 외부로 전파되지 않는다")
    void consumeDlt_invalidMessage_doesNotThrowOutside() {
        assertThatCode(() -> kafkaConsumerService.consumeDlt("no-pipe-here"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("DLT 메시지가 null이어도 내부 catch 처리로 예외가 외부로 전파되지 않는다")
    void consumeDlt_nullMessage_doesNotThrowOutside() {
        assertThatCode(() -> kafkaConsumerService.consumeDlt(null))
                .doesNotThrowAnyException();
    }
}
