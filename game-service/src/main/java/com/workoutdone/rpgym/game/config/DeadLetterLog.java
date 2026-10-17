package com.workoutdone.rpgym.game.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// B-14 에서 DLT 알림을 받고 Loki 에서 eventId · userId · 예외 이름으로 찾았더니 0줄이었다.
// DLT 발행을 직접 만든 recoverer 로 바꾸면서 스프링 기본 처리기가 찍던 "재시도 소진" ERROR 가 사라졌고,
// 리스너에도 로그가 없었기 때문이다. 원인은 DLT 헤더에만 있었다 -> 콘솔로 토픽을 직접 열어야 했다.
//
// 한 줄에 위치(topic-partition@offset) · key(userId) · eventId · 근본 원인을 넣는다.
// 스택 트레이스는 넣지 않는다. 여러 줄이 되면 Loki 에서 줄 단위 검색이 깨지고,
// 스택 트레이스는 DLT 헤더(kafka_dlt-exception-stacktrace)에 이미 남는다.
final class DeadLetterLog {

    // 깨진 JSON 에서도 eventId 만은 건질 수 있게 파싱하지 않고 정규식으로 찾는다.
    private static final Pattern EVENT_ID = Pattern.compile("\"eventId\"\\s*:\\s*\"([^\"]{1,64})\"");
    private static final int MAX_MESSAGE = 300;

    private DeadLetterLog() {
    }

    static String describe(ConsumerRecord<?, ?> record) {
        return "%s-%d@%d key=%s eventId=%s".formatted(
                record.topic(), record.partition(), record.offset(), record.key(), eventId(record.value()));
    }

    static String eventId(Object value) {
        if (value instanceof String json) {
            Matcher m = EVENT_ID.matcher(json);
            if (m.find()) {
                return m.group(1);
            }
        }
        return "-";
    }

    // 리스너 예외는 ListenerExecutionFailedException 으로 감싸져 오므로 맨 안쪽 원인을 쓴다.
    static String cause(Throwable ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = String.valueOf(root.getMessage()).replaceAll("\\s+", " ");
        if (message.length() > MAX_MESSAGE) {
            message = message.substring(0, MAX_MESSAGE) + "...";
        }
        return root.getClass().getName() + ": " + message;
    }
}
