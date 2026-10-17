package com.workoutdone.rpgym.game.outbox.application;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 저장 형식은 "전파 헤더 그대로"다. Propagator 가 Kafka 헤더에 쓰는 것과 같은 키 · 값을 JSON 으로 둔다
 * (기본 W3C 면 {"traceparent": "..."}). 전파 방식(W3C · B3)을 바꿔도 이 코드는 바뀌지 않는다.
 *
 * 발행 때는 저장한 context 를 부모로 "outbox publish" span 을 열고 그 안에서 KafkaTemplate 을 부른다.
 * KafkaTemplate 의 observation(application.yml template.observation-enabled)이 이 span 의 자식을 만들고
 * 헤더에 실으므로, 소비자(Notification)까지 같은 traceId 로 이어진다.
 * span 이 "적재 ~ 발행" 사이의 대기(폴링 주기)를 직접 재지는 않는다. 그 시간은 부모 span 이 끝난 뒤의 간격으로 보인다.
 *
 * 샘플링: Brave 는 부모의 결정(sampled 플래그)을 따른다. 운영에서 TRACING_SAMPLING_PROBABILITY 를 낮춰도
 * 시작 지점에서 뽑힌 trace 는 Outbox 를 지나 끝까지 이어지고, 안 뽑힌 trace 는 중간에 새로 뽑히지 않는다.
 *
 * Tracer · Propagator 빈이 없는 환경(단위 테스트 등)에서는 아무것도 하지 않는다.
 */
@Slf4j
@Component
public class OutboxTraceContext {

    private static final TypeReference<Map<String, String>> MAP = new TypeReference<>() {
    };

    private final ObjectProvider<Tracer> tracer;
    private final ObjectProvider<Propagator> propagator;
    private final ObjectMapper objectMapper;

    public OutboxTraceContext(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator, ObjectMapper objectMapper) {
        this.tracer = tracer;
        this.propagator = propagator;
        this.objectMapper = objectMapper;
    }

    /** 지금 스레드의 trace context 를 전파 헤더 JSON 으로. 없으면 null */
    public String capture() {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        if (t == null || p == null) {
            return null;
        }
        TraceContext context = t.currentTraceContext().context();
        if (context == null) {
            return null;
        }
        Map<String, String> carrier = new LinkedHashMap<>();
        p.inject(context, carrier, Map::put);
        if (carrier.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(carrier);
        } catch (Exception e) {
            // trace 는 부가 정보다. 실패해도 적재(업무 트랜잭션)를 막지 않는다
            log.debug("trace context 직렬화 실패. 이 이벤트는 새 trace 로 발행된다", e);
            return null;
        }
    }

    /** 저장해 둔 context 를 부모로 span 을 열고 그 안에서 실행한다. 저장값이 없으면 그냥 실행한다 */
    public void runInContext(String stored, String spanName, Runnable action) {
        Tracer t = tracer.getIfAvailable();
        Propagator p = propagator.getIfAvailable();
        Map<String, String> carrier = parse(stored);
        if (t == null || p == null || carrier == null) {
            action.run();
            return;
        }
        Span span = p.extract(carrier, Map::get).name(spanName).start();
        try (Tracer.SpanInScope ignored = t.withSpan(span)) {
            action.run();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    private Map<String, String> parse(String stored) {
        if (stored == null || stored.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(stored, MAP);
        } catch (Exception e) {
            log.debug("저장된 trace context 를 읽지 못했다. 새 trace 로 발행한다. value={}", stored);
            return null;
        }
    }
}
