package com.workoutdone.rpgym.game.outbox.application;

import brave.Tracing;
import brave.propagation.StrictCurrentTraceContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.brave.bridge.BraveBaggageManager;
import io.micrometer.tracing.brave.bridge.BraveCurrentTraceContext;
import io.micrometer.tracing.brave.bridge.BravePropagator;
import io.micrometer.tracing.brave.bridge.BraveTracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxTraceContextTest {

    private Tracing tracing;
    private Tracer tracer;
    private OutboxTraceContext sut;

    @BeforeEach
    void setUp() {
        tracing = Tracing.newBuilder().currentTraceContext(StrictCurrentTraceContext.create()).build();
        tracer = new BraveTracer(tracing.tracer(), new BraveCurrentTraceContext(tracing.currentTraceContext()), new BraveBaggageManager());
        Propagator propagator = new BravePropagator(tracing);
        StaticListableBeanFactory beans = new StaticListableBeanFactory();
        beans.addBean("tracer", tracer);
        beans.addBean("propagator", propagator);
        sut = new OutboxTraceContext(beans.getBeanProvider(Tracer.class), beans.getBeanProvider(Propagator.class), new ObjectMapper());
    }

    @AfterEach
    void tearDown() {
        tracing.close();
    }

    @DisplayName("R-17: 적재 때 저장한 context 로 발행하면 같은 traceId 의 자식 span 안에서 실행된다")
    @Test
    void publishContinuesStoredTrace() {
        Span request = tracer.nextSpan().name("consume health event").start();
        String stored;
        try (Tracer.SpanInScope ignored = tracer.withSpan(request)) {
            stored = sut.capture();
        } finally {
            request.end();
        }
        assertThat(stored).isNotBlank();

        // 폴링 스레드: trace 가 없는 상태에서 시작
        assertThat(tracer.currentSpan()).isNull();
        AtomicReference<String> traceIdInside = new AtomicReference<>();
        AtomicReference<String> parentInside = new AtomicReference<>();
        sut.runInContext(stored, "outbox publish", () -> {
            traceIdInside.set(tracer.currentSpan().context().traceId());
            parentInside.set(tracer.currentSpan().context().parentId());
        });

        assertThat(traceIdInside.get()).isEqualTo(request.context().traceId());
        assertThat(parentInside.get()).isEqualTo(request.context().spanId());
        assertThat(tracer.currentSpan()).isNull();
    }

    @DisplayName("R-17: 저장값이 없거나 trace 가 없으면 그냥 실행한다(예전 동작)")
    @Test
    void noContextRunsPlainly() {
        assertThat(sut.capture()).isNull();
        AtomicReference<Boolean> ran = new AtomicReference<>(false);
        sut.runInContext(null, "outbox publish", () -> ran.set(true));
        sut.runInContext("not json", "outbox publish", () -> ran.set(true));
        assertThat(ran.get()).isTrue();
    }
}
