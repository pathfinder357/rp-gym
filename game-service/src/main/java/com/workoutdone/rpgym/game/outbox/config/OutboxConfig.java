package com.workoutdone.rpgym.game.outbox.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Outbox 발행기가 이 서비스의 유일한 스케줄 컴포넌트이므로 @EnableScheduling을 여기서 켠다.
 * 다른 곳에 스케줄 작업이 생기면 그때 config로 옮긴다.
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(OutboxPublishProperties.class)
public class OutboxConfig {

	/**
	 * (R-14) Game 이 발행하는 토픽을 코드로 선언한다. 예전에는 첫 발행 때 브로커가 자동 생성했다.
	 * 파티션 3 = 컨슈머 concurrency 3 과 같은 기준. 복제 계수는 브로커 기본값을 따른다(1대면 1, 3대면 3).
	 * 이미 있으면 KafkaAdmin 은 아무것도 바꾸지 않는다.
	 */
	@Bean
	public NewTopic gameEventsTopic(@Value("${rpgym.outbox.topic}") String topic) {
		return TopicBuilder.name(topic).partitions(3).build();
	}

	// escalateAfter default 값은 10
	// application.yml에 10이 있다.
	// rpgym.outbox.escalate-after: 10
	// retryCount >= escalaterAfter log.warn -> log.error
	// status == PENDING
}
