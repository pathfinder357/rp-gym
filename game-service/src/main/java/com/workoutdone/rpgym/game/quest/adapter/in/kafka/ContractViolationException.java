package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

// Health 가 보낸 메시지가 이벤트 계약을 어길때를 대비한것. 깨진 JSON, 필수 필드 누락, data 변환 실패.
// 몇 번을 다시 읽어도 같은 자리에서 실패하므로 재시도하지 않고 곧바로 DLT 로 보낸다
// (KafkaConsumerConfig 가 재시도 불가 예외로 등록한다).
// 예전에는 로그만 남기고 정상 리턴했다. 그러면 오프셋이 커밋되어 메시지가 로그 한 줄만 남기고 사라진다.
// 예외를 던져야만 에러 핸들러가 메시지를 DLT 로 옮길 수 있다.
public class ContractViolationException extends RuntimeException {

    public ContractViolationException(String message) {
        super(message);
    }

    public ContractViolationException(String message, Throwable cause) {
        super(message, cause);
    }
}
