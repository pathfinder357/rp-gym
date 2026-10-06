package com.workoutdone.rpgym.game.outbox.adapter.out.persistence;

import com.workoutdone.rpgym.game.outbox.domain.OutboxStatus;
import com.workoutdone.rpgym.game.outbox.domain.aggregate.OutboxEvent;
import com.workoutdone.rpgym.game.outbox.domain.repo.OutboxEventRepository;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class OutboxEventRepositoryImpl implements OutboxEventRepository {

    private final OutboxEventJpaRepository outboxEventJpaRepository;

    @Override
    public OutboxEvent save(OutboxEvent event) {
        return outboxEventJpaRepository.save(event);
    }

    /** Pageable은 여기서 만들어 어댑터 안에 갇힌다. 포트는 int limit만 안다. */
    @Override
    public List<OutboxEvent> findPendingForUpdate(int limit) {
        return outboxEventJpaRepository.findByStatusForUpdate(
                OutboxStatus.PENDING, PageRequest.of(0, limit));
    }

    @Override
    public Optional<LocalDateTime> findOldestPendingCreatedAt() {
        return Optional.ofNullable(outboxEventJpaRepository.findOldestPendingCreatedAt());
    }
}
