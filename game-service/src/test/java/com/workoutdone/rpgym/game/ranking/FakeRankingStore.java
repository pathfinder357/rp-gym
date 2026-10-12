package com.workoutdone.rpgym.game.ranking;


import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import com.workoutdone.rpgym.game.ranking.domain.ScoredMember;

import java.util.*;

//테스트용 인메모리 랭킹 저장소
//Redis ZSET 과 같은 순서(점수 내림차순, 동점이면 member 내림차순)를 흉내낸다.
//순위 계산 로직은 순서에 의존하지 않지만, 페이지 경계 테스트를 위해 결정적이어야 함.
public class FakeRankingStore implements RankingStore {

    private final Map<UUID, Double> scores = new HashMap<>();

    @Override
    public void save(UUID userId, double score){
        // ZADD GT 흉내 -- 지금보다 클 때만 바꾼다
        scores.merge(userId, score, Math::max);
    }

    @Override
    public void saveAll(Map<UUID, Double> newScores){
        newScores.forEach(this::save);
    }
    @Override
    public Optional<Double> findScore(UUID userId){
        return Optional.ofNullable(scores.get(userId));
    }

    @Override
    public Map<UUID, Double> findScores(List<UUID> userIds){
        Map<UUID, Double> result = new HashMap<>();
        userIds.forEach(id -> { if (scores.containsKey(id)) result.put(id, scores.get(id)); });
        return result;
    }

    @Override
    public long countHigherThan(double score){
        return scores.values().stream().filter(s -> s > score).count();
    }

    @Override
    public long size(){
        return scores.size();
    }

    @Override
    public List<ScoredMember> findPage(long start, long end){
        List<ScoredMember> sorted = new ArrayList<>();
        scores.forEach((userId, score) -> sorted.add(new ScoredMember(userId, score)));
        sorted.sort(Comparator
                .comparingDouble(ScoredMember::score).reversed()
                .thenComparing(m -> m.userId().toString(),
                        Comparator.reverseOrder()));

        if (start >= sorted.size() || start < 0){
            return List.of();
        }
        return List.copyOf(sorted.subList((int) start, (int) Math.min(end + 1, sorted.size())));
    }
}
