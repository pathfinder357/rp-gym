package com.workoutdone.rpgym.game.ranking.domain;

//랭킹 저장소 아웃바운드 포트. 구현은 Redis Sorted Set.

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface RankingStore {

    //점수를 올린다(ZADD GT). 지금 점수보다 클 때만 바뀌고, 없으면 추가된다.
    //같은 값으로 여러번 호출해도 결과가 같고(멱등), 작은 값이 늦게 와도 점수가 내려가지 않는다(R-20).
    void save(UUID userId, double score);

    // 여러 건을 한번에 올린다(GT). 재구축 배치에서 쓰임
    void saveAll(Map<UUID, Double> scores);

    // 내 점수. 아직 집계 대상이 아니면 빔.
    Optional<Double> findScore(UUID userId);

    //주어진 점수보다 높은 점수를 가진 수.
    //순위는 "나보다 높은 사람 + 1"로 계산함. 이렇게 해야 동점자가 같은 순위를 받음.
    long countHigherThan(double score);

    // 여러 사용자의 점수를 한 번에(ZMSCORE). 없는 사용자는 결과 맵에 넣지 않는다.
    // 정합성 대조(R-15)가 1,000명씩 비교할 때 쓴다.
    Map<UUID, Double> findScores(List<UUID> userIds);

    //전체 참여자 수.(ZCARD)
    long size();

    //점수 내림차순으로 start ~ end 구간을 가져옴. 둘다 0-based이고 끝을 포함함.
    //범위를 벗어나면 빈 목록임.
    List<ScoredMember> findPage(long start, long end);
}
