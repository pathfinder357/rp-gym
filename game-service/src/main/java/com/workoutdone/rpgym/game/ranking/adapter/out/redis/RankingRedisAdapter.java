package com.workoutdone.rpgym.game.ranking.adapter.out.redis;

import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import com.workoutdone.rpgym.game.ranking.domain.ScoredMember;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisZSetCommands.ZAddArgs;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Component
@RequiredArgsConstructor
public class RankingRedisAdapter implements RankingStore {

    private static final String KEY = "ranking:global";

    private final StringRedisTemplate redisTemplate;

    // 실험용 (A-4 랭킹 쓰기 순서 역전, exp/r12-race 브랜치에만 둔다)
    // gt = 지금 코드(ZADD GT) / overwrite = 고치기 전(ZADD, 마지막에 쓴 값이 이긴다). save() 에만 적용
    @Value("${rpgym.experiment.ranking-mode:gt}")
    private String experimentRankingMode;
    // 실험용

    // 리펙토링: ZADD GT 도입 시작부
    // KEY_BYTES를 선언한 이유: 커넥션 수준 api는 자바 객체 직렬화를 자동으로 해주지 않음
    // UUID를 바이트 배열로 직접 변환해서 전달해야함
    private static final byte[] KEY_BYTES = KEY.getBytes(StandardCharsets.UTF_8);

    // 핵심 명제는 다음과 같다. 코드 리펙토링의 결과는 wallet(원본)에서 가져온 복사본을 그대로 읽는게 아니라,
    // [1]복사본이 원본과 다르지 않고, [2]다르면 알수있게 만들고, [3]오류가 나면 다시 만들수 있게 하는것이다.
    // 이걸 줄여서 "보상을 주는 스레드가 겹칠때 랭킹 읽기 모델의 쓰기 순서 역전"라고 칭해볼까 생각중이다.

    // 바꾼 이유: 같은 사용자에게 xp 지급이 두 스레드에서는 겹칠수 있다.
    // (파티 완료 보상 스레드 + 그 멤버의 개인 퀘스트 스레드).
    // 두 스레드가 동시에 xp를 갱신하는건 DB가 행락을 잡은 상태에서 더하기를 하므로 값은 맞다(WalletJpaRepository.addXp)
    // 하지만 여기서 각 스레드는 커밋한 뒤에 지갑을 읽고, 그 다음에 Redis 읽기 모델에 값을 쓰게된다.
    // 이 동작 사이에 틈이 있어서 읽은 순서와, 쓴 순서가 역전되는 걸 체킹하고 리펙토링을 진행했다.
    // 따라서 정리하자면 랭킹(redis)이 어긋나게 됨으로써 [1,2,3]이 전부 성립되지 않는다.

    // [1] 새 점수가 지금 점수보다 클때만 바꾸는 코드
    // GT는 일단 스프링에서 지원이 안돼므로 redis 내장 api를 통해서 데이터를 접근해야함.
    // redisTemplate.execute(...): redis 실체 커넥션에 직접 명령을 날리는 함수
    // KEY_BYTES, bytes(userId): 키와 값을 바이트 배열로 직접 변환해서 전달해야함.
    // ZAddArgs.empty().gt(): 더 클 때만(Greater Than) 업데이트하라는 조건
    // hash map 이기 때문에 k,v
    // 그리고 여기서 또 알아야할게 redis는 단일 스레드이기 때문에 여러 스레드가 동시에 zAdd를 요청해도
    // 단 하나의 zAdd만 수행되기 때문에 락이 자동으로 획득 되는 이점도 있다.
    @Override
    public void save(UUID userId, double score){
        redisTemplate.execute((RedisCallback<Boolean>) connection ->
                connection.zSetCommands().zAdd(KEY_BYTES, score, bytes(userId),
                        "overwrite".equals(experimentRankingMode) ? ZAddArgs.empty() : ZAddArgs.empty().gt()));
    }

    // [1] 파라미터 Map<>형태로 [유저ID:점수] 벌크를 저장하는 코드
    // 랭킹 점수/데이터가 깨졌을 경우 wallet을 바탕으로 redis 데이터를 재 구성하는 함수
    @Override
    public void saveAll(Map<UUID, Double> scores){
        if (scores.isEmpty()){
            return;
        }
        // redis에 일괄 전달할 [k,v] 집합 객체 생성
        Set<Tuple> tuples = new LinkedHashSet<>();
        // Map에 들어 있는 유저 정보집합을 redis 용 튜플 객체로 변환해서 tupe 집합에 담는 코드
        scores.forEach((userId, score) -> tuples.add(new DefaultTuple(bytes(userId), score)));
        // .excuete(...): redis 커넥션에 직접 접근하는 실행 메서드
        // .zSetCommands(): redis Sorted Set 전용 바이트 기반 명령 모음집 가져옴
        // zadd(.., ZaddArgs.empty.gt()): 관리하는 key, 위에서 가져온 여러유저 데이터 모음
        // 에서 tuple로 전달받은 새로 넣으려는 점수가
        // 기존 Redis에 저장된 점수보다 더 클때(greater than) 덮어쓰도록 지정
        // RedisCallback<Long>을 반환 타입으로 가짐으로써 람다식 실행 후 최종적으로 반환하는 결과의 타입을 숫자로
        // 반환
        redisTemplate.execute((RedisCallback<Long>) connection ->
                connection.zSetCommands().zAdd(KEY_BYTES, tuples, ZAddArgs.empty().gt()));
    }

    @Override
    public Optional<Double> findScore(UUID userId){
        return Optional.ofNullable(zset().score(KEY, userId.toString()));
    }

    @Override
    public Map<UUID, Double> findScores(List<UUID> userIds){
        if (userIds.isEmpty()){
            return Map.of();
        }
        List<Double> scores = zset().score(KEY, userIds.stream().map(UUID::toString).toArray());
        Map<UUID, Double> result = new HashMap<>();
        if (scores == null){
            return result;
        }
        for (int i = 0; i < userIds.size() && i < scores.size(); i++){
            if (scores.get(i) != null){
                result.put(userIds.get(i), scores.get(i));
            }
        }
        return result;
    }

    @Override
    public long countHigherThan(double score){
        //ZREVRANK 를 쓰지 않는다.
        //정렬된 인덱스를 주기 때문에 동점자가 15등 16등으로 갈려서,
        //동점자에게 같은 순위를 주는 전체 랭킹 결과와 어긋난다.
        //score 가 항상 정수라 하한을 score + 1 로 주면 "나보다 높은 사람"을 정확히 셀 수 있다.

        return nvl(zset().count(KEY, score + 1, Double.POSITIVE_INFINITY));
    }

    @Override
    public long size(){
        return nvl(zset().zCard(KEY));
    }

    @Override
    public List<ScoredMember> findPage(long start, long end){
        Set<TypedTuple<String>> tuples = zset().reverseRangeWithScores(KEY, start, end);
        if (tuples == null || tuples.isEmpty()){
            return List.of();
        }
        List<ScoredMember> result = new ArrayList<>(tuples.size());
        for (TypedTuple<String> tuple : tuples){
            if (tuple.getValue() == null || tuple.getScore() == null){
                continue;
            }
            result.add(new ScoredMember(UUID.fromString(tuple.getValue()), tuple.getScore()));
        }
        return result;
    }


    private ZSetOperations<String, String> zset(){
        return redisTemplate.opsForZSet();
    }

    private static byte[] bytes(UUID userId){
        return userId.toString().getBytes(StandardCharsets.UTF_8);
    }

    private long nvl(Long value){
        return value == null ? 0L : value;
    }
}
