package com.workoutdone.rpgym.game.character.adapter.out.persistence;

import com.workoutdone.rpgym.game.character.domain.Character;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;


//캐릭터 저장소.
//조회 경로에서는 findByUserId만 씀.
public interface CharacterRepository extends JpaRepository<Character, UUID> {

    Optional<Character> findByUserId(UUID userId);

    // [1] DB가 conflict do update로 행에 대한 락을 잡지만
    // 락은 실질적으로 db row에 데이터를 쓸때만 작동함
    // 그래서 GREATEST sql 함수 적용해서 경합이 일어나는 경우 EXCLUDED.level(새로들어오는 데이터)
    // game_service.characters.level(레거시 데이터) 2개를 A,B로 판단해서 둘 중 큰거를 update
    // 정리하자면 비교 로직 자체를 DB 락 내부(db가 데이터를 바꾸는 순간)로 옮겼다는 뜻이다
    // INSERT ... ON CONFLICT DO UPDATE 구문 자체가 하나의 원자적 쓰기 작업임(비교와 쓰기 사이에 어떤 틈도 허용 x)
    //  - insert 수행할때 캐릭터 테이블의 유니크 인덱스 먼저 조회
    //  - 입력하려는 유니크 인덱스가 있으면 conflict 구문 수행, 없으면 그냥 insert. 아래에 do update로 전환함
    //  - ON CONFLICT: 이미 존재하는 row에 대해서 x-lock을 획득, 락을 쥔 상태에서 do update 절 실행(락 내부)
    //  - GREATEST: 이게 락을 획득 하는 순간에 GREATEST를 비교하는 거기 때문에 jvm 계산이 이미 완료되도
    //    더 큰값을 자체적으로 db레벨에서 수행해서 결과값을 db에 반영하고 락을 해제함.
    //    그리고 pgSQL의 특징 중 하나인 read committed 격리 수준 덕분에
    //    characters.level이 항상 최신값으로 유지되면서 레벨이 깎이는걸 진짜 원천적으로 차단함
    // 주의: 아래 SQL 문자열(텍스트 블록) 안에는 // 주석을 쓰면 안 된다. 그대로 DB로 가서 문법 오류가 난다.
    //       SQL 안에 주석이 꼭 필요하면 -- 를 쓴다
    @Modifying
    @Query(value = """
            INSERT INTO game_service.characters (id, user_id, level, created_at, updated_at)
            VALUES (:id, :userId, :level, now(), now())
            ON CONFLICT (user_id)
            DO UPDATE SET level = GREATEST(game_service.characters.level, EXCLUDED.level),
                          updated_at = now()
            """, nativeQuery = true)
    void upsertLevel(@Param("id") UUID id,
                     @Param("userId") UUID userId,
                     @Param("level") int level);

    // (R-20) 랭킹 재구축의 원천. 사용자별 XP 를 SQL 한 번으로 읽는다(예전: 사용자마다 XpClient 호출).
    // characters 와 wallets 양쪽의 사용자를 합친다.
    //   - characters 만 있음: 가입했지만 아직 XP 가 없다 -> 0
    //   - wallets 만 있음: XP 는 받았는데 레벨 반영(upsertLevel)이 실패했다 -> 지갑 값
    // user_id 기준 키셋 페이지네이션. OFFSET 은 뒤로 갈수록 느려진다.
    //
    // (R-22, 2026-10-11) 예전 쿼리는 (characters UNION wallets) 를 먼저 만든 뒤 after 다음을 골랐다.
    // UNION 은 중복을 지우려고 남은 행 전체를 합치고 정렬하므로, 키셋인데도 한 페이지 비용이 "남은 사용자 수" 에 비례했다
    // (A-10: 10만 명에서 배치와 상관없이 페이지당 약 55~70ms, 배치 100 이면 57.6초).
    // 지금: 두 테이블에서 각각 인덱스(uk_characters_user_id, pk_wallets)로 after 다음 :limit 명만 읽고 합친다.
    // 합친 집합의 앞 :limit 명은 정확하다 -- 전체 순서에서 앞 :limit 명 안에 드는 사용자는
    // 자기가 속한 테이블에서도 반드시 앞 :limit 명 안에 들기 때문이다(그보다 앞선 사용자가 :limit 명 미만이므로).
    // 지갑이 있는 사용자는 지갑 행도 같은 이유로 함께 들어오므로 MAX(xp) 가 지갑 값이다.
    @Query(value = """
            SELECT t.user_id AS userId, COALESCE(MAX(t.xp), 0) AS totalXp
              FROM ((SELECT user_id, NULL::integer AS xp
                       FROM game_service.characters
                      WHERE user_id > :after
                      ORDER BY user_id
                      LIMIT :limit)
                    UNION ALL
                    (SELECT user_id, xp
                       FROM game_service.wallets
                      WHERE user_id > :after
                      ORDER BY user_id
                      LIMIT :limit)) t
             GROUP BY t.user_id
             ORDER BY t.user_id
             LIMIT :limit
            """, nativeQuery = true)
    List<RankingSourceRow> findRankingSource(@Param("after") UUID after, @Param("limit") int limit);

    // 실험 코드 (A-10, exp/r12-race 브랜치에만 둔다): R-22 이전 쿼리. 같은 실행 안에서 전후를 비교하려고 남긴다
    @Query(value = """
            SELECT u.user_id AS userId, COALESCE(w.xp, 0) AS totalXp
              FROM (SELECT user_id FROM game_service.characters
                    UNION
                    SELECT user_id FROM game_service.wallets) u
              LEFT JOIN game_service.wallets w ON w.user_id = u.user_id
             WHERE u.user_id > :after
             ORDER BY u.user_id
             LIMIT :limit
            """, nativeQuery = true)
    List<RankingSourceRow> findRankingSourceUnion(@Param("after") UUID after, @Param("limit") int limit);
    // 실험 코드

    interface RankingSourceRow {
        UUID getUserId();
        Integer getTotalXp();
    }
}
