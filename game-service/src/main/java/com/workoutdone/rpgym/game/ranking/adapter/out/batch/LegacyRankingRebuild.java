package com.workoutdone.rpgym.game.ranking.adapter.out.batch;

import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository;
import com.workoutdone.rpgym.game.character.domain.Character;
import com.workoutdone.rpgym.game.character.domain.XpClient;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 실험 전용 (A-10, exp/r12-race 브랜치에만 둔다). R-20 이전 재구축을 그대로 재현한다.
 *   - characters 를 OFFSET 페이지(findAll(PageRequest))로 읽고, 한 트랜잭션(readOnly) 안에서 끝까지 돈다
 *   - 사용자마다 XpClient(지갑 조회)를 한 번씩 부른다 -> 사용자 N 명이면 조회 N 번
 *   - 점수의 레벨은 characters.level 을 그대로 쓴다
 * 쓰기(saveAll)는 지금 것과 같은 것을 써서, 비교가 "읽는 방식" 하나만 다르게 한다.
 */
@Component
@RequiredArgsConstructor
public class LegacyRankingRebuild {

    private final CharacterRepository characterRepository;
    private final XpClient xpClient;
    private final RankingStore rankingStore;

    @Transactional(readOnly = true)
    public long rebuild(int batchSize) {
        long total = 0;
        int pageNumber = 0;
        Page<Character> page;
        do {
            page = characterRepository.findAll(PageRequest.of(pageNumber, batchSize));
            Map<UUID, Double> batch = new LinkedHashMap<>();
            for (Character character : page.getContent()) {
                UUID userId = character.getUserId();
                int totalXp = xpClient.findTotalXp(userId);
                batch.put(userId, RankingScore.encode(character.getLevel(), totalXp));
            }
            rankingStore.saveAll(batch);
            total += batch.size();
            pageNumber++;
        } while (page.hasNext());
        return total;
    }
}
