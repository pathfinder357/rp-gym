package com.workoutdone.rpgym.game.ranking.application;

import com.workoutdone.rpgym.game.character.domain.XpClient;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import com.workoutdone.rpgym.game.ranking.domain.ScoredMember;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RankingQueryService implements RankingQueryUseCase{

    private static final int MIN_SIZE = 1;
    private static final int MAX_SIZE = 100;

    private final RankingStore rankingStore;
    private final XpClient xpClient;

    @Override
    public RankingPageView getRankings(int page, int size){
        validatePaging(page, size);

        long start = (long) page * size;
        long end = start + size - 1;

        List<ScoredMember> members = rankingStore.findPage(start, end);
        long totalElements = rankingStore.size();

        // 비어 있어도 404 가 아니다. 목록 조회에서 "결과 없음" 은 오류가 아니다.
        if (members.isEmpty()){
            return RankingPageView.empty(page, size, totalElements);
        }
        return new RankingPageView(assignRanks(members, start), page, size, totalElements);


    }

    @Override
    public MyRankingView getMyRanking(UUID userId){
        if (userId == null){
            throw new IllegalArgumentException("userId는 필수입니다.");
        }

        long totalCount = rankingStore.size();
        Optional<Double> score = rankingStore.findScore(userId);

        // ZSET 에 없는 사용자 — 404 가 아니라 기본값 200.
        // level/totalXp 는 XP 원본에서 읽는다. 훅이 늦어도 /characters/me 와 같은 값이 나가야 한다.
        if (score.isEmpty()){
            return MyRankingView.notRanked(userId, totalCount, xpClient.findTotalXp(userId));
        }

        long rank = rankingStore.countHigherThan(score.get()) + 1;
        // 지갑 XP 를 한 번 더 읽는다(조회 1회 추가). 랭킹이 지갑을 따라잡았는지 응답에 싣기 위해서다(R-18).
        return MyRankingView.of(userId, rank, totalCount, score.get(), xpClient.findTotalXp(userId));
    }


    //표준 경쟁 순위 1,2,2,4 를 매긴다. 어떤 사람의 순위는 "나보다 점수가 높은 사람 수 + 1" 이다.
    //페이지 첫 사람의 순위만 저장소에 직접 물어보고, 나머지는 순회하며 유도한다.
    //
    //"순위(rank)" 와 "위치(index)" 는 다른 값이다. 동점자가 있으면 둘이 벌어진다.
    // - rank  : countHigherThan 으로 구한다. 동점자는 같은 값을 받는다
    // - index : 전역 0-based 위치. 페이지 첫 사람의 위치는 곧 start 다
    //
    //index 를 countHigherThan 으로 시작하면 안 된다. 그 값은 "그 점수를 가진 첫 번째 사람의
    //위치" 라서, 페이지가 동점 그룹 중간에서 시작하면 실제 위치보다 작아진다.
    //그러면 동점 그룹 바로 뒤 사람의 순위가 앞으로 밀린다.
    private List<RankingEntryView> assignRanks(List<ScoredMember> members, long start){
        double firstScore = members.get(0).score();

        List<RankingEntryView> result = new ArrayList<>(members.size());
        long rank = rankingStore.countHigherThan(firstScore) + 1;   // 첫 사람의 "순위"
        double prevScore = firstScore;
        long index = start;                                          // 첫 사람의 "위치"

        for (ScoredMember member : members){
            if (member.score() != prevScore){
                rank = index + 1;   // 점수가 바뀌면 현재 위치가 곧 순위다
                prevScore = member.score();
            }
            result.add(RankingEntryView.of(rank, member.userId(), member.score()));
            index++;
        }
        return result;
    }

    private void validatePaging(int page, int size){
        if (page < 0){
            throw new IllegalArgumentException("page는 0이상이어야 합니다: " + page);
        }
        if (size < MIN_SIZE || size > MAX_SIZE){
            throw new IllegalArgumentException(
                    "size는 " + MIN_SIZE + " 이상 " + MAX_SIZE + " 이하여야 합니다: " + size
            );
        }
    }
}
