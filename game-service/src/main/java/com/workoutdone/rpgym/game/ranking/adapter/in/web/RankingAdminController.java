package com.workoutdone.rpgym.game.ranking.adapter.in.web;

import com.workoutdone.rpgym.game.ranking.adapter.out.batch.RankingRebuildJob;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * (R-20) 랭킹 재구축 운영용 내부 API.
 *
 * /api/v1/internal/** 는 게이트웨이가 denyAll 로 막는다(SecurityConfig). 외부 사용자는 부를 수 없고,
 * 운영자가 서비스 포트로 직접 호출한다.
 */
@RestController
@RequestMapping("/api/v1/internal/games/rankings")
@RequiredArgsConstructor
public class RankingAdminController {

    private final RankingRebuildJob rankingRebuildJob;

    @PostMapping("/rebuild")
    public Map<String, Long> rebuild() {
        return Map.of("rebuilt", rankingRebuildJob.rebuild());
    }
}
