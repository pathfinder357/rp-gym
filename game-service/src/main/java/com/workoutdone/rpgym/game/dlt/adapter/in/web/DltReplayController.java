package com.workoutdone.rpgym.game.dlt.adapter.in.web;

import com.workoutdone.rpgym.game.dlt.application.DltReplayService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * (R-19) DLT 재처리 운영용 내부 API. 게이트웨이가 /api/v1/internal/** 를 막으므로 서비스 포트로 직접 부른다.
 *
 *   POST /api/v1/internal/games/dlt/replay?dryRun=true    무엇이 쌓였는지 보기(발행 · 커밋 없음)
 *   POST /api/v1/internal/games/dlt/replay?dryRun=false   원인을 고친 뒤 되돌리기
 *
 * dryRun 기본값이 true 다. 실수로 부르면 아무 일도 일어나지 않아야 한다.
 */
@RestController
@RequestMapping("/api/v1/internal/games/dlt")
@RequiredArgsConstructor
public class DltReplayController {

    private static final int MAX_RECORDS_CAP = 10_000;

    private final DltReplayService dltReplayService;

    @PostMapping("/replay")
    public DltReplayService.Report replay(@RequestParam(defaultValue = "true") boolean dryRun,
                                          @RequestParam(defaultValue = "500") int maxRecords) {
        return dltReplayService.replay(dryRun, Math.max(1, Math.min(maxRecords, MAX_RECORDS_CAP)));
    }
}
