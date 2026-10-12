package com.workoutdone.rpgym.game.quest.adapter.in.web;

import com.workoutdone.rpgym.common.exception.GlobalExceptionHandler;
import com.workoutdone.rpgym.game.exception.GameExceptionHandler;
import com.workoutdone.rpgym.game.quest.application.QuestSuggestionAcceptService;
import com.workoutdone.rpgym.game.quest.application.QuestView;
import com.workoutdone.rpgym.game.quest.application.SuggestionDecision;
import com.workoutdone.rpgym.game.quest.domain.Metric;
import com.workoutdone.rpgym.game.quest.domain.QuestStatus;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

// 판정 결과가 어떤 상태 코드로 나가는지만 본다.
// 판정 자체가 맞는지는 서비스 테스트가 본다.
@WebMvcTest(QuestSuggestionController.class)
@Import({GameExceptionHandler.class, GlobalExceptionHandler.class})
class QuestSuggestionControllerTest {

    private static final String BASE_URL = "/api/v2/internal/games/quest-suggestions";
    private static final UUID SUGGESTION_ID = UUID.randomUUID();
    private static final UUID USER_ID = UUID.randomUUID();

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private QuestSuggestionAcceptService questSuggestionAcceptService;

    private static QuestView questView() {

        return new QuestView(
                UUID.randomUUID(), "20분 산책하기", Metric.ACTIVE_MINUTES,
                20, 31, 0, QuestStatus.ACTIVE, 5, Instant.parse("2026-08-28T14:59:59Z"),
                Instant.parse("2026-08-28T03:00:00Z"));
    }

    private String acceptUrl() {
        return BASE_URL + "/" + SUGGESTION_ID + "/accept";
    }

    private String rejectUrl() {
        return BASE_URL + "/" + SUGGESTION_ID + "/reject";
    }

    private void givenAcceptReturns(SuggestionDecision decision) {
        given(questSuggestionAcceptService.accept(any(), any())).willReturn(decision);
    }

    private void givenAcceptFails(SuggestionDecision.Reason reason) {
        givenAcceptReturns(new SuggestionDecision.Failed(reason));
    }

    @Test
    @DisplayName("수락하면 201과 만들어진 퀘스트를 돌려준다")
    void 수락하면_201이다() throws Exception {
        givenAcceptReturns(new SuggestionDecision.Accepted(questView()));

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.title").value("20분 산책하기"))
                .andExpect(jsonPath("$.metric").value("ACTIVE_MINUTES"))
                .andExpect(jsonPath("$.targetValue").value(20))
                .andExpect(jsonPath("$.baselineValue").value(31))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.rewardXp").value(5));
    }

    @Test
    @DisplayName("거절하면 204다 — 만들어지는 것이 없어 돌려줄 본문이 없다")
    void 거절하면_204다() throws Exception {
        given(questSuggestionAcceptService.reject(any(), any()))
                .willReturn(new SuggestionDecision.Rejected(SUGGESTION_ID));

        mockMvc.perform(post(rejectUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isNoContent());
    }

    @Test
    @DisplayName("없는 제안이거나 남의 제안이면 404다")
    void 못_찾으면_404다() throws Exception {
        givenAcceptFails(SuggestionDecision.Reason.NOT_FOUND);

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("SUGGESTION_NOT_FOUND"));
    }

    @Test
    @DisplayName("이미 처리된 제안이면 409다")
    void 이미_처리됐으면_409다() throws Exception {
        givenAcceptFails(SuggestionDecision.Reason.ALREADY_DECIDED);

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("SUGGESTION_ALREADY_DECIDED"));
    }

    @Test
    @DisplayName("30분이 지난 제안이면 410이다 — 있었지만 이제 없다는 뜻이라 404와 구분한다")
    void 만료됐으면_410이다() throws Exception {
        givenAcceptFails(SuggestionDecision.Reason.ALREADY_EXPIRED);

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isGone())
                .andExpect(jsonPath("$.code").value("SUGGESTION_EXPIRED"));
    }

    @Test
    @DisplayName("근거 스냅샷이 낡았으면 409다")
    void 근거가_낡으면_409다() throws Exception {
        givenAcceptFails(SuggestionDecision.Reason.SUPERSEDED);

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("SUGGESTION_SUPERSEDED"));
    }

    @Test
    @DisplayName("이미 진행 중인 퀘스트가 있으면 409다")
    void 활성_퀘스트가_있으면_409다() throws Exception {
        givenAcceptFails(SuggestionDecision.Reason.QUEST_ALREADY_ACTIVE);

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("QUEST_ALREADY_ACTIVE"));
    }

    @Test
    @DisplayName("건강 데이터 스냅샷이 없으면 409다 — 다시 시도해도 같은 결과라 5xx로 내리지 않는다")
    void 스냅샷이_없으면_409다() throws Exception {
        givenAcceptFails(SuggestionDecision.Reason.SNAPSHOT_MISSING);

        mockMvc.perform(post(acceptUrl()).header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("SNAPSHOT_MISSING"));
    }

    @Test
    @DisplayName("X-User-Id 헤더가 없으면 401이다 — 게이트웨이를 우회했다는 뜻이다")
    void 헤더가_없으면_401이다() throws Exception {
        mockMvc.perform(post(acceptUrl()))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.code").value("UNAUTHORIZED"));
    }

    @Test
    @DisplayName("제안 식별자가 UUID 모양이 아니면 400이다")
    void 식별자가_이상하면_400이다() throws Exception {
        mockMvc.perform(post(BASE_URL + "/not-a-uuid/accept").header("X-User-Id", USER_ID.toString()))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_INPUT"));
    }
}
