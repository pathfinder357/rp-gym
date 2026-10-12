<#
실험 실행기 (2026-10-11) — 실험 한 회차를 명령 한 줄로.

  사용:  cd <저장소 루트>\experiments
         powershell -ExecutionPolicy Bypass -File .\run_exp.ps1 -Exp A-5

  결과:  <저장소 루트>\game-service\exp-results\<실험>_<시각>.txt  (요약 · 검증 · 분석)
         같은 이름의 .log (앱 로그 원본). Claude 가 이 폴더를 직접 읽는다.

  실험:  A-1  R-12 실경합 (스레드 3)          A-1c 대조군 (스레드 1)
         A-2  반대 실험 naive-claim            A-5  늦게 온 이벤트 (R-9 · R-10 · R-13)
         A-7  정합성 대조가 어긋남을 잡는가     A-9  Redis 없이 → UnexpectedRollbackException 0
         A-11 Redis 정지 중 지급 → 카운터 → 재구축 → 대조 0
         A-3  지갑 lost update, 고치기 전(읽고 더하고 쓰기)   A-3c 같은 조건, 지금 코드(xp = xp + :amount)
         A-6  DLT 재처리: 버그로 DLT 간 이벤트 → 수정 배포 → 재처리로 반영 / 이미 처리 · 어제 · 만료 제안은 재처리해도 보상 0 / 깨진 JSON 은 상한 3번에서 멈춤
         A-8  낙관적 락 충돌: 퀘스트마다 5번 충돌 → 일시 장애로 계속 재시도 → 6번째 성공, DLT 0
         B-0  관측 구성 준비 확인: Prometheus job=game-blue 수집 · Grafana 알림 규칙 로드 · Loki 에 앱 로그 · Slack 연락처
         B-12 알림 3종 Slack 도착: DLT 적재 / 컨슈머 지연(10분 지속) / Outbox 최장 대기 — 주입부터 firing 까지 시간
         B-13 새 알림: 대조 불일치 / 랭킹 반영 실패 / 처리 지연 p95 (+ 선택: 대조 멈춤 -Phases 2, 약 31분)
         B-16 장애 주입: PostgreSQL 60초 · Kafka 90초 정지 중 흐르는 이벤트 -> DLT 0 · 유실 0 · 중복 0 · 복구 뒤 대조 0
         B-14 원인 추적: 깨진 JSON · 코드 버그 -> DLT -> Slack -> Loki 에서 eventId 로 원인을 찾을 수 있나 (DLT 헤더와 비교)
         B-17 trace 이어짐: Health 가 보낸 traceparent -> Game 소비 -> Outbox 적재 -> (다른 스레드) 발행 -> game.events 헤더까지 한 trace 인가
         B-19 Outbox 발행측: 50건 쌓고 10건째 발행 직후 JVM 즉시 종료(kill -9) -> 재기동 -> 유실 0 · 중복 건수 · 미발행 0
         B-15 lag 알림 오탐 · 정지 감지: 배경 트래픽(분당 36건) 중 1 평상시 30분 오탐? 2 전 파티션 멈춤 15분 몇 분에 울리나? 3 앱 자체가 15분 죽으면? (약 75분, -Phases 로 고름)
         C-20 Kafka 3대(R-14): C-19 기동 · 토픽 6개 / ① kafka-2 정지 중 100건 -> 발행 실패 0 / ② 2대 정지 -> NotEnoughReplicas · Outbox 알림 -> 복구 후 유실 0 (끝나면 1대 구성으로 되돌림)
         A-10 랭킹 재구축 성능: 10만 명, 고치기 전(사용자마다 조회) vs 지금(키셋 SQL) × 배치 100 · 1,000 · 10,000, 재구축 중 ZSCORE 지연
         A-4  랭킹 쓰기 순서 역전, 고치기 전(ZADD 덮어쓰기)    A-4c 같은 조건, 지금 코드(ZADD GT)
  주의:  A-1 · A-1c · A-2 · A-3 · A-3c · A-4 · A-4c · A-7 · A-9 · A-11 의 지연 · 모드 인자는 exp/r12-race 브랜치 코드에만 있다(다른 브랜치에선 무시된다).
#>
param(
    [Parameter(Mandatory = $true)][string]$Exp,
    [int]$DelayMs = 200,
    # 저장소 안(experiments/)에서 돌리므로 저장소 루트 = 이 파일의 상위 폴더
    [string]$Root = (Split-Path -Parent $PSScriptRoot),
    [int]$AppPort = 19012,
    [int]$MaxWaitSec = 600,
    # B-12 에서 돌릴 단계만 고른다. 예: -Phases 2  (기본 123 = 전부)
    [string]$Phases = '123'
)

$ErrorActionPreference = 'Continue'
# docker exec psql 은 UTF-8 로 출력한다. PowerShell 5.1 기본(CP949)으로 읽으면 한글이 깨진다
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$env:PYTHONIOENCODING = 'utf-8'
$Here = $PSScriptRoot
$R12 = 'R12_실경합'
$A5 = 'A5_늦은이벤트'
$Fast = '--rpgym.reconcile.initial-delay=5000 --rpgym.reconcile.interval=15000'
$Delay = "--rpgym.experiment.party-tx-delay-ms=$DelayMs"

# ---------------------------------------------------------------- 실험 정의
$R12Base = @{ Kit = $R12; Cleanup = '99_cleanup.sql'; Gen = 'gen_aligned.py'; Seed = '01_seed_aligned.sql'
              Template = '02_events_aligned.template.txt'; Events = '02_events_aligned.txt'; Verify = '03_verify_aligned.sql'; Fill = 'R12' }
function Def($base, $extra) { $h = @{}; foreach ($k in $base.Keys) { $h[$k] = $base[$k] }; foreach ($k in $extra.Keys) { $h[$k] = $extra[$k] }; return $h }

$Experiments = @{
    'A-1'  = Def $R12Base @{ Desc = 'R-12 실경합: 4명 · 목표 3000 · 스레드 3'; Threads = 3; Args = $Delay; Analyze = @('overlap_report.py') }
    'A-1c' = Def $R12Base @{ Desc = 'R-12 대조군: 스레드 1'; Threads = 1; Args = "$Delay --spring.kafka.listener.concurrency=1"; Analyze = @('overlap_report.py') }
    'A-2'  = Def $R12Base @{ Desc = 'R-12 반대 실험: 완료 판단을 자바에서 (naive-claim)'; Threads = 3
                             Args = "$Delay --rpgym.experiment.party-mode=naive-claim"; Analyze = @('a2_report.py', 'overlap_report.py') }
    'A-5'  = @{ Desc = '늦게 온 이벤트: 어제 이벤트(개인 · 파티) + 30분 넘은 제안'; Kit = $A5; Threads = 3; Args = ''
                Cleanup = "$A5\99_cleanup_a5.sql"; Gen = 'gen_a5.py'; Seed = '01_seed_a5.sql'; Template = '02_events_a5.template.txt'
                Events = '02_events_a5.txt'; Verify = '03_verify_a5.sql'; Fill = 'A5'; Analyze = @() }
    'A-7'  = Def $R12Base @{ Desc = '정합성 대조: 원장 1행 삭제 · 지갑 1명 변경을 게이지가 잡는가'; Threads = 3; Args = "$Fast"; Analyze = @(); Post = 'A-7' }
    'A-9'  = Def $R12Base @{ Desc = 'Redis 없이 400건: 롤백 예외 0 · 레벨은 커밋 · 실패 카운터 400'; Threads = 3; Args = ''; Analyze = @(); RedisDown = $true; Post = 'A-9' }
    'A-3'  = Def $R12Base @{ Desc = '지갑 lost update, 고치기 전: 개인(+10) · 파티(+30) 지급이 같은 유저 지갑을 읽고 더하고 쓰기'; Threads = 3
                             GenArgs = @('--personal'); Analyze = @(); Post = 'A-3'
                             Args = "$Delay --rpgym.experiment.wallet-mode=read-modify-write --rpgym.experiment.wallet-delay-ms=$DelayMs" }
    'A-3c' = Def $R12Base @{ Desc = '지갑 lost update, 지금 코드: 같은 조건에서 xp = xp + :amount'; Threads = 3
                             GenArgs = @('--personal'); Analyze = @(); Post = 'A-3'
                             Args = "$Delay --rpgym.experiment.wallet-mode=atomic --rpgym.experiment.wallet-delay-ms=$DelayMs" }
    'A-4'  = Def $R12Base @{ Desc = '랭킹 쓰기 순서 역전, 고치기 전: 지갑을 읽고 ZADD 로 덮어쓰기(늦게 쓴 옛 값이 이긴다)'; Threads = 3
                             GenArgs = @('--personal'); Analyze = @(); Post = 'A-4'
                             Args = "$Delay $Fast --rpgym.experiment.wallet-delay-ms=$DelayMs --rpgym.experiment.ranking-stall-below-xp=40 --rpgym.experiment.ranking-stall-ms=2000 --rpgym.experiment.ranking-mode=overwrite" }
    'A-4c' = Def $R12Base @{ Desc = '랭킹 쓰기 순서 역전, 지금 코드: 같은 조건에서 ZADD GT'; Threads = 3
                             GenArgs = @('--personal'); Analyze = @(); Post = 'A-4'
                             Args = "$Delay $Fast --rpgym.experiment.wallet-delay-ms=$DelayMs --rpgym.experiment.ranking-stall-below-xp=40 --rpgym.experiment.ranking-stall-ms=2000 --rpgym.experiment.ranking-mode=gt" }
    'A-6'  = @{ Desc = 'DLT 재처리: 버그 수정 뒤 반영 · 이미 처리 · 어제 · 만료 제안은 보상 0 · 깨진 JSON 은 상한에서 멈춤'; Kit = 'A6_DLT재처리'; Threads = 3; Args = ''; Post = 'A-6' }
    'A-8'  = @{ Desc = '낙관적 락 충돌: 퀘스트마다 5번 충돌 -> 일시 장애로 재시도 -> 성공, DLT 0'; Kit = 'A8_낙관적락'; Threads = 3
                Args = '--rpgym.experiment.conflict-user-prefix=a8000001- --rpgym.experiment.conflict-times=5'
                Cleanup = "A8_낙관적락\99_cleanup_a8.sql"; Gen = 'gen_a8.py'; Seed = '01_seed_a8.sql'; Template = '02_events_a8.template.txt'
                Events = '02_events_a8.txt'; Verify = '03_verify_a8.sql'; Fill = 'A5'; Analyze = @(); Post = 'A-8' }
    'B-0'  = @{ Desc = '관측 구성 준비 확인 (docker-compose-observe.yml)'; Kit = '.'; Threads = 3; Args = ''; Post = 'B-0' }
    'B-12' = @{ Desc = '알림 3종: DLT 적재 · 컨슈머 지연 · Outbox 최장 대기 -> Grafana firing -> Slack'; Kit = 'B12_알림3종'; Threads = 3; Args = ''; Post = 'B-12' }
    'B-13' = @{ Desc = '새 알림: 대조 불일치 · 랭킹 반영 실패 · 처리 지연 p95 (+ 대조 멈춤) -> Grafana firing -> Slack'; Kit = 'B13_알림5종'; Threads = 3; Args = ''; Post = 'B-13' }
    'B-16' = @{ Desc = '장애 주입: PostgreSQL 60초 · Kafka 90초 정지 -> DLT 0 · 유실 0 · 중복 0 · 복구 뒤 대조 0'; Kit = 'B16_장애주입'; Threads = 3; Args = ''; Post = 'B-16' }
    'B-17' = @{ Desc = 'trace 이어짐: traceparent 붙인 이벤트 -> Game 소비 -> Outbox 발행 -> game.events 헤더까지 한 trace (Zipkin)'; Kit = 'B17_trace'; Threads = 3; Args = ''; Post = 'B-17' }
    'B-19' = @{ Desc = 'Outbox 발행측 최소 한 번: 발행 직후 · PUBLISHED 커밋 전 강제 종료 -> 유실 0 · 중복 건수 · 미발행 0'; Kit = 'B19_발행측'; Threads = 3; Args = ''; Post = 'B-19' }
    'B-15' = @{ Desc = 'lag 알림 오탐 · 정지 감지: 배경 트래픽 중 평상시 30분 / 전 파티션 멈춤 15분 / 앱 죽음 15분'; Kit = 'B15_lag오탐'; Threads = 3; Args = ''; Post = 'B-15' }
    'C-20' = @{ Desc = 'Kafka 3대(R-14): 기동 · 토픽 확인 / 1대 정지 중 발행 실패 0 / 2대 정지 -> NotEnoughReplicas -> Outbox 알림 -> 복구 후 유실 0'; Kit = 'C20_kafka3대'; Threads = 3; Args = ''; Post = 'C-20' }
    'B-14' = @{ Desc = '원인 추적: DLT 알림 뒤 Loki 에서 eventId 로 원인 찾기 (DLT 헤더와 비교)'; Kit = 'B14_원인추적'; Threads = 3; Args = ''; Post = 'B-14' }
    'A-10' = @{ Desc = '랭킹 재구축 성능: 10만 명, 방식 · 배치 크기별 재구축 시간과 재구축 중 ZSCORE 지연'; Kit = 'A10_재구축'; Threads = 3; Args = ''; Post = 'A-10' }
    'A-11' = Def $R12Base @{ Desc = 'Redis 정지 중 지급 → 대조가 랭킹 어긋남을 잡고 → 재구축 1회 → 0'; Threads = 3; Args = "$Fast"; Analyze = @(); RedisDown = $true; Post = 'A-11' }
}
if (-not $Experiments.ContainsKey($Exp)) {
    Write-Host "모르는 실험: $Exp  (가능: $(($Experiments.Keys | Sort-Object) -join ', '))" -ForegroundColor Red
    exit 1
}
$E = $Experiments[$Exp]
$Kit = Join-Path $Here $E.Kit

# ---------------------------------------------------------------- 결과 파일 · 공통 함수
$OutDir = Join-Path $Root 'game-service\exp-results'
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$Stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$Result = Join-Path $OutDir "${Exp}_$Stamp.txt"
$AppLog = Join-Path $OutDir "${Exp}_$Stamp.log"
$AppErr = Join-Path $OutDir "${Exp}_$Stamp.err.log"

function W([string]$t) { Add-Content -Path $Result -Value $t -Encoding UTF8; Write-Host $t }
function Run([string]$title, [scriptblock]$cmd) {
    W ""; W "### $title"
    $out = & $cmd 2>&1 | ForEach-Object { "$_" }
    foreach ($l in $out) { W $l }
    return $out
}
function Stop-App {
    $c = Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue
    foreach ($x in $c) { Stop-Process -Id $x.OwningProcess -Force -ErrorAction SilentlyContinue }
    if ($script:Gradle -and -not $script:Gradle.HasExited) {
        Start-Sleep -Seconds 5
        if (-not $script:Gradle.HasExited) { & taskkill /T /F /PID $script:Gradle.Id 2>&1 | Out-Null }
    }
}
function Fail([string]$why) {
    W ""; W "!!! 실패: $why"
    if (Test-Path $AppLog) { W "--- 앱 로그 마지막 60줄"; Get-Content $AppLog -Tail 60 | ForEach-Object { W $_ } }
    Stop-App
    if ($E.RedisDown) { & docker start rp-gym-redis 2>&1 | Out-Null }
    W ""; W "결과 파일: $Result"
    exit 1
}
function Psql-File([string]$path) {
    $name = Split-Path $path -Leaf
    & docker cp $path "rp-gym-postgres-game:/tmp/$name" 2>&1 | Out-Null
    & docker exec rp-gym-postgres-game psql -U postgres -d game_db -f "/tmp/$name" 2>&1 | ForEach-Object { "$_" }
}
function Sql([string]$q) { & docker exec rp-gym-postgres-game psql -U postgres -d game_db -c $q 2>&1 | ForEach-Object { "$_" } }
function Offsets {
    & docker exec rp-gym-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic health.events 2>&1 |
        ForEach-Object { "$_" } | Where-Object { $_ -match '^health\.events:\d+:\d+' }
}
function Metrics([string]$pattern) {
    try {
        $c = (Invoke-WebRequest -UseBasicParsing "http://localhost:$AppPort/actuator/prometheus" -TimeoutSec 10).Content
        ($c -split "`n") | Where-Object { $_ -match $pattern -and $_ -notmatch '^#' }
    } catch { "metrics 읽기 실패: $($_.Exception.Message)" }
}
function Start-Redis {
    & docker start rp-gym-redis 2>&1 | ForEach-Object { "$_" }
    for ($i = 0; $i -lt 20; $i++) {
        Start-Sleep -Seconds 3
        $pong = (& docker exec rp-gym-redis redis-cli ping 2>&1 | ForEach-Object { "$_" }) -join ' '
        if ($pong -match 'PONG|NOAUTH') { "redis-cli ping: $pong ($(($i + 1) * 3)초)"; break }
    }
    "컨테이너 상태: $((& docker ps -a --filter name=rp-gym-redis --format '{{.Status}} {{.Ports}}' 2>&1) -join ' ')"
    "Redis 로그 마지막 줄: $((& docker logs --tail 3 rp-gym-redis 2>&1 | ForEach-Object { "$_" }) -join ' | ')"
}
function LogCount([string]$pattern) { @(Select-String -Path $AppLog -Pattern $pattern -ErrorAction SilentlyContinue).Count }

W "# 실험 $Exp — $($E.Desc)"
W "시각: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  /  앱 인자: $($E.Args)"
W "브랜치: $(& git -C $Root rev-parse --abbrev-ref HEAD 2>&1)  /  커밋: $(& git -C $Root rev-parse --short HEAD 2>&1)"

# ---------------------------------------------------------------- A-6 은 앱을 두 번 띄우는 별도 흐름
function Start-AppWith([string]$appArgs) {
    W ""; W "### 앱 기동 (앱 인자: $appArgs)"
    $log = $AppLog; $err = $AppErr
    if ($script:AppRun) { $log = $AppLog -replace '\.log$', "-run$($script:AppRun).log"; $err = $AppErr -replace '\.err\.log$', "-run$($script:AppRun).err.log" }
    $script:CurLog = $log
    $script:Gradle = Start-Process -FilePath (Join-Path $Root 'gradlew.bat') -WorkingDirectory $Root `
        -ArgumentList ":game-service:bootRun --args=`"$appArgs`"" -RedirectStandardOutput $log -RedirectStandardError $err -NoNewWindow -PassThru
    $t0 = Get-Date
    while ($true) {
        Start-Sleep -Seconds 3
        if ($script:Gradle.HasExited) { Fail '앱이 기동 중에 꺼졌다 (아래 로그 참고)' }
        if (@(Select-String -Path $log -Pattern 'partitions assigned' -ErrorAction SilentlyContinue).Count -ge 3) {
            W "기동 확인 ($([int]((Get-Date) - $t0).TotalSeconds)초, 로그: $log)"; break
        }
        if (((Get-Date) - $t0).TotalSeconds -gt 240) { Fail '4분 안에 앱이 뜨지 않았다' }
    }
    Start-Sleep -Seconds 5
    $script:AppRun = 1 + [int]$script:AppRun
}
function DltEnd {
    $sum = 0
    & docker exec rp-gym-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic health.events.DLT 2>&1 |
        ForEach-Object { "$_" } | Where-Object { $_ -match '^health\.events\.DLT:\d+:(\d+)' } | ForEach-Object { $sum += [long]$Matches[1] }
    return $sum
}
function Produce([string]$file, [string]$topic) {
    & docker cp $file 'rp-gym-kafka:/tmp/a6.txt' 2>&1 | Out-Null
    & docker exec rp-gym-kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $topic --property parse.key=true --property key.separator='|' < /tmp/a6.txt" 2>&1 | ForEach-Object { "$_" }
}
function Replay([bool]$dry) {
    try {
        $j = (Invoke-WebRequest -UseBasicParsing -Method Post -TimeoutSec 120 `
            "http://localhost:$AppPort/api/v1/internal/games/dlt/replay?dryRun=$($dry.ToString().ToLower())&maxRecords=500").Content | ConvertFrom-Json
    } catch { return "재처리 호출 실패: $($_.Exception.Message)" }
    "dryRun=$($j.dryRun) replayed=$($j.replayed) skipped=$($j.skipped) seen=$(@($j.items).Count)"
    @($j.items) | Group-Object action, { if ($_.exception) { ($_.exception -split '\.')[-1] } else { '-' } }, replayCount |
        Sort-Object Name | ForEach-Object { "  $($_.Count)건  [action, 원래 예외, replayCount] = $($_.Name)" }
}
# ---------------------------------------------------------------- B: 관측 구성 공통
$ObserveCompose = @('-f', 'docker-compose-develop.yml', '-f', 'docker-compose-observe.yml')
$GrafanaAuth = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('admin:admin')) }
function Start-Observe {
    # C-20(Kafka 3대)이 중간에 멈췄다면 남은 브로커를 치운다. kafka 는 아래 up 이 1대 구성으로 다시 만든다
    & docker rm -f rp-gym-kafka-2 rp-gym-kafka-3 rp-gym-kafka-init 2>&1 | Out-Null
    Push-Location $Root
    & docker compose @ObserveCompose up -d postgres-game kafka redis prometheus grafana loki alloy zipkin 2>&1 | ForEach-Object { "$_" }
    # Grafana 는 알림 규칙 파일을 시작할 때만 읽는다. 규칙 파일만 바뀌면 compose 가 컨테이너를 다시 만들지 않으므로 매번 재시작한다
    # (2026-10-12 R-23 첫 검증: 규칙 식을 바꿨는데 예전 식으로 평가되어 20분 동안 안 울림)
    & docker restart rp-gym-grafana 2>&1 | ForEach-Object { "Grafana 재시작: $_" }
    Pop-Location
    for ($i = 0; $i -lt 40; $i++) {
        try { if ((Invoke-WebRequest -UseBasicParsing 'http://localhost:3000/api/health' -TimeoutSec 5).StatusCode -eq 200) { "Grafana 응답 ($(($i + 1) * 3)초)"; break } } catch { }
        Start-Sleep -Seconds 3
    }
}
function PromQuery([string]$q) {
    try {
        $r = Invoke-RestMethod -TimeoutSec 10 "http://localhost:9091/api/v1/query?query=$([uri]::EscapeDataString($q))"
        if (-not $r.data.result) { return "(결과 없음) $q" }
        $r.data.result | ForEach-Object { "$(($_.metric.PSObject.Properties | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ',') => $($_.value[1])" }
    } catch { "Prometheus 조회 실패: $($_.Exception.Message)" }
}
function GameAlertStates {
    try {
        $r = Invoke-RestMethod -TimeoutSec 10 -Headers $GrafanaAuth 'http://localhost:3000/api/prometheus/grafana/api/v1/rules'
        foreach ($g in $r.data.groups) { foreach ($rule in $g.rules) { "[$($g.name)] $($rule.name) : state=$($rule.state) health=$($rule.health)" } }
    } catch { "Grafana 규칙 조회 실패: $($_.Exception.Message) (admin/admin 이 아니면 알려 주세요)" }
}
function LokiCount([string]$selector, [string]$range = '5m') {
    try {
        $q = "sum(count_over_time(${selector}[$range]))"
        $r = Invoke-RestMethod -TimeoutSec 10 "http://localhost:3100/loki/api/v1/query?query=$([uri]::EscapeDataString($q))"
        if ($r.data.result) { $r.data.result[0].value[1] } else { 0 }
    } catch { "Loki 조회 실패: $($_.Exception.Message)" }
}
function RuleState([string]$name) {
    try {
        $r = Invoke-RestMethod -TimeoutSec 10 -Headers $GrafanaAuth 'http://localhost:3000/api/prometheus/grafana/api/v1/rules'
        foreach ($g in $r.data.groups) { foreach ($rule in $g.rules) { if ($rule.name -eq $name) { return $rule.state } } }
        return '(규칙 없음)'
    } catch { return "(조회 실패)" }
}
# 규칙이 원하는 상태가 될 때까지 기다린다. 60초마다 진행 상황과 함께 볼 지표를 남긴다
function WaitRule([string]$name, [string]$want, [int]$timeoutSec, [string]$watch) {
    $t0 = Get-Date; $next = 60
    while ($true) {
        $st = RuleState $name
        $el = [int]((Get-Date) - $t0).TotalSeconds
        if ($st -eq $want) { W "  -> '$name' = $want  ($(Get-Date -Format 'HH:mm:ss'), 시작 후 ${el}초)"; return $el }
        if ($el -ge $next) {
            $v = if ($watch) { (PromQuery $watch) -join ' | ' } else { '' }
            W "  ... ${el}초: 상태 $st  $v"; $next += 60
        }
        if ($el -gt $timeoutSec) { W "  !!! ${timeoutSec}초가 지나도 '$name' 이 $want 가 아니다 (마지막 상태 $st)"; return -1 }
        Start-Sleep -Seconds 5
    }
}
function FillProduce([string]$tpl, [hashtable]$map, [string]$topic) {
    $text = Get-Content (Join-Path $Kit "$tpl.template.txt") -Raw -Encoding UTF8
    foreach ($k in $map.Keys) { $text = $text.Replace($k, $map[$k]) }
    $out = Join-Path $Kit "$tpl.txt"
    [System.IO.File]::WriteAllText($out, $text, (New-Object System.Text.UTF8Encoding $false))
    Produce $out $topic | Out-Null
}
if ($E.Post -eq 'B-12') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    $stuckFlag = Join-Path $OutDir 'b12-stuck.flag'
    $pubFlag = Join-Path $OutDir 'b12-publish-fail.flag'
    Remove-Item $stuckFlag, $pubFlag -ErrorAction SilentlyContinue
    Run '관측 구성 기동 (develop + observe, 규칙은 game 만)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b12.sql') } | Out-Null
    Push-Location $Kit; Run '데이터 생성 (gen_b12.py)' { python gen_b12.py } | Out-Null; Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_b12.sql') } | Out-Null
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    $sf = $stuckFlag -replace '\\', '/'; $pf = $pubFlag -replace '\\', '/'
    Start-AppWith "--rpgym.experiment.stuck-user-prefix=b1200002- --rpgym.experiment.stuck-flag=$sf --rpgym.experiment.publish-fail-flag=$pf"
    W "기준 상태 대기 (70초: 규칙이 한 번 이상 평가되게)"; Start-Sleep -Seconds 70
    Run '시작 전 규칙 상태 (기대: 전부 inactive)' { GameAlertStates } | Out-Null
    Run 'Grafana 에 올라간 lag 규칙 식 (기대: rpgym_game_consumer_group_lag)' {
        try { (Invoke-RestMethod -TimeoutSec 10 -Headers $GrafanaAuth 'http://localhost:3000/api/v1/provisioning/alert-rules/game-consumer-lag-stuck').data | ForEach-Object { $_.model.expr } | Where-Object { $_ } }
        catch { "규칙 조회 실패: $($_.Exception.Message)" }
    } | Out-Null
    Run '커밋 기준 lag 지표가 들어오는지 (R-23, 기대: 파티션 3줄)' { PromQuery 'rpgym_game_consumer_group_lag{job=~"game-.*"}' } | Out-Null

    $s1 = $s2 = $s2r = $s3 = $s3r = '(건너뜀)'
    if ($Phases -match '1') {
    W ""; W "## 1) DLT 적재 알림 (규칙: 10분 안에 DLT 적재 > 0 이면 즉시, Slack 은 firing 뒤 group_wait 10초)"
    W "주입: 깨진 JSON 1건  $(Get-Date -Format 'HH:mm:ss')"
    FillProduce '02_p1_dlt' $map 'health.events'
    $s1 = WaitRule 'Game Consumer DLT Detected' 'firing' 300 'sum(rpgym_kafka_consumer_dlt_total{job=~"game-.*"})'
    }
    # 2) 지연: 지켜볼 지표 = 커밋 기준 lag(R-23, 알림이 쓰는 것) + 예전 records_lag_max(비교용)
    $lagWatch = 'max(rpgym_game_consumer_group_lag{job=~"game-.*"}) or vector(-1)'
    if ($Phases -match '2') {

    W ""; W "## 2) 컨슈머 지연 알림 (규칙: lag 이 10분 내내 0 으로 안 내려오면. 기대 firing 약 10~11분)"
    New-Item -ItemType File -Path $stuckFlag -Force | Out-Null
    W "주입: stuck 파일 생성 + 같은 유저 이벤트 6건  $(Get-Date -Format 'HH:mm:ss')"
    FillProduce '03_p2_lag' $map 'health.events'
    $s2 = WaitRule 'Game Consumer Lag Stuck' 'firing' 1200 $lagWatch
    Run '비교: 같은 시각 예전 지표 records_lag_max (R-23 전 알림이 보던 것)' { PromQuery 'max(kafka_consumer_fetch_manager_records_lag_max{job=~"game-.*"})' } | Out-Null
    Remove-Item $stuckFlag -ErrorAction SilentlyContinue
    W "해제: stuck 파일 삭제  $(Get-Date -Format 'HH:mm:ss')"
    $s2r = WaitRule 'Game Consumer Lag Stuck' 'inactive' 300 $lagWatch
    }
    if ($Phases -match '3') {

    W ""; W "## 3) Outbox 최장 대기 알림 (규칙: 가장 오래된 PENDING 이 60초 초과인 상태가 2분 지속. 기대 firing 약 3~4분)"
    New-Item -ItemType File -Path $pubFlag -Force | Out-Null
    W "주입: 발행 실패 파일 생성 + 퀘스트 완료 이벤트 1건  $(Get-Date -Format 'HH:mm:ss')"
    FillProduce '04_p3_outbox' $map 'health.events'
    $s3 = WaitRule 'Game Outbox Oldest Pending High' 'firing' 600 'max(rpgym_game_outbox_oldest_pending_age_seconds{job=~"game-.*"})'
    Remove-Item $pubFlag -ErrorAction SilentlyContinue
    W "해제: 발행 실패 파일 삭제  $(Get-Date -Format 'HH:mm:ss')"
    $s3r = WaitRule 'Game Outbox Oldest Pending High' 'inactive' 300 'max(rpgym_game_outbox_oldest_pending_age_seconds{job=~"game-.*"})'
    Run 'Outbox 행 (기대: QUEST_COMPLETED 1건 PUBLISHED)' { Sql "SELECT event_type, status, to_char(created_at, 'HH24:MI:SS') AS created, to_char(published_at AT TIME ZONE 'Asia/Seoul', 'HH24:MI:SS') AS published FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1200004%';" } | Out-Null
    }

    W ""; W "### 요약 (주입 -> Grafana firing 까지 초. Slack 도착은 firing 뒤 약 10초 = group_wait)"
    W "DLT 적재: $s1 초 / 컨슈머 지연: $s2 초 (해제 후 inactive 까지 $s2r 초) / Outbox 최장 대기: $s3 초 (해제 후 inactive 까지 $s3r 초)"
    Run 'Grafana 로그: 알림 발송 흔적 (slack · notify)' {
        & docker logs rp-gym-grafana --since 40m 2>&1 | ForEach-Object { "$_" } | Where-Object { $_ -match 'slack|[Nn]otif' -and $_ -notmatch 'plugin' } | Select-Object -Last 30
    } | Out-Null
    W ""; W "### 앱 종료"
    Stop-App
    Remove-Item $stuckFlag, $pubFlag -ErrorAction SilentlyContinue
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# 앱 지표를 Prometheus 를 거치지 않고 바로 읽어 한 줄로 (장애 중 타임라인용)
function Snap([string]$tag) {
    $lag = 0; $age = '-'; $dlt = '-'
    foreach ($l in (Metrics '^rpgym_game_consumer_group_lag\{|^rpgym_game_outbox_oldest_pending_age_seconds|^rpgym_kafka_consumer_dlt_total')) {
        $v = ($l -split ' ')[-1]
        if ($l -match '^rpgym_game_consumer_group_lag') { $lag += [double]$v }
        elseif ($l -match '^rpgym_game_outbox_oldest') { $age = [math]::Round([double]$v, 1) }
        elseif ($l -match '^rpgym_kafka_consumer_dlt_total') { $dlt = $v }
    }
    W "  $(Get-Date -Format 'HH:mm:ss') [$tag] 커밋 기준 lag 합 $lag · Outbox 최장 대기 ${age}초 · DLT 적재 $dlt"
}
function CountSql([string]$q) {
    $r = (Sql $q) -match '^\s*\d+\s*$' | Select-Object -First 1
    if ($r) { [int]$r } else { -1 }
}
function Wait-Kafka {
    for ($i = 0; $i -lt 40; $i++) {
        & docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1 | Out-Null
        if ($LASTEXITCODE -eq 0) { return "Kafka 응답 ($(($i + 1) * 3)초)" }
        Start-Sleep -Seconds 3
    }
    return 'Kafka 가 2분 안에 응답하지 않음'
}
function DltOffsetMap {
    $m = @{}
    & docker exec rp-gym-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic health.events.DLT 2>&1 |
        ForEach-Object { "$_" } | Where-Object { $_ -match '^health\.events\.DLT:(\d+):(\d+)' } | ForEach-Object { $m[[int]$Matches[1]] = [long]$Matches[2] }
    return $m
}
# Loki 에서 시작 시각 이후 줄을 찾는다. 반환: "N 줄" 과 앞 몇 줄
function LokiFind([string]$logql, [DateTimeOffset]$since, [int]$show = 5) {
    try {
        $start = $since.ToUnixTimeMilliseconds() * 1000000
        $end = [DateTimeOffset]::Now.ToUnixTimeMilliseconds() * 1000000
        $u = "http://localhost:3100/loki/api/v1/query_range?query=$([uri]::EscapeDataString($logql))&start=$start&end=$end&limit=1000&direction=forward"
        $r = Invoke-RestMethod -TimeoutSec 15 $u
        $lines = @(); foreach ($st in $r.data.result) { foreach ($v in $st.values) { $lines += $v[1] } }
        "$($lines.Count) 줄  ($logql)"
        $lines | Select-Object -First $show | ForEach-Object { "    | " + $(if ($_.Length -gt 230) { $_.Substring(0, 230) + ' …' } else { $_ }) }
    } catch { "Loki 조회 실패: $($_.Exception.Message)" }
}
if ($E.Post -eq 'B-14') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    Run '관측 구성 기동 (develop + observe, 규칙은 game 만)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b14.sql') } | Out-Null
    Push-Location $Kit; Run '데이터 생성 (gen_b14.py)' { python gen_b14.py } | Out-Null; Pop-Location
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    # Eureka 가 없어서 30초마다 찍히는 heartbeat ERROR 가 ERROR 검색을 덮으므로 이 실험에서는 끈다
    Start-AppWith '--rpgym.experiment.fail-user-prefix=b1400002- --eureka.client.enabled=false'
    W "기준 상태 대기 (70초: DLT 규칙이 한 번 이상 평가되게, 지난 10분 안에 DLT 적재가 있었다면 그것이 끝나길)"; Start-Sleep -Seconds 70
    Run 'DLT 규칙 시작 상태 (기대: inactive)' { "Game Consumer DLT Detected : $(RuleState 'Game Consumer DLT Detected')" } | Out-Null
    if ((RuleState 'Game Consumer DLT Detected') -ne 'inactive') {
        # 직전 실행의 DLT 가 10분 창 안에 있으면 이미 firing 이라 이번 주입의 알림 시간을 잴 수 없고 Slack 도 새로 안 온다
        W "직전 DLT 가 10분 창 안에 남아 있다 -> inactive 가 될 때까지 대기 (최대 12분)"
        if ((WaitRule 'Game Consumer DLT Detected' 'inactive' 720 'sum(increase(rpgym_kafka_consumer_dlt_total{job=~"game-.*"}[10m]))') -lt 0) { Fail 'DLT 규칙이 inactive 로 돌아오지 않는다' }
    }
    $before = DltOffsetMap
    W "DLT 파티션별 끝 오프셋 (주입 전): $(($before.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ' ')"

    $tInject = [DateTimeOffset]::Now
    W ""; W "## 주입: 깨진 JSON 1건(eventId ...001) + 코드 버그 1건(eventId ...002)  $($tInject.ToString('HH:mm:ss'))"
    FillProduce '02_inject' $map 'health.events'
    $s = WaitRule 'Game Consumer DLT Detected' 'firing' 300 'sum(rpgym_kafka_consumer_dlt_total{job=~"game-.*"})'
    W "주입 -> Grafana firing: $s 초 (Slack 은 그 뒤 약 10초)"
    W "Loki 수집 대기 (20초)"; Start-Sleep -Seconds 20

    W ""; W "## 사람이 하는 일 1: Slack 을 보고 Loki 에서 찾기 (주입 시각부터)"
    $from = $tInject.AddSeconds(-5)
    Run 'Loki: 깨진 JSON 의 eventId 로' { LokiFind '{job="game-blue"} |= "b1400009-0000-4000-8000-000000000001"' $from } | Out-Null
    Run 'Loki: 코드 버그의 eventId 로' { LokiFind '{job="game-blue"} |= "b1400009-0000-4000-8000-000000000002"' $from } | Out-Null
    Run 'Loki: 코드 버그 유저 id 로' { LokiFind '{job="game-blue"} |= "b1400002-0000-4000-8000-000000000001"' $from } | Out-Null
    Run 'Loki: DLT · 예외 단어로 (계약 위반 · 리스너 실패 · 재시도 소진)' { LokiFind '{job="game-blue"} |~ "(?i)DLT|dead.?letter|ContractViolation|ListenerExecutionFailed|exhausted|injected bug"' $from 8 } | Out-Null
    Run 'Loki: ERROR 줄 전부 (주입 이후)' { LokiFind '{job="game-blue"} |= " ERROR "' $from 8 } | Out-Null
    Run 'Loki: R-24 DLT 적재 줄 (기대 2줄: 위치 · eventId · 원인)' { LokiFind '{job="game-blue"} |= "DLT 적재"' $from 8 } | Out-Null
    Run 'Loki: R-24 재시도 줄 (기대: 버그 건 시도 1~4회, 깨진 JSON 은 재시도 없음)' { LokiFind '{job="game-blue"} |= "컨슈머 처리 실패"' $from 8 } | Out-Null

    W ""; W "## 사람이 하는 일 2: DLT 레코드 헤더 직접 열기 (주입 전 끝 오프셋부터)"
    foreach ($p in ($before.Keys | Sort-Object)) {
        $o = $before[$p]
        $raw = (& docker exec rp-gym-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic health.events.DLT --partition $p --offset $o --max-messages 5 --timeout-ms 5000 --property print.key=true --property print.headers=true 2>&1 | ForEach-Object { "$_" }) -join "`n"
        $keys = [regex]::Matches($raw, 'b14000\d\d-0000-4000-8000-\d{12}') | ForEach-Object { $_.Value } | Select-Object -Unique
        if (-not $keys) { continue }
        W "  파티션 $p · 키/식별자: $($keys -join ', ')"
        foreach ($m in [regex]::Matches($raw, 'kafka_dlt-exception-(?:cause-)?fqcn:([^,\n]+)')) { W "    예외 클래스: $($m.Groups[1].Value)" }
        foreach ($m in [regex]::Matches($raw, 'kafka_dlt-exception-message:([^\n]{0,200})')) { W "    예외 메시지: $($m.Groups[1].Value)" }
        # 원본 오프셋 헤더는 8바이트 숫자라 콘솔에서 깨진다. R-24 로그 줄의 @오프셋으로 대신 본다
    }
    Run 'DLT 적재 카운터 (기대 2)' { Metrics '^rpgym_kafka_consumer_dlt_total' } | Out-Null
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# ---------------------------------------------------------------- B-17: trace 이어짐
function OffsetMapOf([string]$topic) {
    $m = @{}
    $pat = '^' + [regex]::Escape($topic) + ':(\d+):(\d+)'
    & docker exec rp-gym-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $topic 2>&1 |
        ForEach-Object { "$_" } | Where-Object { $_ -match $pat } | ForEach-Object { $m[[int]$Matches[1]] = [long]$Matches[2] }
    return $m
}
function ZipkinTrace([string]$tid) {
    try { return @(Invoke-RestMethod -TimeoutSec 10 "http://localhost:9411/api/v2/trace/$tid") } catch { return @() }
}
function ShowSpans($spans) {
    foreach ($s in ($spans | Sort-Object { [double]$_.timestamp })) {
        $tags = @()
        if ($s.tags) { foreach ($pr in $s.tags.PSObject.Properties) { if ($pr.Name -match 'destination|topic|listener') { $tags += "$($pr.Name)=$($pr.Value)" } } }
        $t = [DateTimeOffset]::FromUnixTimeMilliseconds([long]([double]$s.timestamp / 1000)).ToLocalTime().ToString('HH:mm:ss.fff')
        $par = if ($s.parentId) { $s.parentId } else { '(뿌리)' }
        "  $t [$($s.localEndpoint.serviceName)] '$($s.name)' kind=$($s.kind) id=$($s.id) parent=$par $([math]::Round([double]$s.duration / 1000, 1))ms $($tags -join ' ')"
    }
}
function OX([bool]$ok) { if ($ok) { 'O' } else { 'X' } }
if ($E.Post -eq 'B-17') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    $tid = 'b1700000000000000000000000000001'; $psid = 'b170000000000001'
    Run '관측 구성 기동 (develop + observe, 규칙은 game 만)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b17.sql') } | Out-Null
    $b17Lua = "local n=0 for _,m in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do if string.sub(m,1,9)=='b1700001-' then redis.call('ZREM',KEYS[1],m) n=n+1 end end return n"
    W "랭킹에서 지난 실험 유저 지우기: $((& docker exec rp-gym-redis redis-cli EVAL $b17Lua 1 ranking:global 2>&1) -join ' ')"
    Push-Location $Kit; Run '데이터 생성 (gen_b17.py)' { python gen_b17.py } | Out-Null; Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_b17.sql') } | Out-Null
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    Start-AppWith '--eureka.client.enabled=false'
    W "기준 상태 대기 (10초)"; Start-Sleep -Seconds 10
    $before = OffsetMapOf 'game.events'
    W "game.events 파티션별 끝 오프셋 (주입 전): $(($before.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ' ')"

    $tInject = [DateTimeOffset]::Now
    W ""; W "## 주입  $($tInject.ToString('HH:mm:ss'))"
    W "  A) Health 가 보냈다고 치고 traceparent 헤더를 붙인 1건: 00-$tid-$psid-01 (유저 ...001)"
    $text = Get-Content (Join-Path $Kit '02_traced.template.txt') -Raw -Encoding UTF8
    foreach ($k in $map.Keys) { $text = $text.Replace($k, $map[$k]) }
    $tracedFile = Join-Path $Kit '02_traced.txt'
    [System.IO.File]::WriteAllText($tracedFile, $text, (New-Object System.Text.UTF8Encoding $false))
    & docker cp $tracedFile 'rp-gym-kafka:/tmp/b17.txt' 2>&1 | Out-Null
    & docker exec rp-gym-kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic health.events --property parse.key=true --property key.separator='|' --property parse.headers=true < /tmp/b17.txt" 2>&1 | ForEach-Object { W "    $_" }
    W "  B) 대조: 헤더 없이 1건 (유저 ...002)"
    FillProduce '03_plain' $map 'health.events'

    $q = "SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1700002%' AND status = 'PUBLISHED';"
    $t0 = Get-Date; $n = 0
    while (((Get-Date) - $t0).TotalSeconds -lt 60) { $n = CountSql $q; if ($n -ge 2) { break }; Start-Sleep -Seconds 2 }
    W "Outbox 발행 (기대 2): $n  ($([int]((Get-Date) - $t0).TotalSeconds)초)"
    Run 'Outbox 행에 저장된 trace_context (R-17: 적재 때 traceparent 저장)' {
        Sql "SELECT right(aggregate_id::text, 3) AS q, event_type, status, trace_context FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1700002%' ORDER BY aggregate_id;"
    } | Out-Null
    W "Zipkin 전송 대기 (15초: span 은 비동기로 모아 보낸다)"; Start-Sleep -Seconds 15

    W ""; W "## 1) 입구부터 출구까지: Zipkin 에서 넣은 traceId 하나로 조회"
    $spans = ZipkinTrace $tid
    Run "Zipkin trace $tid (span $($spans.Count)개, 시간순)" { ShowSpans $spans } | Out-Null
    $okIn = @($spans | Where-Object { $_.kind -eq 'CONSUMER' -and $_.parentId -eq $psid }).Count -gt 0
    $okRelay = @($spans | Where-Object { $_.name -eq 'outbox publish' }).Count -gt 0
    $okSend = @($spans | Where-Object { $_.kind -eq 'PRODUCER' }).Count -gt 0

    W ""; W "## 2) 출구: game.events 레코드 헤더 (= Notification 이 이어 받을 값)"
    $outTid = $null; $ctlTid = $null
    foreach ($p in ($before.Keys | Sort-Object)) {
        $raw = & docker exec rp-gym-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic game.events --partition $p --offset $before[$p] --max-messages 20 --timeout-ms 5000 --property print.key=true --property print.headers=true 2>&1 | ForEach-Object { "$_" }
        foreach ($line in $raw) {
            if ($line -notmatch 'traceparent:00-([0-9a-f]{32})-([0-9a-f]{16})-\d\d') { continue }
            $ltid = $Matches[1]; $lsp = $Matches[2]
            if ($line -match 'b170000[12]-0000-4000-8000-000000000001') { $outTid = $ltid; W "  유저 ...001 (파티션 $p): traceparent 00-$ltid-$lsp" }
            elseif ($line -match 'b170000[12]-0000-4000-8000-000000000002') { $ctlTid = $ltid; W "  대조 ...002 (파티션 $p): traceparent 00-$ltid-$lsp" }
        }
    }
    if (-not $outTid) { W "  !!! 유저 ...001 의 발행 레코드에서 traceparent 를 못 찾음" }
    $okOut = ($outTid -eq $tid)

    W ""; W "## 3) 같은 traceId 로 Loki (로그 줄 [traceId,spanId])"
    $from = $tInject.AddSeconds(-5)
    Run "Loki: traceId $tid" { LokiFind "{job=`"game-blue`"} |= `"$tid`"" $from 10 } | Out-Null

    W ""; W "## 4) 대조: 헤더 없이 넣은 건은 Game 소비에서 새 trace 가 시작돼야 한다"
    $okCtl = $false
    if ($ctlTid) {
        $cs = ZipkinTrace $ctlTid
        Run "Zipkin trace $ctlTid (span $($cs.Count)개)" { ShowSpans $cs } | Out-Null
        $okCtl = ($ctlTid -ne $tid) -and (@($cs | Where-Object { $_.kind -eq 'CONSUMER' -and -not $_.parentId }).Count -gt 0)
    } else { W "  !!! 대조 건의 발행 레코드를 못 찾음" }

    W ""; W "## 판정"
    W "  [$(OX $okIn)] 입구: Game 소비 span 의 부모 = 넣은 span $psid (Health 의 trace 를 이어 받음)"
    W "  [$(OX $okRelay)] 같은 trace 안에 'outbox publish' span (다른 스레드 · 다른 트랜잭션의 발행이 이어 붙음, R-17)"
    W "  [$(OX $okSend)] 같은 trace 안에 game.events 발행 span (PRODUCER)"
    W "  [$(OX $okOut)] 출구: game.events 레코드 traceparent 의 traceId = $tid"
    W "  [$(OX $okCtl)] 대조: 헤더 없는 건은 다른 traceId, 소비 span 이 뿌리"
    W ""; W "캡처: http://localhost:9411/zipkin/traces/$tid  (앱을 꺼도 Zipkin 컨테이너에 남아 있다)"
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# ---------------------------------------------------------------- B-19: Outbox 발행측 최소 한 번
function ReadGameEvents([hashtable]$from, [string]$userRegex) {
    # 원본 줄을 결과 파일 옆에 남긴다(세기가 이상하면 이 파일로 직접 센다). 2026-10-12 B-19 · C-20 에서 0 줄로 나온 원인 추적용
    $dump = $Result -replace '\.txt$', "-gameevents-$((Get-Date).ToString('HHmmss')).txt"
    $all = @()
    foreach ($k in @($from.Keys | Sort-Object)) {
        $off = [string]$from[$k]
        $all += @(& docker exec rp-gym-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic game.events --partition "$k" --offset $off --max-messages 2000 --timeout-ms 8000 --property print.key=true 2>&1 | ForEach-Object { "$_" })
    }
    [System.IO.File]::WriteAllLines($dump, [string[]]$all, (New-Object System.Text.UTF8Encoding $false))
    $hit = @($all | Where-Object { ($_ -match $userRegex) -and ($_ -match '"eventId"\s*:\s*"[0-9a-f-]{36}"') })
    W "  (game.events 원본 $($all.Count) 줄 -> $dump, 그중 대상 $($hit.Count) 줄)"
    return ,$hit
}
function EventIdsOf($lines) { @($lines | ForEach-Object { if ($_ -match '"eventId"\s*:\s*"([0-9a-f-]{36})"') { $Matches[1] } }) }
if ($E.Post -eq 'B-19') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    # PowerShell 변수는 대소문자를 가리지 않는다 ($N = $n). 겹치지 않는 이름을 쓴다
    $total = 50; $crashAt = 10
    $pubFlag = Join-Path $OutDir 'b19-publish-fail.flag'
    $crashFlag = Join-Path $OutDir 'b19-crash.flag'
    Remove-Item $pubFlag, $crashFlag -ErrorAction SilentlyContinue
    Run '관측 구성 기동 (develop + observe)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b19.sql') } | Out-Null
    $b19Lua = "local n=0 for _,m in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do if string.sub(m,1,9)=='b1900001-' then redis.call('ZREM',KEYS[1],m) n=n+1 end end return n"
    W "랭킹에서 지난 실험 유저 지우기: $((& docker exec rp-gym-redis redis-cli EVAL $b19Lua 1 ranking:global 2>&1) -join ' ')"
    Push-Location $Kit; Run '데이터 생성 (gen_b19.py)' { python gen_b19.py } | Out-Null; Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_b19.sql') } | Out-Null
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    $qAll = "SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1900002%';"
    $qPend = "SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1900002%' AND status <> 'PUBLISHED';"
    $qStatus = "SELECT status, count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1900002%' GROUP BY status ORDER BY status;"

    W ""; W "## 1) 발행을 막아 둔 채 Outbox $total 행 쌓기"
    New-Item -ItemType File -Force -Path $pubFlag | Out-Null
    $pf = $pubFlag -replace '\\', '/'; $cf = $crashFlag -replace '\\', '/'
    Start-AppWith "--eureka.client.enabled=false --rpgym.experiment.publish-fail-flag=$pf --rpgym.experiment.crash-after-send-flag=$cf --rpgym.experiment.crash-after-sends=$crashAt"
    $before = OffsetMapOf 'game.events'
    W "game.events 파티션별 끝 오프셋 (시작): $(($before.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join ' ')"
    FillProduce '02_events' $map 'health.events'
    $t0 = Get-Date; $n = 0
    while (((Get-Date) - $t0).TotalSeconds -lt 90) { $n = CountSql $qAll; if ($n -ge $total) { break }; Start-Sleep -Seconds 2 }
    W "Outbox 적재 (기대 $total): $n  ($([int]((Get-Date) - $t0).TotalSeconds)초)"
    Start-Sleep -Seconds 3
    Run 'Outbox 상태 (기대: PENDING 50)' { Sql $qStatus } | Out-Null
    $pre = ReadGameEvents $before 'b1900001-'
    W "game.events 에 나간 b19 레코드 (기대 0): $($pre.Count)"

    W ""; W "## 2) 막은 것을 풀면서 '$crashAt 건째 발행 직후 즉시 종료' 켜기"
    $crashLog = $script:CurLog
    New-Item -ItemType File -Force -Path $crashFlag | Out-Null
    Remove-Item $pubFlag -ErrorAction SilentlyContinue
    $tc = [DateTimeOffset]::Now; W "  풀어 줌: $($tc.ToString('HH:mm:ss.fff'))"
    $t0 = Get-Date
    while (((Get-Date) - $t0).TotalSeconds -lt 60) {
        if (-not (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue)) { break }
        Start-Sleep -Milliseconds 500
    }
    $died = -not (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue)
    W "  앱 종료 확인: $died  ($([math]::Round(((Get-Date) - $t0).TotalSeconds, 1))초)"
    if (-not $died) { Fail '앱이 스스로 멈추지 않았다 (crash-after-send-flag 가 안 먹음 = exp/r12-race 브랜치 코드인지 확인)' }
    Start-Sleep -Seconds 5
    if ($script:Gradle -and -not $script:Gradle.HasExited) { & taskkill /T /F /PID $script:Gradle.Id 2>&1 | Out-Null }
    Remove-Item $crashFlag -ErrorAction SilentlyContinue
    Run '죽기 직전 앱 로그 [B-19]' { Select-String -Path $crashLog -Pattern '\[B-19\]' | ForEach-Object { ($_.Line -split ' : ', 2)[-1] } } | Out-Null
    Run 'Outbox 상태 (기대: 여전히 PENDING 50 = PUBLISHED 표시가 커밋되지 않음)' { Sql $qStatus } | Out-Null
    $mid = ReadGameEvents $before 'b1900001-'
    W "game.events 에 이미 나간 b19 레코드 (기대 $crashAt): $($mid.Count)"

    W ""; W "## 3) 플래그 없이 재기동 -> PENDING 을 다시 집어 발행"
    Start-AppWith '--eureka.client.enabled=false'
    $t0 = Get-Date; $p = -1
    while (((Get-Date) - $t0).TotalSeconds -lt 60) { $p = CountSql $qPend; if ($p -eq 0) { break }; Start-Sleep -Seconds 2 }
    W "미발행 0 까지: $([int]((Get-Date) - $t0).TotalSeconds)초 (마지막 미발행 $p)"
    Run 'Outbox 상태 (기대: PUBLISHED 50)' { Sql $qStatus } | Out-Null

    W ""; W "## 4) game.events 에서 세기"
    $all = ReadGameEvents $before 'b1900001-'
    $ids = EventIdsOf $all
    $g = @($ids | Group-Object)
    $dups = @($g | Where-Object { $_.Count -gt 1 })
    W "  레코드 $($ids.Count) · 서로 다른 eventId $($g.Count) · 2번 이상 나간 eventId $($dups.Count) (중복 레코드 $($ids.Count - $g.Count))"
    $dupSame = $true
    foreach ($d in ($dups | Select-Object -First 12)) {
        $copies = @($all | Where-Object { $_ -match $d.Name } | Select-Object -Unique)
        if ($copies.Count -ne 1) { $dupSame = $false }
        W "    eventId $($d.Name) x$($d.Count) · 서로 다른 내용 $($copies.Count)"
    }
    $outboxIds = @(Sql "SELECT event_id FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b1900002%';" | Where-Object { $_ -match '^\s*[0-9a-f-]{36}\s*$' } | ForEach-Object { $_.Trim() })
    $missing = @($outboxIds | Where-Object { $ids -notcontains $_ })
    W "  Outbox eventId $($outboxIds.Count) 개 중 game.events 에 없는 것: $($missing.Count)"
    Run '소비 쪽 (기대: 원장 50행 · XP 500 · 2행 이상 유저 0)' {
        Sql "SELECT count(*) AS ledger_rows, coalesce(sum(amount),0) AS xp, (SELECT count(*) FROM (SELECT user_id FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b1900001%' GROUP BY user_id HAVING count(*) > 1) x) AS users_over_1 FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b1900001%';"
    } | Out-Null

    W ""; W "## 판정"
    W "  [$(OX ($outboxIds.Count -eq $total -and $missing.Count -eq 0))] 유실 0: Outbox $total 건이 전부 game.events 에 있다"
    W "  [$(OX ($p -eq 0))] 최종 미발행 0"
    W "  [$(OX ($mid.Count -ge 1))] 죽기 전에 나간 $($mid.Count) 건은 PUBLISHED 로 커밋되지 못했다 -> 재기동 뒤 다시 나감 (중복 레코드 $($ids.Count - $g.Count))"
    W "  [$(OX $dupSame)] 중복 레코드는 같은 eventId · 같은 내용 -> 받는 쪽이 eventId 로 걸러낼 수 있다"
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# ---------------------------------------------------------------- B-15: lag 알림 오탐 · 정지 감지
function RuleStateHealth([string]$name) {
    try {
        $r = Invoke-RestMethod -TimeoutSec 10 -Headers $GrafanaAuth 'http://localhost:3000/api/prometheus/grafana/api/v1/rules'
        foreach ($g in $r.data.groups) { foreach ($rule in $g.rules) { if ($rule.name -eq $name) { return "$($rule.state)/$($rule.health)" } } }
        return '(규칙 없음)'
    } catch { return '(조회 실패)' }
}
# 배경 트래픽 한 묶음: 유저 300명 중 6명을 골라 동기화 이벤트 1건씩 (퀘스트 없음 = 최신 측정값만 갱신)
function B15Batch {
    $script:b15n++
    $ts = [DateTimeOffset]::Now
    $day = $ts.ToString('yyyy-MM-dd'); $at = $ts.ToString('yyyy-MM-ddTHH:mm:sszzz')
    $lines = foreach ($j in 1..6) {
        $u = 'b1500001-0000-4000-8000-{0:D12}' -f (Get-Random -Minimum 1 -Maximum 301)
        $ev = 'b1500009-{0:D4}-4000-8000-{1:D12}' -f ($script:b15n % 10000), ($script:b15n * 10 + $j)
        $steps = $script:b15n * 10
        "$u|{`"eventId`":`"$ev`",`"eventType`":`"HEALTH_ACTIVITY_SYNCED`",`"userId`":`"$u`",`"data`":{`"activityDate`":`"$day`",`"measuredAt`":`"$at`",`"cumulative`":{`"steps`":$steps,`"activeMinutes`":0,`"activeCalories`":0}}}"
    }
    $f = Join-Path $Kit 'batch.txt'
    [System.IO.File]::WriteAllText($f, (($lines -join "`n") + "`n"), (New-Object System.Text.UTF8Encoding $false))
    Produce $f 'health.events' | Out-Null
    $script:b15sent += 6
}
function KafkaGroupLag {
    $sum = 0
    & docker exec rp-gym-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group game-service 2>&1 |
        ForEach-Object { "$_" } | Where-Object { $_ -match '\shealth\.events\s+\d+\s+(\d+|-)\s+(\d+)\s+(\d+|-)' } |
        ForEach-Object { if ($Matches[3] -ne '-') { $sum += [long]$Matches[3] } }
    return $sum
}
# $sec 동안 10초마다 배경 트래픽, 60초마다 상태 한 줄. $stopWhen 상태가 되면 그 시점(초)을 돌려준다
function B15Wait([int]$sec, [string]$tag, [string]$stopWhen) {
    $t0 = Get-Date; $nextTick = Get-Date; $nextLog = (Get-Date).AddSeconds(60)
    $script:b15seen = @{}
    while ($true) {
        $el = [int]((Get-Date) - $t0).TotalSeconds
        if ((Get-Date) -ge $nextTick) { B15Batch; $nextTick = (Get-Date).AddSeconds(10) }
        if ((Get-Date) -ge $nextLog) {
            $sh = RuleStateHealth 'Game Consumer Lag Stuck'; $st = ($sh -split '/')[0]; $script:b15seen[$sh] = 1
            $lag = (PromQuery 'max by (job) (rpgym_game_consumer_group_lag{job=~"game-.*"})') -join ' '
            $win = (PromQuery 'min_over_time(max by (job) (rpgym_game_consumer_group_lag{job=~"game-.*"})[10m:1m])') -join ' '
            $up = (PromQuery 'up{job="game-blue"}') -join ' '
            W "  $(Get-Date -Format 'HH:mm:ss') [$tag +${el}s] 규칙 $sh · 커밋 lag 최대 $lag · 10분 최솟값 $win · $up · 보낸 이벤트 $($script:b15sent)"
            $nextLog = (Get-Date).AddSeconds(60)
            if ($stopWhen -and $st -eq $stopWhen) { return $el }
        }
        if ($el -ge $sec) { return -1 }
        Start-Sleep -Seconds 1
    }
}
if ($E.Post -eq 'B-15') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    $ph = if ($PSBoundParameters.ContainsKey('Phases')) { $Phases } else { '123' }
    $normalMin = 30; $stuckMin = 15; $downMin = 15
    # 3 단계만 돌릴 때(R-25 확인)는 앱 다운 알림이 울리는 데 3분 남짓이면 되므로 5분만 죽인다
    if ($ph -eq '3') { $downMin = 5 }
    W "단계: $ph  (1 평상시 $normalMin 분 오탐 확인 · 2 앱은 살아 있고 전 파티션 멈춤 $stuckMin 분 · 3 앱 자체가 죽음 $downMin 분)"
    $script:b15n = 0; $script:b15sent = 0
    $stuckFlag = Join-Path $OutDir 'b15-stuck.flag'
    Remove-Item $stuckFlag -ErrorAction SilentlyContinue
    Run '관측 구성 기동 (develop + observe, 규칙은 game 만)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b15.sql') } | Out-Null
    $sf = $stuckFlag -replace '\\', '/'
    $appArgs = "--eureka.client.enabled=false --rpgym.experiment.stuck-user-prefix=b1500001- --rpgym.experiment.stuck-flag=$sf"
    Start-AppWith $appArgs
    W "기준 상태 대기 (70초)"; Start-Sleep -Seconds 70
    Run 'lag 규칙 시작 상태 (기대: inactive)' { RuleStateHealth 'Game Consumer Lag Stuck' } | Out-Null
    $p1 = $p2 = $p2r = '(건너뜀)'; $p3 = '(건너뜀)'; $p3down = ''

    if ($ph -match '1') {
        W ""; W "## 1) 평상시 $normalMin 분: 10초마다 6건씩 흘린다 (분당 36건). 규칙이 한 번도 inactive 를 벗어나면 안 된다"
        B15Wait ($normalMin * 60) 'normal' '' | Out-Null
        $seen = ($script:b15seen.Keys | Sort-Object) -join ', '
        Run '이 구간 커밋 lag 순간 최댓값 (0 보다 크면 = 순간 lag 는 생겼다)' { PromQuery "max_over_time(max by (job) (rpgym_game_consumer_group_lag{job=~`"game-.*`"})[$($normalMin)m:30s])" } | Out-Null
        Run '이 구간 규칙 식(10분 최솟값)의 최댓값 (기대 0 = 오탐 없음)' { PromQuery "max_over_time(min_over_time(max by (job) (rpgym_game_consumer_group_lag{job=~`"game-.*`"})[10m:1m])[$($normalMin - 10)m:1m])" } | Out-Null
        W "이 구간에 본 규칙 상태: $seen"
        $p1 = if ($seen -match 'pending|firing|alerting') { "오탐 있음 ($seen)" } else { "오탐 없음 ($seen)" }
    }
    if ($ph -match '2') {
        W ""; W "## 2) 앱은 살아 있고 전 파티션 멈춤 $stuckMin 분 (배경 유저 전부가 stuck 대상 -> 파티션마다 첫 이벤트에서 무한 재시도)"
        New-Item -ItemType File -Force -Path $stuckFlag | Out-Null
        W "주입: stuck 파일 생성  $(Get-Date -Format 'HH:mm:ss')"
        $r = B15Wait ($stuckMin * 60) 'stuck' 'firing'
        $p2 = if ($r -ge 0) { "firing $r 초" } else { "$stuckMin 분 동안 안 울림" }
        if ($r -ge 0) { $rest = $stuckMin * 60 - $r; if ($rest -gt 0) { B15Wait $rest 'stuck' '' | Out-Null } }
        Remove-Item $stuckFlag -ErrorAction SilentlyContinue
        W "해제: stuck 파일 삭제  $(Get-Date -Format 'HH:mm:ss')  (Kafka 쪽 밀린 양 $(KafkaGroupLag))"
        $r = B15Wait 900 'release' 'inactive'
        $p2r = if ($r -ge 0) { "inactive $r 초" } else { '15분 안에 안 풀림' }
    }
    if ($ph -match '3') {
        W ""; W "## 3) 앱 자체가 죽음 $downMin 분 (트래픽은 계속). lag 지표를 앱이 내므로 지표가 사라진다 -> 규칙은 어떻게 되나"
        # (R-25) 시작 전에 앱 다운 규칙이 풀려 있어야 이번 다운의 시간을 잴 수 있다(지난 실험 사이 앱이 꺼져 있던 동안 firing 이었을 수 있음)
        WaitRule 'Game Service Down' 'inactive' 300 '(sum(up{job=~"game-.*"}) or vector(0))' | Out-Null
        Stop-App
        $tDown = Get-Date
        W "앱 종료  $($tDown.ToString('HH:mm:ss'))"
        $rd = WaitRule 'Game Service Down' 'firing' 600 '(sum(up{job=~"game-.*"}) or vector(0))'
        $p3down = if ($rd -ge 0) { "Game Service Down firing: 앱 종료부터 $([int]((Get-Date) - $tDown).TotalSeconds) 초" } else { 'Game Service Down 10분 안에 안 울림' }
        W "  -> $p3down"
        $restMin = [math]::Max(1, $downMin - [int][math]::Ceiling(((Get-Date) - $tDown).TotalMinutes))
        B15Wait ($restMin * 60) 'app down' '' | Out-Null
        $seen = ($script:b15seen.Keys | Sort-Object) -join ', '
        $backlog = KafkaGroupLag
        W "이 구간에 본 lag 규칙 상태: $seen · Kafka 쪽 실제 밀린 양: $backlog 건"
        Run '그 순간 전체 규칙 상태' { GameAlertStates } | Out-Null
        $p3 = "규칙 상태 $seen · 실제 밀린 양 $backlog"
        Start-AppWith $appArgs
        $t0 = Get-Date; $nextTick = Get-Date
        while (((Get-Date) - $t0).TotalSeconds -lt 300) {
            if ((Get-Date) -ge $nextTick) { B15Batch; $nextTick = (Get-Date).AddSeconds(10) }
            $l = KafkaGroupLag; if ($l -le 12) { break }; Start-Sleep -Seconds 5
        }
        W "재기동 뒤 밀린 것 소화: $([int]((Get-Date) - $t0).TotalSeconds)초 (남은 lag $(KafkaGroupLag))"
        $rdr = WaitRule 'Game Service Down' 'inactive' 300 '(sum(up{job=~"game-.*"}) or vector(0))'
        $p3down += " · 재기동 뒤 inactive $rdr 초"
    }

    W ""; W "### 요약"
    W "  1) 평상시 오탐: $p1"
    W "  2) 앱 살아 있고 멈춤: $p2 · 해제 뒤 $p2r"
    W "  3) 앱 자체가 죽음: $p3 · $p3down"
    W "  보낸 배경 이벤트: $($script:b15sent)"
    W ""; W "### 앱 종료"
    Stop-App
    Remove-Item $stuckFlag -ErrorAction SilentlyContinue
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# ---------------------------------------------------------------- C-19 · C-20: Kafka 3대 (R-14)
function TopicDesc([string]$topic) {
    & docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $topic 2>&1 |
        ForEach-Object { "$_" } | Where-Object { $_ -match 'Partition:' } | ForEach-Object { '  ' + (($_ -replace '\s+', ' ').Trim() -replace '^Topic: \S+ ', '') }
}
function LogCount([string]$pattern) { @(Select-String -Path $script:CurLog -Pattern $pattern -ErrorAction SilentlyContinue).Count }
function C20Counts([hashtable]$from, [string]$userRegex, [int]$expect) {
    $lines = ReadGameEvents $from $userRegex   # ReadGameEvents 가 ,배열 로 돌려주므로 @() 로 다시 감싸지 않는다
    $ids = EventIdsOf $lines
    $g = @($ids | Group-Object)
    "  game.events 레코드 $($ids.Count) · 서로 다른 eventId $($g.Count) (기대 $expect) · 2번 이상 나간 eventId $(@($g | Where-Object { $_.Count -gt 1 }).Count)"
}
if ($E.Post -eq 'C-20') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    $HaCompose = $ObserveCompose + @('-f', 'docker-compose-kafka-ha.yml')
    Run '관측 구성 기동 (develop + observe)' { Start-Observe } | Out-Null

    W ""; W "## C-19) Kafka 3대로 바꿔 띄우기 (rp-gym-kafka 는 3대용 볼륨으로 다시 만들어진다)"
    Push-Location $Root
    Run '브로커 3대 + kafka-init' { & docker compose @HaCompose up -d kafka kafka-2 kafka-3 kafka-init 2>&1 | ForEach-Object { "$_" } } | Out-Null
    Pop-Location
    $t0 = Get-Date
    while (((Get-Date) - $t0).TotalSeconds -lt 180) {
        if (((& docker inspect -f '{{.State.Status}}' rp-gym-kafka-init 2>&1) -join '') -eq 'exited') { break }
        Start-Sleep -Seconds 3
    }
    W "kafka-init 끝남 ($([int]((Get-Date) - $t0).TotalSeconds)초, 종료 코드 $((& docker inspect -f '{{.State.ExitCode}}' rp-gym-kafka-init 2>&1) -join ''))"
    Run '토픽 (기대: 6개 · ReplicationFactor 3 · min.insync.replicas=2)' {
        & docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe 2>&1 | ForEach-Object { "$_" } |
            Where-Object { $_ -match '^Topic: \S+\s+TopicId' } | ForEach-Object { ($_ -replace '\s+', ' ').Trim() }
    } | Out-Null
    Run 'game.events 파티션별 리더 · 복제 · ISR (기대: ISR 3대)' { TopicDesc 'game.events' } | Out-Null

    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_c20.sql') } | Out-Null
    $c20Lua = "local n=0 for _,m in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do if string.sub(m,1,9)=='c1000001-' then redis.call('ZREM',KEYS[1],m) n=n+1 end end return n"
    W "랭킹에서 지난 실험 유저 지우기: $((& docker exec rp-gym-redis redis-cli EVAL $c20Lua 1 ranking:global 2>&1) -join ' ')"
    Push-Location $Kit; Run '데이터 생성 (gen_c20.py)' { python gen_c20.py } | Out-Null; Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_c20.sql') } | Out-Null
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    Start-AppWith '--eureka.client.enabled=false --spring.kafka.bootstrap-servers=localhost:19030,localhost:19032,localhost:19033 --rpgym.reconcile.initial-delay=5000 --rpgym.reconcile.interval=15000'
    $dlt0 = DltEnd
    $before = OffsetMapOf 'game.events'
    $pubFail0 = LogCount '발행 실패'
    $qDone1 = "SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%' AND substr(quest_id::text, 25)::bigint BETWEEN 1 AND 100 AND status = 'COMPLETED';"
    $qDone2 = "SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%' AND substr(quest_id::text, 25)::bigint BETWEEN 201 AND 220 AND status = 'COMPLETED';"
    $qPend = "SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND status <> 'PUBLISHED';"

    W ""; W "## C-20 ①) 브로커 1대(kafka-2) 정지: 4초마다 10건씩 10묶음을 흘리는 중 8초에 정지 -> 70초 뒤 재기동"
    $t0 = Get-Date; $k2Stop = $null; $k2Start = $null
    foreach ($c in 1..10) {
        FillProduce ('p1_{0:D2}' -f $c) $map 'health.events'
        if (-not $k2Stop -and ((Get-Date) - $t0).TotalSeconds -ge 8) {
            & docker stop rp-gym-kafka-2 2>&1 | Out-Null; $k2Stop = Get-Date
            W "  kafka-2 정지  $($k2Stop.ToString('HH:mm:ss'))  (묶음 $c 뒤)"
        }
        Start-Sleep -Seconds 4
    }
    Run 'kafka-2 정지 중 game.events 리더 · ISR (기대: 리더가 1 · 3 으로 옮겨지고 ISR 2대)' { TopicDesc 'game.events' } | Out-Null
    while (((Get-Date) - $k2Stop).TotalSeconds -lt 70) { Start-Sleep -Seconds 2 }
    & docker start rp-gym-kafka-2 2>&1 | Out-Null; $k2Start = Get-Date
    W "  kafka-2 재기동  $($k2Start.ToString('HH:mm:ss'))"
    $t1 = Get-Date
    while (((Get-Date) - $t1).TotalSeconds -lt 120) { if ((CountSql $qDone1) -ge 100 -and (CountSql $qPend) -eq 0) { break }; Start-Sleep -Seconds 3 }
    W "  완료 $(CountSql $qDone1)/100 · Outbox 미발행 $(CountSql $qPend)"
    $pubFail1 = (LogCount '발행 실패') - $pubFail0
    W "  이 구간 Outbox '발행 실패' 로그: $pubFail1 줄 (기대 0)"
    W (C20Counts $before 'c1000001-0000-4000-8000-0000000000\d\d|c1000001-0000-4000-8000-000000000100' 100)
    Start-Sleep -Seconds 30
    Run '복구 뒤 ISR (기대: 다시 3대)' { TopicDesc 'game.events' } | Out-Null
    Run '대조 게이지 (기대: 전부 0)' { PromQuery 'rpgym_reconcile_mismatch{job=~"game-.*"}' } | Out-Null

    W ""; W "## C-20 ②) 브로커 2대(kafka-2, kafka-3) 정지 -> ISR 1 < min.insync 2 -> acks=all 쓰기 거부"
    $ner0 = LogCount 'NotEnoughReplicas|NOT_ENOUGH_REPLICAS'
    $pubFail0 = LogCount '발행 실패'
    & docker stop rp-gym-kafka-2 rp-gym-kafka-3 2>&1 | Out-Null; $k23Stop = Get-Date
    W "  kafka-2 · kafka-3 정지  $($k23Stop.ToString('HH:mm:ss'))"
    Start-Sleep -Seconds 15
    Run '정지 뒤 game.events 리더 · ISR (기대: 리더 1, ISR 1대)' { TopicDesc 'game.events' } | Out-Null
    W "  이벤트 20건 주입 (주입 도구는 acks=1: Health 설정이 아니라 입력을 넣기 위한 편의. acks=all 이면 입력도 거부된다)"
    $text = Get-Content (Join-Path $Kit 'p2.template.txt') -Raw -Encoding UTF8
    foreach ($k in $map.Keys) { $text = $text.Replace($k, $map[$k]) }
    $p2File = Join-Path $Kit 'p2.txt'
    [System.IO.File]::WriteAllText($p2File, $text, (New-Object System.Text.UTF8Encoding $false))
    & docker cp $p2File 'rp-gym-kafka:/tmp/c20.txt' 2>&1 | Out-Null
    & docker exec rp-gym-kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic health.events --producer-property acks=1 --property parse.key=true --property key.separator='|' < /tmp/c20.txt" 2>&1 | ForEach-Object { W "    $_" }
    $t1 = Get-Date
    while (((Get-Date) - $t1).TotalSeconds -lt 90) { if ((CountSql $qDone2) -ge 20) { break }; Start-Sleep -Seconds 3 }
    W "  Game 처리 · 완료: $(CountSql $qDone2)/20 ($([int]((Get-Date) - $t1).TotalSeconds)초) · Outbox 미발행 $(CountSql $qPend)"
    $r2 = WaitRule 'Game Outbox Oldest Pending High' 'firing' 420 'max(rpgym_game_outbox_oldest_pending_age_seconds{job=~"game-.*"})'
    W "  -> Outbox 최장 대기 알림: 정지부터 $([int]((Get-Date) - $k23Stop).TotalSeconds)초"
    W "  이 구간 로그: NotEnoughReplicas $((LogCount 'NotEnoughReplicas|NOT_ENOUGH_REPLICAS') - $ner0) 줄 · '발행 실패' $((LogCount '발행 실패') - $pubFail0) 줄 · 오프셋 커밋 실패 $(LogCount 'Offset commit failed|CommitFailed|commit.*(failed|exception)') 줄(누적)"
    Run '발행 실패 로그 예시 (마지막 2줄)' { Select-String -Path $script:CurLog -Pattern '발행 실패' | Select-Object -Last 2 | ForEach-Object { $l = ($_.Line -split ' : ', 2)[-1]; if ($l.Length -gt 300) { $l.Substring(0, 300) + ' …' } else { $l } } } | Out-Null
    & docker start rp-gym-kafka-2 2>&1 | Out-Null; $k2Start = Get-Date
    W "  kafka-2 재기동 (ISR 2 = min.insync 충족)  $($k2Start.ToString('HH:mm:ss'))"
    $t1 = Get-Date
    while (((Get-Date) - $t1).TotalSeconds -lt 180) { if ((CountSql $qPend) -eq 0) { break }; Start-Sleep -Seconds 2 }
    W "  쌓인 Outbox 발행 끝: 재기동부터 $([int]((Get-Date) - $t1).TotalSeconds)초 (남은 미발행 $(CountSql $qPend))"
    & docker start rp-gym-kafka-3 2>&1 | Out-Null
    W "  kafka-3 재기동  $(Get-Date -Format 'HH:mm:ss')"
    W (C20Counts $before 'c1000001-0000-4000-8000-0000000002\d\d' 20)
    $r2r = WaitRule 'Game Outbox Oldest Pending High' 'inactive' 300 'max(rpgym_game_outbox_oldest_pending_age_seconds{job=~"game-.*"})'
    Start-Sleep -Seconds 20
    Run '검증 SQL' { Psql-File (Join-Path $Kit '03_verify_c20.sql') } | Out-Null
    Run '대조 게이지 (기대: 전부 0)' { PromQuery 'rpgym_reconcile_mismatch{job=~"game-.*"}' } | Out-Null
    W "DLT 증가 (기대 0): $((DltEnd) - $dlt0)"
    Run '끝 ISR (기대: 3대)' { TopicDesc 'game.events' } | Out-Null

    W ""; W "### 앱 종료 · Kafka 1대 구성으로 되돌리기"
    Stop-App
    & docker rm -f rp-gym-kafka-2 rp-gym-kafka-3 rp-gym-kafka-init 2>&1 | Out-Null
    Push-Location $Root
    & docker compose @ObserveCompose up -d kafka 2>&1 | ForEach-Object { W "  $_" }
    Pop-Location
    W (Wait-Kafka)
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

if ($E.Post -eq 'B-16') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    Run '관측 구성 기동 (develop + observe, 규칙은 game 만)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b16.sql') } | Out-Null
    Push-Location $Kit; Run '데이터 생성 (gen_b16.py)' { python gen_b16.py } | Out-Null; Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_b16.sql') } | Out-Null
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    Start-AppWith '--rpgym.reconcile.initial-delay=5000 --rpgym.reconcile.interval=15000'
    $dlt0 = DltEnd
    W "DLT 끝 오프셋 합 (시작): $dlt0"
    Snap '시작'
    $pDone = "SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%' AND substr(quest_id::text, 25)::bigint BETWEEN 1 AND 150 AND status = 'COMPLETED';"
    $kDone = "SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%' AND substr(quest_id::text, 25)::bigint BETWEEN 201 AND 350 AND status = 'COMPLETED';"
    $kPend = "SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND status <> 'PUBLISHED';"

    W ""; W "## P) PostgreSQL 60초 정지 (4초마다 10건씩 15묶음을 흘리는 중에, 10초 뒤 DB 정지 -> 60초 뒤 재기동)"
    $t0 = Get-Date; $pgStop = $null; $pgStart = $null
    foreach ($c in 1..15) {
        FillProduce ('p_{0:D2}' -f $c) $map 'health.events'
        $el = ((Get-Date) - $t0).TotalSeconds
        if (-not $pgStop -and $el -ge 10) {
            & docker stop rp-gym-postgres-game 2>&1 | Out-Null; $pgStop = Get-Date
            W "  $(Get-Date -Format 'HH:mm:ss') >>> PostgreSQL 정지"
        }
        if ($pgStop -and -not $pgStart -and ((Get-Date) - $pgStop).TotalSeconds -ge 60) {
            & docker start rp-gym-postgres-game 2>&1 | Out-Null; $pgStart = Get-Date
            W "  $(Get-Date -Format 'HH:mm:ss') >>> PostgreSQL 재기동 (정지 $([int]($pgStart - $pgStop).TotalSeconds)초)"
        }
        # DB 가 꺼진 동안은 앱 지표 수집(Outbox 대기 게이지가 DB 를 조회)이 커넥션 대기로 늘어지므로 지표를 읽지 않는다
        if ($pgStop -and -not $pgStart) { W "  $(Get-Date -Format 'HH:mm:ss') [묶음 $c] 발행함 (DB 꺼짐)" } else { Snap "묶음 $c" }
        $wait = 4 * $c - ((Get-Date) - $t0).TotalSeconds
        if ($wait -gt 0) { Start-Sleep -Milliseconds ([int]($wait * 1000)) }
    }
    if (-not $pgStart) {
        $rest = 60 - ((Get-Date) - $pgStop).TotalSeconds
        if ($rest -gt 0) { Start-Sleep -Milliseconds ([int]($rest * 1000)) }
        & docker start rp-gym-postgres-game 2>&1 | Out-Null; $pgStart = Get-Date
        W "  $(Get-Date -Format 'HH:mm:ss') >>> PostgreSQL 재기동 (정지 $([int]($pgStart - $pgStop).TotalSeconds)초)"
    }
    $tw = Get-Date
    while ($true) {
        Start-Sleep -Seconds 10
        $done = CountSql $pDone
        Snap "복구 대기 · 완료 $done/150"
        if ($done -ge 150) { W "  P 전부 처리: 재기동 후 $([int]((Get-Date) - $pgStart).TotalSeconds)초"; break }
        if (((Get-Date) - $tw).TotalSeconds -gt 300) { W "  !!! 5분이 지나도 완료 $done/150"; break }
    }

    W ""; W "## K) Kafka 90초 정지 (100건을 넣고 2초 뒤 Kafka 정지 -> 90초 뒤 재기동 -> 남은 50건)"
    FillProduce 'k_pre' $map 'health.events'
    Start-Sleep -Seconds 2
    & docker stop rp-gym-kafka 2>&1 | Out-Null; $kStop = Get-Date
    W "  $(Get-Date -Format 'HH:mm:ss') >>> Kafka 정지 (그 순간 K 완료 $(CountSql $kDone)/150 · 발행 안 된 Outbox $(CountSql $kPend))"
    while (((Get-Date) - $kStop).TotalSeconds -lt 90) {
        Start-Sleep -Seconds 10
        Snap "Kafka 꺼짐 · K 완료 $(CountSql $kDone) · 미발행 Outbox $(CountSql $kPend)"
    }
    & docker start rp-gym-kafka 2>&1 | Out-Null; $kStart = Get-Date
    W "  $(Get-Date -Format 'HH:mm:ss') >>> Kafka 재기동 (정지 $([int]($kStart - $kStop).TotalSeconds)초) · $(Wait-Kafka)"
    FillProduce 'k_post' $map 'health.events'
    $tw = Get-Date
    while ($true) {
        Start-Sleep -Seconds 10
        $done = CountSql $kDone; $pend = CountSql $kPend
        Snap "복구 대기 · K 완료 $done/150 · 미발행 Outbox $pend"
        if ($done -ge 150 -and $pend -eq 0) { W "  K 전부 처리 · 전부 발행: 재기동 후 $([int]((Get-Date) - $kStart).TotalSeconds)초"; break }
        if (((Get-Date) - $tw).TotalSeconds -gt 300) { W "  !!! 5분이 지나도 K 완료 $done/150 · 미발행 $pend"; break }
    }

    W ""; W "대조 두 주기 대기 (40초)"; Start-Sleep -Seconds 40
    W "DLT 증가 (기대 0): $((DltEnd) - $dlt0)"
    Run 'DLT 적재 카운터 (기대 0)' { Metrics '^rpgym_kafka_consumer_dlt_total' } | Out-Null
    Run '대조 게이지 (기대: 전부 0 · 실패 카운터 sql 은 DB 정지 동안 올라갔을 수 있음)' { Metrics '^rpgym_reconcile_' } | Out-Null
    Run '규칙 상태 (기대: 짧은 장애라 firing 없음)' { GameAlertStates } | Out-Null
    W "앱 로그: DB 연결 실패 흔적 $(LogCount 'Connection to .* refused|PSQLException|CannotCreateTransaction|JDBCConnectionException') 줄 · Kafka 연결 끊김 흔적 $(LogCount 'Connection to node .* could not be established|Disconnecting from node|NetworkException') 줄"
    Run '검증 (03_verify_b16.sql)' { Psql-File (Join-Path $Kit '03_verify_b16.sql') } | Out-Null
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

if ($E.Post -eq 'B-13') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    # B-13 의 기본 단계는 1 · 3 · 4 (대조 멈춤 2 는 30분 기준이라 원할 때만 -Phases 2 또는 -Phases 1342)
    $ph = if ($PSBoundParameters.ContainsKey('Phases')) { $Phases } else { '134' }
    W "단계: $ph  (1 대조 불일치 · 3 랭킹 반영 실패 · 4 처리 지연 p95 · 2 대조 멈춤)"
    $slowFlag = Join-Path $OutDir 'b13-slow.flag'
    $pauseFlag = Join-Path $OutDir 'b13-reconcile-pause.flag'
    Remove-Item $slowFlag, $pauseFlag -ErrorAction SilentlyContinue
    Run '관측 구성 기동 (develop + observe, 규칙은 game 만)' { Start-Observe } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_b13.sql') } | Out-Null
    Push-Location $Kit; Run '데이터 생성 (gen_b13.py)' { python gen_b13.py } | Out-Null; Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_b13.sql') } | Out-Null
    $now = [DateTimeOffset]::Now
    $map = @{ '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW__' = $now.ToString('yyyy-MM-ddTHH:mm:sszzz') }
    $slf = $slowFlag -replace '\\', '/'; $paf = $pauseFlag -replace '\\', '/'
    # max-poll-records=1: 이벤트 하나를 70초씩 처리하므로, 한 번에 여러 건을 받으면 poll 간격(기본 5분)을 넘겨 그룹에서 쫓겨난다
    Start-AppWith "--rpgym.reconcile.initial-delay=5000 --rpgym.reconcile.interval=15000 --spring.kafka.consumer.max-poll-records=1 --rpgym.experiment.slow-user-prefix=b1300004- --rpgym.experiment.slow-flag=$slf --rpgym.experiment.slow-ms=70000 --rpgym.experiment.reconcile-pause-flag=$paf"
    Run '랭킹 재구축 (시드 유저까지 ZSET 을 맞춰 두려고)' { (Invoke-WebRequest -UseBasicParsing -Method Post "http://localhost:$AppPort/api/v1/internal/games/rankings/rebuild" -TimeoutSec 60).Content } | Out-Null
    W "기준 상태 대기 (70초)"; Start-Sleep -Seconds 70
    Run '시작 전 규칙 상태 (기대: 전부 inactive)' { GameAlertStates } | Out-Null
    Run '대조 게이지 (기대: 전부 0)' { PromQuery 'rpgym_reconcile_mismatch{job=~"game-.*"}' } | Out-Null
    $r1 = $r1r = $r3 = $r4 = $r4r = $r2 = $r2r = '(건너뜀)'

    if ($ph -match '1') {
        W ""; W "## 1) 대조 불일치 (규칙: 어긋난 수 > 0 이면 즉시. 대조 15초 주기 · 랭킹은 두 번 연속이라 30초)"
        W "주입: 유저 b1300001 지갑 +50 (원장은 그대로)  $(Get-Date -Format 'HH:mm:ss')"
        Sql "UPDATE game_service.wallets SET xp = xp + 50 WHERE user_id = 'b1300001-0000-4000-8000-000000000001';" | Out-Null
        $r1 = WaitRule 'Game Reconcile Mismatch' 'firing' 300 'max by (check) (rpgym_reconcile_mismatch{job=~"game-.*"}) > 0'
        Run '그 순간 대조 게이지 (기대: ledger_wallet 1 · ranking 1)' { PromQuery 'rpgym_reconcile_mismatch{job=~"game-.*"} > 0' } | Out-Null
        Sql "UPDATE game_service.wallets SET xp = xp - 50 WHERE user_id = 'b1300001-0000-4000-8000-000000000001';" | Out-Null
        W "해제: 지갑 되돌림  $(Get-Date -Format 'HH:mm:ss')"
        $r1r = WaitRule 'Game Reconcile Mismatch' 'inactive' 300 'max(rpgym_reconcile_mismatch{job=~"game-.*"})'
    }
    if ($ph -match '3') {
        W ""; W "## 3) 랭킹 반영 실패 (규칙: 10분 안에 실패 > 0 이면 즉시. resolved 는 10분 창이 지난 뒤)"
        Run 'Redis 정지' { docker stop rp-gym-redis } | Out-Null
        W "주입: Redis 정지 상태에서 퀘스트 완료 이벤트 1건  $(Get-Date -Format 'HH:mm:ss')"
        FillProduce '03_p3_ranking' $map 'health.events'
        $r3 = WaitRule 'Game Ranking Update Failed' 'firing' 300 'sum(rpgym_ranking_update_failed_total{job=~"game-.*"})'
        Run 'Redis 다시 켜기' { Start-Redis } | Out-Null
        # 다시 켜진 Redis 에는 방금 유저 점수가 없다. 대조가 두 번 연속 보기 전에 맞춰서, 대조 불일치 알림이 덩달아 울리지 않게 한다
        Run '랭킹 재구축 (복구 절차)' { (Invoke-WebRequest -UseBasicParsing -Method Post "http://localhost:$AppPort/api/v1/internal/games/rankings/rebuild" -TimeoutSec 60).Content } | Out-Null
    }
    if ($ph -match '4') {
        W ""; W "## 4) 처리 지연 p95 (규칙: 처리 지연 p95 > 60초가 10분 지속. 기대 firing 약 11~12분)"
        New-Item -ItemType File -Path $slowFlag -Force | Out-Null
        W "주입: slow 파일 생성(유저 b1300004 이벤트를 70초씩) + 이벤트 12건  $(Get-Date -Format 'HH:mm:ss')"
        FillProduce '04_p4_latency' $map 'health.events'
        $p95 = 'histogram_quantile(0.95, sum by (le) (rate(rpgym_game_consumer_pipeline_latency_seconds_bucket{job=~"game-.*"}[5m])))'
        $r4 = WaitRule 'Game Pipeline Latency High' 'firing' 1200 $p95
        Remove-Item $slowFlag -ErrorAction SilentlyContinue
        W "해제: slow 파일 삭제  $(Get-Date -Format 'HH:mm:ss')  (p95 는 5분 창이라 바로 내려가지 않는다)"
        $r4r = WaitRule 'Game Pipeline Latency High' 'inactive' 900 $p95
        W "[B-13] slowed 로그 줄 수: $(@(Select-String -Path $script:CurLog -Pattern '\[B-13\] slowed' -ErrorAction SilentlyContinue).Count)"
    }
    if ($ph -match '2') {
        W ""; W "## 2) 대조 멈춤 (규칙: 마지막 대조 성공이 30분 넘게 지나면. 기대 firing 약 31분)"
        New-Item -ItemType File -Path $pauseFlag -Force | Out-Null
        W "주입: pause 파일 생성(대조를 건너뜀)  $(Get-Date -Format 'HH:mm:ss')"
        $r2 = WaitRule 'Game Reconcile Not Running' 'firing' 2400 'time() - max(rpgym_reconcile_last_success_timestamp{job=~"game-.*"})'
        Remove-Item $pauseFlag -ErrorAction SilentlyContinue
        W "해제: pause 파일 삭제  $(Get-Date -Format 'HH:mm:ss')"
        $r2r = WaitRule 'Game Reconcile Not Running' 'inactive' 300 'time() - max(rpgym_reconcile_last_success_timestamp{job=~"game-.*"})'
    }

    W ""; W "### 요약 (주입 -> Grafana firing 까지 초. Slack 은 firing 뒤 약 10초)"
    W "대조 불일치: $r1 초 (되돌린 뒤 inactive $r1r 초) / 랭킹 반영 실패: $r3 초 / 처리 지연 p95: $r4 초 (해제 뒤 inactive $r4r 초) / 대조 멈춤: $r2 초 (해제 뒤 inactive $r2r 초)"
    Run '끝 규칙 상태' { GameAlertStates } | Out-Null
    W ""; W "### 앱 종료"
    Stop-App
    Remove-Item $slowFlag, $pauseFlag -ErrorAction SilentlyContinue
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

if ($E.Post -eq 'B-0') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    Run '관측 구성 기동 (develop + observe)' { Start-Observe } | Out-Null
    Run 'Grafana 연락처 (기대: slack-monitoring-alert)' {
        try { (Invoke-RestMethod -TimeoutSec 10 -Headers $GrafanaAuth 'http://localhost:3000/api/v1/provisioning/contact-points') | ForEach-Object { "$($_.name) type=$($_.type)" } }
        catch { "연락처 조회 실패: $($_.Exception.Message)" }
    } | Out-Null
    Start-AppWith '--rpgym.reconcile.initial-delay=5000 --rpgym.reconcile.interval=15000'
    W "수집 · 평가 대기 (60초: Prometheus 15초 주기, 알림 규칙 평가 1분 주기)"; Start-Sleep -Seconds 60
    Run 'Prometheus: game-blue 수집 (기대: up => 1)' { PromQuery 'up{job=~"game-.*"}' } | Out-Null
    Run 'Prometheus: 대조 게이지가 game-blue 라벨로 (기대: 5줄, 값 0)' { PromQuery 'rpgym_reconcile_mismatch{job=~"game-.*"}' } | Out-Null
    Run 'Prometheus: lag 지표 이름 확인 (R-11 메모: 배포 후 확인 필요였던 것)' { PromQuery 'kafka_consumer_fetch_manager_records_lag_max{job=~"game-.*"}' } | Out-Null
    Run 'Grafana 알림 규칙 상태 (기대: game-consumer-alerts 그룹이 보이고 health=ok)' { GameAlertStates } | Out-Null
    W ""; W "### Loki: 앱 로그 (기대: 0 보다 큼)"
    W "{job=`"game-blue`"} 최근 5분 줄 수: $(LokiCount '{job="game-blue"}')"
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

if ($E.Post -eq 'A-6') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    Push-Location $Root
    Run '인프라 기동' { docker compose -f docker-compose-develop.yml up -d postgres-game kafka redis } | Out-Null
    Pop-Location
    Start-Sleep -Seconds 3
    Run 'DLT 토픽 (없으면 만든다)' { docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic health.events.DLT --partitions 3 --config retention.ms=1209600000 } | Out-Null
    # 지난 실험이 DLT 에 남긴 레코드를 되돌리지 않도록, 재처리 그룹의 위치를 지금 끝으로 옮긴다
    Run '재처리 그룹 위치를 DLT 끝으로' { docker exec rp-gym-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group game-service-dlt-replay --topic health.events.DLT --reset-offsets --to-latest --execute } | Out-Null
    Run '지난 데이터 정리' { Psql-File (Join-Path $Kit '99_cleanup_a6.sql') } | Out-Null
    Push-Location $Kit
    Run '데이터 생성 (gen_a6.py)' { python gen_a6.py } | Out-Null
    Pop-Location
    Run '시드' { Psql-File (Join-Path $Kit '01_seed_a6.sql') } | Out-Null

    # 같은 시각 값을 1단계와 DLT 직접 넣기에 함께 쓴다(D 는 1단계와 완전히 같은 레코드여야 한다)
    $now = [DateTimeOffset]::Now
    $fmt = 'yyyy-MM-ddTHH:mm:sszzz'
    $y = New-Object DateTimeOffset ($now.Date.AddDays(-1).AddHours(23)), $now.Offset
    $map = @{ '__YESTERDAY_2300__' = $y.ToString($fmt); '__YESTERDAY__' = $y.ToString('yyyy-MM-dd')
              '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW_MINUS_31__' = $now.AddMinutes(-31).ToString($fmt); '__NOW__' = $now.ToString($fmt) }
    W ""; W "이벤트 시각 치환: $(($map.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join '  ')"
    foreach ($t in @('02_phase1', '03_dlt_direct')) {
        $text = Get-Content (Join-Path $Kit "$t.template.txt") -Raw -Encoding UTF8
        foreach ($k in ($map.Keys | Sort-Object -Property Length -Descending)) { $text = $text.Replace($k, $map[$k]) }
        [System.IO.File]::WriteAllText((Join-Path $Kit "$t.txt"), $text, (New-Object System.Text.UTF8Encoding $false))
    }

    $fast = '--rpgym.reconcile.initial-delay=5000 --rpgym.reconcile.interval=15000'
    W ""; W "## 1단계: 버그가 있는 버전 (F 유저 이벤트에서 일부러 예외)"
    Start-AppWith "$fast --rpgym.experiment.fail-user-prefix=a6000001-0000-4000-8000-0000000001"
    $dlt0 = DltEnd
    W "DLT 끝 오프셋 합 (시작): $dlt0"
    Run '1단계 발행: F 10 + D 10 + 깨진 JSON 1 -> health.events' { Produce (Join-Path $Kit '02_phase1.txt') 'health.events' } | Out-Null
    $t1 = Get-Date
    while ($true) {
        Start-Sleep -Seconds 5
        $d = (DltEnd) - $dlt0
        $done = ((Sql "SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a6000002-0000-4000-8000-0000000002%' AND status = 'COMPLETED';") -match '^\s*\d+\s*$' | Select-Object -First 1)
        if ($d -ge 11 -and [int]$done -ge 10) { W "DLT 에 11건 쌓임 · D 완료 10 ($([int]((Get-Date) - $t1).TotalSeconds)초)"; break }
        if (((Get-Date) - $t1).TotalSeconds -gt 240) { W "!!! 4분이 지나도 DLT +$d · D 완료 $done. 여기서 진행한다"; break }
    }
    Run 'DLT 직접 넣기: D 10(같은 eventId) + Y 10(어제) + S 10(31분 지난 제안)' { Produce (Join-Path $Kit '03_dlt_direct.txt') 'health.events.DLT' } | Out-Null
    Start-Sleep -Seconds 2
    W "DLT 증가 합계 (기대 41): $((DltEnd) - $dlt0)"
    Run '1단계 끝 상태 (기대: F ACTIVE 10 · D COMPLETED 10 · Y ACTIVE 10)' { Sql "SELECT substr(quest_id::text, 25, 10) AS grp, status, count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a6000002%' GROUP BY 1, 2 ORDER BY 1, 2;" } | Out-Null
    Run 'DLT 적재 카운터 (기대: 11 = F 10 + 깨진 JSON 1)' { Metrics '^rpgym_kafka_consumer_dlt' } | Out-Null
    W "[A-6] injected bug 로그 줄 수 (F 10건 × 시도 4번 = 40 근처): $(@(Select-String -Path $script:CurLog -Pattern '\[A-6\] injected bug' -ErrorAction SilentlyContinue).Count)"
    W ""; W "### 앱 종료 (= 버그 수정 배포 준비)"
    Stop-App
    Start-Sleep -Seconds 5

    W ""; W "## 2단계: 버그를 고친 버전 (실험 설정 없이)"
    Start-AppWith $fast
    Run '재처리 dryRun (보기만)' { Replay $true } | Out-Null
    Run '재처리 1회차 (기대: replayed 41)' { Replay $false } | Out-Null
    W "처리 대기 20초"; Start-Sleep -Seconds 20
    foreach ($i in 2..4) {
        $exp = if ($i -lt 4) { "깨진 JSON 1건 replayed (replayCount $($i - 1) -> $i)" } else { '깨진 JSON 1건 skipped (replayCount 3 = 상한)' }
        Run "재처리 ${i}회차 (기대: $exp)" { Replay $false } | Out-Null
        W "처리 대기 10초"; Start-Sleep -Seconds 10
    }
    Run '재처리 카운터' { Metrics '^rpgym_kafka_dlt_replayed' } | Out-Null
    Run 'DLT 적재 카운터 (2단계 앱 기준: 깨진 JSON 이 돌아온 3)' { Metrics '^rpgym_kafka_consumer_dlt' } | Out-Null
    W "대조 두 주기 대기 (35초)"; Start-Sleep -Seconds 35
    Run '대조 게이지 (기대: 전부 0)' { Metrics '^rpgym_reconcile_' } | Out-Null
    Run '검증 (04_verify_a6.sql)' { Psql-File (Join-Path $Kit '04_verify_a6.sql') } | Out-Null
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# ---------------------------------------------------------------- A-10 은 이벤트를 쓰지 않는 별도 흐름
if ($E.Post -eq 'A-10') {
    if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
        Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
    }
    Push-Location $Root
    Run '인프라 기동' { docker compose -f docker-compose-develop.yml up -d postgres-game kafka redis } | Out-Null
    Pop-Location
    Start-Sleep -Seconds 3
    $a10Lua = "local n=0 for _,m in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do if string.sub(m,1,9)=='88888888-' then redis.call('ZREM',KEYS[1],m) n=n+1 end end return n"
    Run '시드 10만 명 (지난 회차 정리 포함)' { Psql-File (Join-Path $Kit '01_seed_a10.sql') } | Out-Null
    Run '실행 계획 (R-22 전후 쿼리)' { Psql-File (Join-Path $Kit '02_explain_a10.sql') } | Out-Null

    # 정합성 대조는 끈다: 10만 명 랭킹 검사가 재구축과 겹치면 측정이 흔들린다
    W ""; W "### 앱 기동 (로그: $AppLog, 앱 인자: --rpgym.reconcile.enabled=false)"
    $script:Gradle = Start-Process -FilePath (Join-Path $Root 'gradlew.bat') -WorkingDirectory $Root `
        -ArgumentList ":game-service:bootRun --args=`"--rpgym.reconcile.enabled=false`"" -RedirectStandardOutput $AppLog -RedirectStandardError $AppErr -NoNewWindow -PassThru
    $t0 = Get-Date
    while ($true) {
        Start-Sleep -Seconds 3
        if ($script:Gradle.HasExited) { Fail '앱이 기동 중에 꺼졌다 (아래 로그 참고)' }
        if ((LogCount 'partitions assigned') -ge $E.Threads) { W "기동 확인 ($([int]((Get-Date) - $t0).TotalSeconds)초)"; break }
        if (((Get-Date) - $t0).TotalSeconds -gt 240) { Fail '4분 안에 앱이 뜨지 않았다' }
    }
    Start-Sleep -Seconds 5

    # 탐침 대상: 실험 유저 1번. 첫 회차는 JIT 예열용(결과는 참고만)
    # '지금' 은 R-22 쿼리(테이블마다 인덱스로 읽고 합침), 'R-22 전 쿼리' 는 UNION 을 먼저 만드는 예전 쿼리
    $probe = '88888888-0000-4000-8000-000000000001'
    $configs = @(
        @{ Name = '예열 (참고만)';            Mode = 'keyset'; Batch = 1000 },
        @{ Name = '고치기 전 · 배치 1,000';   Mode = 'legacy'; Batch = 1000 },
        @{ Name = 'R-22 전 쿼리 · 배치 1,000'; Mode = 'keyset-union'; Batch = 1000 },
        @{ Name = '지금 · 배치 100';          Mode = 'keyset'; Batch = 100 },
        @{ Name = '지금 · 배치 1,000';        Mode = 'keyset'; Batch = 1000 },
        @{ Name = '지금 · 배치 10,000';       Mode = 'keyset'; Batch = 10000 }
    )
    $rows = @()
    foreach ($c in $configs) {
        W ""; W "### $($c.Name)  (mode=$($c.Mode), batch=$($c.Batch))"
        W "랭킹에서 실험 유저 지우기: $((& docker exec rp-gym-redis redis-cli EVAL $a10Lua 1 ranking:global 2>&1) -join ' ')"
        Start-Sleep -Seconds 2
        $wall = [Diagnostics.Stopwatch]::StartNew()
        try {
            $json = (Invoke-WebRequest -UseBasicParsing -Method Post -TimeoutSec 1800 `
                "http://localhost:$AppPort/api/v1/internal/games/rankings/rebuild-experiment?mode=$($c.Mode)&batch=$($c.Batch)&probeUser=$probe").Content
        } catch { $json = "호출 실패: $($_.Exception.Message)" }
        $wall.Stop()
        W $json
        W "호출 전체 시간(실행기 측정): $([int]$wall.Elapsed.TotalMilliseconds) ms"
        W "ZCARD ranking:global: $((& docker exec rp-gym-redis redis-cli ZCARD ranking:global 2>&1) -join ' ')"
        try {
            $r = $json | ConvertFrom-Json
            $rows += [pscustomobject]@{ 설정 = $c.Name; 대상 = $r.rebuilt; 재구축ms = $r.elapsedMs
                평소p99us = $r.probeIdle.p99us; 중p50us = $r.probeDuring.p50us; 중p99us = $r.probeDuring.p99us
                중p999us = $r.probeDuring.p999us; 중maxUs = $r.probeDuring.maxUs; 탐침수 = $r.probeDuring.count }
        } catch { }
    }
    W ""; W "### 요약"
    ($rows | Format-Table -AutoSize | Out-String -Width 250) -split "`r?`n" | Where-Object { $_ -ne '' } | ForEach-Object { W $_ }

    W ""; W "### 정리"
    W "랭킹에서 실험 유저 지우기: $((& docker exec rp-gym-redis redis-cli EVAL $a10Lua 1 ranking:global 2>&1) -join ' ')"
    Run 'DB 실험 데이터 지우기' { Psql-File (Join-Path $Kit '99_cleanup_a10.sql') } | Out-Null
    W ""; W "### 앱 종료"
    Stop-App
    W ""; W "=== 끝. 결과 파일: $Result"
    exit 0
}

# ---------------------------------------------------------------- 1. 사전 확인 · 인프라
if (Get-NetTCPConnection -LocalPort $AppPort -State Listen -ErrorAction SilentlyContinue) {
    Fail "포트 $AppPort 에 이미 앱이 떠 있다. 다른 터미널의 bootRun 을 Ctrl+C 로 끄고 다시 실행"
}
Push-Location $Root
Run '인프라 기동' { docker compose -f docker-compose-develop.yml up -d postgres-game kafka redis } | Out-Null
Pop-Location
Start-Sleep -Seconds 3
$desc = Run '토픽 파티션 수' { docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic health.events }
if (-not ($desc -match 'PartitionCount:\s*3')) { Fail 'health.events 파티션이 3개가 아니다 (위 출력 확인)' }

# ---------------------------------------------------------------- 2. 정리 · 데이터 생성
Run '지난 데이터 정리' { Psql-File (Join-Path $Here $E.Cleanup) } | Out-Null
Push-Location $Kit
$genArgs = @(); if ($E.GenArgs) { $genArgs = $E.GenArgs }
Run "데이터 생성 ($($E.Gen) $($genArgs -join ' '))" { python $E.Gen @genArgs } | Out-Null
Pop-Location
# A-11: 지난 회차가 재구축으로 같은 유저 · 같은 점수를 남겨 두면 Redis 복구 뒤에도 랭킹이 맞아 보인다(2026-10-11 재실행에서 발견). 랭킹 키를 비우고 시작한다
if ($E.Post -eq 'A-11') { Run '랭킹 키 비우기 (ranking:global)' { docker exec rp-gym-redis redis-cli DEL ranking:global } | Out-Null }
# A-4: 지난 회차가 남긴 실험 유저 점수(같은 유저 · 더 큰 점수)가 있으면 GT 가 옛 값 쓰기를 막은 것처럼 보인다. 실험 유저(33333333-)만 랭킹에서 지운다
if ($E.Post -eq 'A-4') {
    $lua = "local n=0 for _,m in ipairs(redis.call('ZRANGE',KEYS[1],0,-1)) do if string.sub(m,1,9)=='33333333-' then redis.call('ZREM',KEYS[1],m) n=n+1 end end return n"
    Run '랭킹에서 실험 유저만 지우기 (지운 수)' { docker exec rp-gym-redis redis-cli EVAL $lua 1 ranking:global } | Out-Null
}
if ($E.RedisDown) { Run 'Redis 정지' { docker stop rp-gym-redis } | Out-Null }

# ---------------------------------------------------------------- 3. 앱 기동
W ""; W "### 앱 기동 (로그: $AppLog)"
$argList = if ($E.Args) { ":game-service:bootRun --args=`"$($E.Args)`"" } else { ':game-service:bootRun' }
$script:Gradle = Start-Process -FilePath (Join-Path $Root 'gradlew.bat') -WorkingDirectory $Root `
    -ArgumentList $argList -RedirectStandardOutput $AppLog -RedirectStandardError $AppErr -NoNewWindow -PassThru
$t0 = Get-Date
while ($true) {
    Start-Sleep -Seconds 3
    if ($script:Gradle.HasExited) { Fail '앱이 기동 중에 꺼졌다 (아래 로그 참고)' }
    $assigned = LogCount 'partitions assigned'
    if ($assigned -ge $E.Threads) { W "partitions assigned $assigned 줄 확인 ($([int]((Get-Date) - $t0).TotalSeconds)초)"; break }
    if (((Get-Date) - $t0).TotalSeconds -gt 240) { Fail "4분 안에 partitions assigned 가 $($E.Threads)번 나오지 않았다" }
}
Start-Sleep -Seconds 3

# ---------------------------------------------------------------- 4. 시드 · 이벤트 · 발행
Run '시드' { Psql-File (Join-Path $Kit $E.Seed) } | Out-Null
$now = [DateTimeOffset]::Now
$fmt = 'yyyy-MM-ddTHH:mm:sszzz'
if ($E.Fill -eq 'R12') {
    $map = @{ '__MEASURED_AT__' = $now.ToString($fmt); '__DATE__' = $now.ToString('yyyy-MM-dd') }
} else {
    $y = New-Object DateTimeOffset ($now.Date.AddDays(-1).AddHours(23)), $now.Offset
    $map = @{ '__YESTERDAY_2300__' = $y.ToString($fmt); '__YESTERDAY__' = $y.ToString('yyyy-MM-dd')
              '__TODAY__' = $now.ToString('yyyy-MM-dd'); '__NOW_MINUS_31__' = $now.AddMinutes(-31).ToString($fmt)
              '__NOW_MINUS_29__' = $now.AddMinutes(-29).ToString($fmt); '__NOW__' = $now.ToString($fmt) }
}
$text = Get-Content (Join-Path $Kit $E.Template) -Raw -Encoding UTF8
foreach ($k in ($map.Keys | Sort-Object -Property Length -Descending)) { $text = $text.Replace($k, $map[$k]) }
[System.IO.File]::WriteAllText((Join-Path $Kit $E.Events), $text, (New-Object System.Text.UTF8Encoding $false))
W ""; W "이벤트 시각 치환: $(($map.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)=$($_.Value)" }) -join '  ')"
$before = Offsets
& docker cp (Join-Path $Kit $E.Events) 'rp-gym-kafka:/tmp/events.txt' 2>&1 | Out-Null
& docker exec rp-gym-kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic health.events --property parse.key=true --property key.separator='|' < /tmp/events.txt" 2>&1 | Out-Null
Start-Sleep -Seconds 2
$after = Offsets
W ""; W "### 파티션별 발행 건수"
foreach ($a in $after) {
    $p = ($a -split ':')[1]; $na = [long](($a -split ':')[2])
    $b = $before | Where-Object { ($_ -split ':')[1] -eq $p }
    $nb = if ($b) { [long](($b -split ':')[2]) } else { 0 }
    W "파티션 $p : +$($na - $nb)"
}

# ---------------------------------------------------------------- 5. 처리 끝날 때까지 (로그가 20초 조용하면 끝)
W ""; W "### 처리 대기"
$t1 = Get-Date; $last = -1; $quiet = 0
while ($true) {
    Start-Sleep -Seconds 5
    if ($script:Gradle.HasExited) { Fail '처리 중에 앱이 꺼졌다' }
    # 로그 크기가 아니라 '처리 완료' 줄 수로 본다. 대조 실패 · Eureka 재시도 로그는 처리와 상관없이 계속 늘어난다(A-11 첫 실행에서 발견)
    $len = LogCount 'HealthEventConsumer.*(result=|outcome=)'
    if ($len -eq $last) { $quiet += 5 } else { $quiet = 0; $last = $len }
    $el = ((Get-Date) - $t1).TotalSeconds
    if ($quiet -ge 20 -and $el -ge 30) { W "처리 끝 ($([int]$el)초)"; break }
    if ($el -gt $MaxWaitSec) { W "!!! ${MaxWaitSec}초가 지나도 로그가 계속 늘어난다. 여기서 끊는다"; break }
}

# ---------------------------------------------------------------- 6. 실험별 후처리 (앱이 떠 있는 동안)
switch ($E.Post) {
    'A-9' {
        W ""; W "### A-9 확인 (기대: 롤백 예외 0 · 레벨 행 400 · redis 실패 카운터 400)"
        W "UnexpectedRollbackException 로그 줄 수: $(LogCount 'UnexpectedRollbackException')"
        W "Redis 연결 실패 흔적 줄 수: $(LogCount 'RedisConnectionFailure|Unable to connect to Redis')"
        Run '레벨 행(characters) — 실험 유저' { Sql "SELECT count(*) AS characters_rows, min(level), max(level) FROM game_service.characters WHERE user_id::text LIKE '33333333-0000-4000-8000-%';" } | Out-Null
        Run '실패 카운터' { Metrics '^rpgym_ranking_update_failed' } | Out-Null
        Run 'Redis 다시 켜기' { Start-Redis } | Out-Null
    }
    'A-11' {
        W ""; W "### A-11-1 Redis 가 꺼진 동안 (기대: 실패 카운터 redis 400, (R-21 후) SQL 게이지 갱신 · last_success > 0 · rpgym_reconcile_failed_total{stage=ranking} > 0 · stage=sql 0)"
        Run '실패 카운터' { Metrics '^rpgym_ranking_update_failed' } | Out-Null
        Run '대조 게이지' { Metrics '^rpgym_reconcile_' } | Out-Null
        Run 'Redis 다시 켜기' { Start-Redis } | Out-Null
        W "대조 두 주기 대기 (40초)"; Start-Sleep -Seconds 40
        W ""; W "### A-11-2 Redis 복구 후, 재구축 전 (기대: ranking = 재구축 대상 수(지난번 525: 이번 400 + 이전 실험 잔여) · 나머지 0)"
        Run '대조 게이지' { Metrics '^rpgym_reconcile_mismatch' } | Out-Null
        Run '재구축 호출' { (Invoke-WebRequest -UseBasicParsing -Method Post "http://localhost:$AppPort/api/v1/internal/games/rankings/rebuild" -TimeoutSec 60).Content } | Out-Null
        W "대조 두 주기 대기 (40초)"; Start-Sleep -Seconds 40
        W ""; W "### A-11-3 재구축 후 (기대: 전부 0)"
        Run '대조 게이지' { Metrics '^rpgym_reconcile_' } | Out-Null
    }
    'A-3' {
        W ""; W "### A-3 지갑 (기대: 개인 완료 400 · 원장 QUEST 400건 4000 + PARTY_QUEST 400건 12000. 지갑 = 원장 합이면 0 / A-3 은 어긋난 유저가 나와야 재현, A-3c 는 0)"
        Run '개인 퀘스트 완료' { Sql "SELECT status, count(*) FROM game_service.quests WHERE quest_id::text LIKE '66666666-0000-4000-8000-%' GROUP BY status;" } | Out-Null
        Run '원장 (출처별)' { Sql "SELECT source_type, count(*) AS rows, sum(amount) AS xp FROM game_service.xp_ledgers WHERE user_id::text LIKE '33333333-0000-4000-8000-%' GROUP BY source_type ORDER BY 1;" } | Out-Null
        Run '지갑 <> 원장 합 (lost update)' { Sql "WITH l AS (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE '33333333-0000-4000-8000-%' GROUP BY user_id) SELECT count(*) AS mismatched_users, coalesce(sum(l.total - w.xp), 0) AS lost_xp, count(*) FILTER (WHERE l.total - w.xp = 10) AS lost_personal_10, count(*) FILTER (WHERE l.total - w.xp = 30) AS lost_party_30, count(*) FILTER (WHERE l.total - w.xp NOT IN (10, 30)) AS other FROM game_service.wallets w JOIN l USING (user_id) WHERE w.xp <> l.total;" } | Out-Null
        Run '지갑 합계' { Sql "SELECT count(*) AS wallets, sum(xp) AS wallet_xp FROM game_service.wallets WHERE user_id::text LIKE '33333333-0000-4000-8000-%';" } | Out-Null
        W "[A-3] rmw 로그 줄 수 (A-3 은 800, A-3c 는 0): $(LogCount '\[A-3\] rmw')"
    }
    'A-4' {
        W ""; W "### A-4 랭킹 (지갑을 40 미만으로 읽은 리스너만 2초 멈춤 = 옛 값을 읽은 쪽의 GC 정지를 인위적으로 주입. 기대: 지갑 = 원장. 대조 ranking = 랭킹 점수가 지갑과 다른 유저 수(두 주기 연속). A-4 는 0 보다 커야 재현, A-4c 는 0)"
        Run '지갑 <> 원장 합 (A-4 는 지갑을 원자 갱신하므로 0 이어야 한다)' { Sql "WITH l AS (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE '33333333-0000-4000-8000-%' GROUP BY user_id) SELECT count(*) AS mismatched_users FROM game_service.wallets w JOIN l USING (user_id) WHERE w.xp <> l.total;" } | Out-Null
        W "대조 두 주기 대기 (40초)"; Start-Sleep -Seconds 40
        Run '대조 게이지' { Metrics '^rpgym_reconcile_' } | Out-Null
        W "대조 ERROR 로그 (어긋난 id 샘플): $(LogCount 'check=ranking count=\d+')"
        W "[A-4] zadd 로그 줄 수 (두 모드 모두 800): $(LogCount '\[A-4\] zadd')"
        Run '레벨 행(characters) — 실험 유저' { Sql "SELECT count(*) AS characters_rows, min(level), max(level) FROM game_service.characters WHERE user_id::text LIKE '33333333-0000-4000-8000-%';" } | Out-Null
    }
    'A-8' {
        W ""; W "### A-8 (기대: 완료 10 · 충돌 로그 50줄 · DLT 카운터 0)"
        $t8 = Get-Date
        while ($true) {
            $done = ((Sql "SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%' AND status = 'COMPLETED';") -match '^\s*\d+\s*$' | Select-Object -First 1)
            if ([int]$done -ge 10) { W "완료 10 확인 (추가 대기 $([int]((Get-Date) - $t8).TotalSeconds)초)"; break }
            if (((Get-Date) - $t8).TotalSeconds -gt 180) { W "!!! 3분 더 기다려도 완료 $done"; break }
            Start-Sleep -Seconds 5
        }
        W "[A-8] 충돌 주입 로그 줄 수: $(LogCount '\[A-8\] conflict injected')"
        W "낙관적 락 예외 흔적 줄 수: $(LogCount 'ObjectOptimisticLockingFailure|OptimisticLock|StaleObjectState')"
        Run 'DLT 적재 카운터 (기대 0)' { Metrics '^rpgym_kafka_consumer_dlt' } | Out-Null
        Run '퀘스트별 첫 처리 ~ 완료 (충돌 5번 × 재시도 2초 = 10초 안팎)' { Sql "SELECT substr(quest_id::text, 25) AS quest, version, to_char(updated_at, 'HH24:MI:SS') AS completed_at FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%' ORDER BY 1;" } | Out-Null
    }
    'A-7' {
        W ""; W "### A-7-1 손대기 전 (기대: 전부 0)"
        Start-Sleep -Seconds 20
        Run '대조 게이지' { Metrics '^rpgym_reconcile_mismatch' } | Out-Null
        Run '일부러 어긋내기: 파티 1 의 원장 1행 삭제 · 파티 2 멤버 1명 지갑 +100' {
            Sql "DELETE FROM game_service.xp_ledgers WHERE ledger_id = (SELECT ledger_id FROM game_service.xp_ledgers WHERE source_type = 'PARTY_QUEST' AND source_id = '22222222-0000-4000-8000-000000000001' ORDER BY user_id LIMIT 1);"
            Sql "UPDATE game_service.wallets SET xp = xp + 100 WHERE user_id = (SELECT m.user_id FROM game_service.party_quest_members m JOIN game_service.wallets w ON w.user_id = m.user_id WHERE m.party_quest_id = '22222222-0000-4000-8000-000000000002' ORDER BY m.user_id LIMIT 1);"
        } | Out-Null
        W "대조 두 주기 대기 (40초)"; Start-Sleep -Seconds 40
        W ""; W "### A-7-2 어긋낸 뒤 (기대: ledger_wallet 2 · party_reward 1 · ranking 1 · 나머지 0)"
        Run '대조 게이지' { Metrics '^rpgym_reconcile_' } | Out-Null
        W "대조 ERROR 로그 줄 수(어긋난 id 샘플): $(LogCount 'check=\w+ count=\d+ sample=')"
    }
}

# ---------------------------------------------------------------- 7. 검증 · 종료 · 분석
Run "검증 ($($E.Verify))" { Psql-File (Join-Path $Kit $E.Verify) } | Out-Null
if ($E.Post -eq 'A-7') { W "(A-7 은 일부러 어긋냈으므로 검증 3번 · 6번에 행이 나오는 것이 정상)" }
W ""; W "### 앱 종료"
Stop-App
if ($E.RedisDown) { & docker start rp-gym-redis 2>&1 | Out-Null }
Start-Sleep -Seconds 3
foreach ($s in $E.Analyze) {
    Run "분석 ($s)" { python (Join-Path $Kit $s) $AppLog } | Out-Null
}
W ""; W "=== 끝. 결과 파일: $Result"
