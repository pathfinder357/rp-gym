# Load test step 2 : sample the PENDING backlog once per second until it drains.
# Run this in a second window right after 01_seed.sql.
#
#   powershell -ExecutionPolicy Bypass -File .\02_watch.ps1
#
# Output columns:  elapsed_sec, pending, published, published_delta (= throughput/sec)

$container = "rp-gym-postgres-game"
$db        = "game_db"
$prefix    = "aaaaaaaa-0000-4000-8000-%"

$query = @"
SELECT count(*) FILTER (WHERE status='PENDING')   AS pending,
       count(*) FILTER (WHERE status='PUBLISHED') AS published
  FROM game_service.outbox_events
 WHERE outbox_id::text LIKE '$prefix';
"@

$start = Get-Date
$prevPublished = 0
$zeroStreak = 0

Write-Host "elapsed_sec,pending,published,published_delta"

while ($true) {
    $raw = docker exec -i $container psql -U postgres -d $db -t -A -F "," -c $query
    $parts = ($raw | Select-Object -First 1).Trim().Split(",")
    $pending   = [int]$parts[0]
    $published = [int]$parts[1]
    $delta     = $published - $prevPublished
    $prevPublished = $published

    $elapsed = [math]::Round(((Get-Date) - $start).TotalSeconds, 1)
    Write-Host "$elapsed,$pending,$published,$delta"

    if ($pending -eq 0) {
        $zeroStreak++
        if ($zeroStreak -ge 3) {
            Write-Host ""
            Write-Host "drained. total elapsed = $elapsed sec, published = $published"
            Write-Host "average throughput = $([math]::Round($published / $elapsed, 1)) events/sec"
            break
        }
    } else {
        $zeroStreak = 0
    }

    Start-Sleep -Seconds 1
}
