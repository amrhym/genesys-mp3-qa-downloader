# Genesys Cloud → MP3 QA downloader (Solution B)

Pulls finished voice-call recordings from Genesys Cloud as **MP3** and lands
them (plus a metadata sidecar) in a folder / bucket for the QA automation to
consume. The Opus→MP3 conversion happens **inside Genesys Cloud** via the
recording API's `formatId=MP3` server-side transcoding — no FFmpeg, no
third-party converter, no new data processor.

```
Genesys Cloud (recordings, Opus)
      │  GET /api/v2/conversations/{id}/recordings?formatId=MP3
      │  (202 while Genesys transcodes → 200 with signed MP3 URLs)
      ▼
downloader.py  (cron / Cloud Scheduler + Cloud Run Job)
      ▼
OUTPUT_DIR/YYYY-MM-DD/{conversationId}/{recordingId}_ch{N}.mp3 + metadata.json
      ▼
QA automation ……… lifecycle policy deletes after retention period
```

## Genesys Cloud prerequisites

1. **OAuth client** (Admin → Integrations → OAuth → Add client):
   - Grant type: *Client Credentials*
   - Assigned role needs only: `recording:recording:view` and
     `analytics:conversationDetail:view`
   - Do **not** grant `recording:recording:viewSensitiveData` — redacted
     recordings stay redacted, which is what you want for QA.
2. **Recording policies** (Admin → Quality → Policies) must already record
   the calls in scope. Nothing else is needed on the Genesys side.
3. Region hosts: Ireland `api/login.mypurecloud.ie`, Frankfurt
   `api/login.mypurecloud.de` (full list: AWS regions for Genesys Cloud).

## Run it

```bash
export GC_CLIENT_ID=...           # from a secret store, never hardcoded
export GC_CLIENT_SECRET=...
export GC_API_HOST=api.mypurecloud.ie
export GC_LOGIN_HOST=login.mypurecloud.ie
export OUTPUT_DIR=/data/recordings
export STATE_FILE=/data/state.json
python3 downloader.py             # run every 5-15 min from cron/scheduler
```

| Env var | Default | Meaning |
|---|---|---|
| `LOOKBACK_HOURS` | 24 | Scan window (capped at 168 — analytics API limit) |
| `SAFETY_LAG_MINUTES` | 15 | Skip calls that ended more recently than this |
| `MAX_CONVERSATIONS` | 0 | Per-run cap, 0 = unlimited (use for testing) |

Behavioral guarantees (all live-tested):

- **Idempotent** — processed recording IDs are tracked in `STATE_FILE`;
  reruns skip completed work. Corrupt state files are quarantined to
  `*.bad`, never a crash loop.
- **Single instance** — an flock on `STATE_FILE` prevents overlapping cron
  runs; the second instance exits 0 immediately.
- **No premature closure** — a conversation with no recordings is only
  finalized 2 h after it ended (ingestion can lag the hangup).
- **Verified downloads** — files stream to `.part`, are length-checked
  against Content-Length, then atomically renamed. No silent truncation.
- **Rate-limit safe** — self-throttles below the 120 recording-ops/min
  platform limit and honors 429 Retry-After (bounded retries).
- **Poison-pill protection** — a conversation failing 10 runs in a row is
  parked under `given_up` in the state file and logged loudly.
- Recordings are written `0600` (umask) since they contain personal data.

## Output

Per conversation: one MP3 per channel (`_ch0` = one party, `_ch1` = the
other — ideal speaker separation for AI QA) and a `metadata.json` with
conversation start/end, participants and the file list.

## Production deployment on GCP (EMA side)

- Container: `python:3.12-slim` + this script (no dependencies).
- **Cloud Run Job** triggered by **Cloud Scheduler** every 5–15 min.
- Credentials in **Secret Manager**, injected as env vars.
- `OUTPUT_DIR` on a mounted **GCS bucket** (Cloud Storage FUSE) in an EU
  region, or adapt `download_file` to write via the GCS client.
- `STATE_FILE` on a small persistent volume or GCS.
- **Lifecycle rule** on the bucket deletes objects after the agreed
  retention period.
- Alert on: non-zero exit, `given_up` growth, zero downloads during
  business hours.

## Capacity

Each conversation costs ~2–5 API calls (analytics amortized + recordings
call + 202 retries). At the 120/min recording-endpoint limit the practical
ceiling is roughly **>50,000 calls/day** with one token — far above typical
QA volumes. API calls count toward the org's monthly fair-use pool
(~110k requests per CX1/CX2 named-user license).

## Compliance notes (Germany/EU case)

- Audio path: Genesys Cloud (EU region) → EMA GCP (EU region) over TLS.
  Transcoding occurs inside Genesys Cloud, an existing approved processor.
- No third party ever receives the audio; signed download URLs are
  short-lived and fetched immediately.
- Retention enforced by bucket lifecycle + state pruning; Genesys-side
  retention is governed separately by recording policies.
- Rotate the OAuth client secret on a schedule; least-privilege role only.
