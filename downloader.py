#!/usr/bin/env python3
"""Genesys Cloud -> MP3 recording downloader (Solution B reference implementation).

Flow per run (designed to be executed repeatedly by cron / Cloud Scheduler):
  1. Authenticate with OAuth client credentials.
  2. Query analytics for voice conversations in the lookback window.
  3. For each finished, not-yet-processed conversation, ask Genesys to
     transcode its recordings to MP3 (server-side) and download the files.
  4. Write MP3s + a metadata sidecar JSON per conversation to OUTPUT_DIR.
  5. Record processed recording IDs in STATE_FILE so reruns are idempotent.

Zero third-party dependencies (Python 3.9+ standard library only).

Required environment variables:
  GC_CLIENT_ID, GC_CLIENT_SECRET

Optional environment variables (defaults in parentheses):
  GC_API_HOST       (api.mypurecloud.ie)    e.g. api.mypurecloud.de for Frankfurt
  GC_LOGIN_HOST     (login.mypurecloud.ie)
  OUTPUT_DIR        (./recordings)
  STATE_FILE        (./state.json)
  LOOKBACK_HOURS    (24)    how far back to scan; capped at 168 (analytics limit)
  SAFETY_LAG_MINUTES (15)   skip conversations that ended more recently than this
  MAX_CONVERSATIONS (0)     cap per run, 0 = unlimited (useful for testing)
"""

import base64
import fcntl
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("gc-mp3")

API_HOST = os.environ.get("GC_API_HOST", "api.mypurecloud.ie")
LOGIN_HOST = os.environ.get("GC_LOGIN_HOST", "login.mypurecloud.ie")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "./recordings"))
STATE_FILE = Path(os.environ.get("STATE_FILE", "./state.json"))
LOOKBACK_HOURS = float(os.environ.get("LOOKBACK_HOURS", "24"))
SAFETY_LAG_MINUTES = float(os.environ.get("SAFETY_LAG_MINUTES", "15"))
MAX_CONVERSATIONS = int(os.environ.get("MAX_CONVERSATIONS", "0"))

# Platform limit is 120 recording operations per token per minute; we
# self-throttle below that so bursts never trigger HTTP 429.
SECONDS_BETWEEN_RECORDING_CALLS = 0.6
# How long to keep retrying while Genesys transcodes (HTTP 202) one recording.
TRANSCODE_RETRY_SCHEDULE = [5, 5, 10, 15, 30, 30, 60]
# Analytics conversation-details queries reject intervals longer than 7 days.
MAX_LOOKBACK_HOURS = 168
# An empty recordings list is only final once the call has been over this long
# (recording ingestion can lag the end of the call by minutes).
CONFIRM_EMPTY_AFTER_MINUTES = 120
# Give up on a conversation after this many failed runs (logged loudly,
# recorded under state['given_up'] for operator follow-up).
MAX_CONVERSATION_ATTEMPTS = 10
MAX_429_RETRIES = 6
MAX_NETWORK_RETRIES = 3
DOWNLOAD_DEADLINE_SECONDS = 900

ID_RE = re.compile(r"^[0-9a-fA-F][0-9a-fA-F-]{7,63}$")
CHANNEL_RE = re.compile(r"^[0-9S]$")


class GenesysClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self._last_recording_call = 0.0

    def authenticate(self):
        body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
        basic = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()
        req = urllib.request.Request(
            f"https://{LOGIN_HOST}/oauth/token",
            data=body,
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.load(resp)
        self.token = data["access_token"]
        log.info("authenticated, token valid for %ss", data.get("expires_in"))

    def request(self, method, path, body=None):
        """API request with bounded 429/network retries and one-shot re-auth.

        Returns (status, parsed_json_or_None). Raises after retry budgets.
        """
        url = f"https://{API_HOST}{path}"
        data = json.dumps(body).encode() if body is not None else None
        rate_limited = network_errors = 0
        reauthenticated = False
        while True:
            req = urllib.request.Request(
                url,
                data=data,
                method=method,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    return resp.status, (json.loads(raw) if raw else None)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    rate_limited += 1
                    if rate_limited > MAX_429_RETRIES:
                        raise RuntimeError(f"rate limited {rate_limited}x on {path}")
                    wait = _retry_after_seconds(e.headers.get("Retry-After"))
                    log.warning("rate limited, sleeping %ss", wait)
                    time.sleep(wait)
                    continue
                if e.code == 401 and not reauthenticated:
                    reauthenticated = True
                    log.info("token rejected, re-authenticating once")
                    self.authenticate()
                    continue
                return e.code, _safe_json(e)
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                network_errors += 1
                if network_errors > MAX_NETWORK_RETRIES:
                    raise
                log.warning("network error (%s), retry %d", e, network_errors)
                time.sleep(5 * network_errors)

    def recording_request(self, path):
        """request() plus client-side throttling for the recording endpoints."""
        elapsed = time.monotonic() - self._last_recording_call
        if elapsed < SECONDS_BETWEEN_RECORDING_CALLS:
            time.sleep(SECONDS_BETWEEN_RECORDING_CALLS - elapsed)
        self._last_recording_call = time.monotonic()
        return self.request("GET", path)


def _retry_after_seconds(header_value):
    try:
        return max(1, min(int(header_value), 300))
    except (TypeError, ValueError):
        return 60


def _safe_json(http_error):
    try:
        return json.load(http_error)
    except Exception:
        return None


def _parse_ts(iso_string):
    return datetime.fromisoformat(iso_string.replace("Z", "+00:00"))


def load_state():
    state = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            bad = STATE_FILE.with_suffix(".bad")
            STATE_FILE.replace(bad)
            log.error("state file unreadable, quarantined to %s", bad)
            state = {}
    for key in ("recordings", "conversations_done", "attempts",
                "skipped_archived", "given_up"):
        state.setdefault(key, {})
    return state


def save_state(state):
    # Prune with a horizon decoupled from the current LOOKBACK_HOURS so a
    # one-off run with a small lookback cannot wipe still-relevant markers.
    horizon = max(timedelta(hours=LOOKBACK_HOURS * 2), timedelta(days=14))
    cutoff = (datetime.now(timezone.utc) - horizon).isoformat()
    for key in ("recordings", "conversations_done", "skipped_archived", "given_up"):
        state[key] = {k: v for k, v in state[key].items() if str(v) >= cutoff}
    state["attempts"] = {
        k: v for k, v in state["attempts"].items()
        if k not in state["conversations_done"] and k not in state["given_up"]
    }
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(STATE_FILE)


def find_voice_conversations(client, start, end):
    """Page through analytics for voice conversations in [start, end]."""
    conversations = []
    page = 1
    while True:
        status, data = client.request(
            "POST",
            "/api/v2/analytics/conversations/details/query",
            {
                "interval": f"{start:%Y-%m-%dT%H:%M:%S.000Z}/{end:%Y-%m-%dT%H:%M:%S.000Z}",
                "order": "asc",
                "orderBy": "conversationStart",
                "segmentFilters": [
                    {
                        "type": "or",
                        "predicates": [{"dimension": "mediaType", "value": "voice"}],
                    }
                ],
                "paging": {"pageSize": 100, "pageNumber": page},
            },
        )
        if status != 200:
            raise RuntimeError(f"analytics query failed: HTTP {status} {data}")
        batch = data.get("conversations", [])
        conversations.extend(batch)
        if len(batch) < 100:
            return conversations
        page += 1


def fetch_recordings_as_mp3(client, conversation_id):
    """Ask Genesys to transcode to MP3; poll while it does (HTTP 202).

    Returns the recordings list on success, [] when the conversation has no
    recordings (yet), or None when transcoding did not finish in time.
    """
    path = f"/api/v2/conversations/{conversation_id}/recordings?formatId=MP3&maxWaitMs=5000"
    for wait in TRANSCODE_RETRY_SCHEDULE + [0]:
        status, data = client.recording_request(path)
        if status == 200:
            return data or []
        if status == 404:
            return []
        if status != 202:
            raise RuntimeError(f"recordings call failed: HTTP {status} {data}")
        if wait:
            log.info("  %s still transcoding, retrying in %ss", conversation_id, wait)
            time.sleep(wait)
    return None


def download_file(url, dest):
    """Stream to a .part file, verify length, then atomically rename."""
    part = dest.with_suffix(dest.suffix + ".part")
    deadline = time.monotonic() + DOWNLOAD_DEADLINE_SECONDS
    written = 0
    req = urllib.request.Request(url)  # signed URL: no auth header needed
    with urllib.request.urlopen(req, timeout=120) as resp, open(part, "wb") as f:
        expected = resp.headers.get("Content-Length")
        while chunk := resp.read(1 << 16):
            f.write(chunk)
            written += len(chunk)
            if time.monotonic() > deadline:
                raise RuntimeError(f"download exceeded {DOWNLOAD_DEADLINE_SECONDS}s")
    if written == 0 or (expected and written != int(expected)):
        part.unlink(missing_ok=True)
        raise RuntimeError(f"truncated download: {written}/{expected or '?'} bytes")
    part.replace(dest)
    return written


def write_sidecar(conv_dir, conv):
    """(Re)build metadata.json from what is actually on disk, atomically."""
    sidecar = {
        "conversationId": conv["conversationId"],
        "conversationStart": conv.get("conversationStart"),
        "conversationEnd": conv.get("conversationEnd"),
        "divisionIds": conv.get("divisionIds"),
        "participants": conv.get("participants"),
        "files": sorted(p.name for p in conv_dir.glob("*.mp3")),
        "downloadedAt": datetime.now(timezone.utc).isoformat(),
    }
    tmp = conv_dir / "metadata.json.tmp"
    tmp.write_text(json.dumps(sidecar, indent=1))
    tmp.replace(conv_dir / "metadata.json")


def process_conversation(client, conv, state, conversation_age_minutes):
    """Returns True when the conversation is fully handled (skip next runs)."""
    conv_id = conv["conversationId"]
    if not ID_RE.match(conv_id):
        raise RuntimeError(f"suspicious conversationId {conv_id!r}")
    recordings = fetch_recordings_as_mp3(client, conv_id)
    if recordings is None:
        log.warning("  %s: transcoding timeout, will retry next run", conv_id)
        return False
    audio = [r for r in recordings if r.get("media") == "audio"]
    if not audio:
        # Recording ingestion can lag the end of the call; only treat an
        # empty result as final once the call has been over long enough.
        if conversation_age_minutes < CONFIRM_EMPTY_AFTER_MINUTES:
            log.info("  %s: no recordings yet (call ended %.0f min ago), re-check later",
                     conv_id, conversation_age_minutes)
            return False
        log.info("  %s: no audio recordings (confirmed)", conv_id)
        return True

    day = conv["conversationStart"][:10]
    conv_dir = OUTPUT_DIR / day / conv_id
    done = True
    downloaded_any = False
    for rec in audio:
        rec_id = rec["id"]
        if not ID_RE.match(rec_id):
            raise RuntimeError(f"suspicious recordingId {rec_id!r}")
        if rec_id in state["recordings"]:
            continue
        if rec.get("fileState") == "ARCHIVED":
            log.warning("  %s: recording %s is archived, skipping", conv_id, rec_id)
            state["skipped_archived"][rec_id] = datetime.now(timezone.utc).isoformat()
            continue
        media_uris = rec.get("mediaUris") or {}
        if not media_uris:
            log.warning("  %s: recording %s has no media yet", conv_id, rec_id)
            done = False
            continue
        conv_dir.mkdir(parents=True, exist_ok=True)
        try:
            for channel, info in sorted(media_uris.items()):
                if not CHANNEL_RE.match(channel):
                    raise RuntimeError(f"suspicious channel key {channel!r}")
                dest = conv_dir / f"{rec_id}_ch{channel}.mp3"
                size = download_file(info["mediaUri"], dest)
                log.info("  saved %s (%d bytes)", dest, size)
            state["recordings"][rec_id] = datetime.now(timezone.utc).isoformat()
            downloaded_any = True
        except Exception:
            # Signed URLs expire and CDNs hiccup; one recording failing must
            # not abort its siblings. The conversation retries next run.
            log.exception("  %s: recording %s download failed", conv_id, rec_id)
            done = False

    if downloaded_any:
        write_sidecar(conv_dir, conv)
    return done


def run(client):
    now = datetime.now(timezone.utc)
    lookback = min(LOOKBACK_HOURS, MAX_LOOKBACK_HOURS)
    if lookback != LOOKBACK_HOURS:
        log.warning("LOOKBACK_HOURS capped to %d (analytics interval limit)",
                    MAX_LOOKBACK_HOURS)
    start = now - timedelta(hours=lookback)
    end = now - timedelta(minutes=SAFETY_LAG_MINUTES)
    state = load_state()

    conversations = find_voice_conversations(client, start, end)
    log.info("found %d voice conversations in window", len(conversations))

    processed = failures = 0
    for conv in conversations:
        conv_id = conv["conversationId"]
        if conv_id in state["conversations_done"] or conv_id in state["given_up"]:
            continue
        conv_end = conv.get("conversationEnd")
        if not conv_end:
            continue  # call still in progress
        age_minutes = (now - _parse_ts(conv_end)).total_seconds() / 60
        if age_minutes < SAFETY_LAG_MINUTES:
            continue  # too fresh; recording may not be ingested yet
        if MAX_CONVERSATIONS and processed >= MAX_CONVERSATIONS:
            log.info("MAX_CONVERSATIONS=%d reached, stopping", MAX_CONVERSATIONS)
            break
        processed += 1
        attempts = state["attempts"].get(conv_id, 0) + 1
        state["attempts"][conv_id] = attempts
        log.info("processing %s (started %s, attempt %d)",
                 conv_id, conv.get("conversationStart"), attempts)
        try:
            if process_conversation(client, conv, state, age_minutes):
                state["conversations_done"][conv_id] = datetime.now(
                    timezone.utc
                ).isoformat()
                state["attempts"].pop(conv_id, None)
            elif attempts >= MAX_CONVERSATION_ATTEMPTS:
                log.error("%s: giving up after %d attempts — NEEDS OPERATOR REVIEW",
                          conv_id, attempts)
                state["given_up"][conv_id] = datetime.now(timezone.utc).isoformat()
        except Exception:
            failures += 1
            log.exception("  %s failed, will retry next run", conv_id)
        save_state(state)

    log.info("run complete: %d processed, %d failures, %d awaiting retry, %d given up",
             processed, failures, len(state["attempts"]), len(state["given_up"]))
    return 1 if failures else 0


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    os.umask(0o077)  # recordings are personal data: owner-only files
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_FILE.with_suffix(".lock")
    with open(lock_path, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log.info("another run is still active, exiting")
            return 0
        client = GenesysClient(os.environ["GC_CLIENT_ID"], os.environ["GC_CLIENT_SECRET"])
        try:
            client.authenticate()
            return run(client)
        except Exception:
            log.exception("run aborted")
            return 1


if __name__ == "__main__":
    sys.exit(main())
