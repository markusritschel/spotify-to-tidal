"""
Spotify → Tidal Transfer — FastAPI Backend
Deploy on Railway. Frontend (Netlify) talks to this via REST + SSE.
"""

import asyncio
import concurrent.futures
import json
import queue
import re
import threading
import time
import uuid
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import spotipy
import tidalapi

# ─── App setup ───────────────────────────────────────────────────────────────

app = FastAPI(title="Spotify → Tidal Transfer API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Restrict to your Netlify URL in production if desired
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── In-memory state (fine for single Railway instance) ──────────────────────

tidal_sessions: dict[str, dict] = {}           # session_id → {session, url, status}
transfer_jobs:  dict[str, queue.Queue] = {}    # job_id → progress event queue
cancel_flags:   dict[str, threading.Event] = {} # job_id → cancellation event
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)


# ─── Health check ─────────────────────────────────────────────────────────────

@app.get("/")
async def health():
    return {"status": "ok", "service": "spotify-to-tidal"}


# ─── Tidal Auth ───────────────────────────────────────────────────────────────

@app.post("/tidal/auth")
async def start_tidal_auth():
    """
    Starts the Tidal OAuth device flow.
    Returns a URL the user must visit to log in, plus a session_id to poll with.
    """
    session_id = str(uuid.uuid4())
    ready = threading.Event()

    def _init_tidal():
        sess = tidalapi.Session()
        try:
            login, future = sess.login_oauth()
            tidal_sessions[session_id] = {
                "session": sess,
                "url": login.verification_uri_complete if login.verification_uri_complete.startswith("http") else "https://" + login.verification_uri_complete,
                "status": "pending",
            }
            ready.set()
            future.result(timeout=600)  # Wait up to 10 min for user
            tidal_sessions[session_id]["status"] = "authenticated"
        except Exception as e:
            tidal_sessions.setdefault(session_id, {})["status"] = "failed"
            tidal_sessions[session_id]["error"] = str(e)
            ready.set()

    threading.Thread(target=_init_tidal, daemon=True).start()
    ready.wait(timeout=15)  # Wait for URL to be generated

    data = tidal_sessions.get(session_id, {})
    if data.get("status") == "failed":
        raise HTTPException(500, f"Tidal init failed: {data.get('error', 'unknown')}")
    if "url" not in data:
        raise HTTPException(504, "Tidal took too long to initialize. Please try again.")

    return {"session_id": session_id, "url": data["url"]}


@app.get("/tidal/auth/status")
async def tidal_auth_status(session_id: str):
    """Poll this endpoint until status becomes 'authenticated'."""
    data = tidal_sessions.get(session_id)
    if not data:
        return {"status": "not_found"}
    resp: dict = {"status": data["status"]}
    if data["status"] == "authenticated":
        sess = data["session"]
        resp["session_tokens"] = {
            "token_type":    sess.token_type,
            "access_token":  sess.access_token,
            "refresh_token": sess.refresh_token,
            "expiry_time":   sess.expiry_time.timestamp() if sess.expiry_time else None,
        }
    return resp


class TidalRestoreRequest(BaseModel):
    token_type:    str
    access_token:  str
    refresh_token: Optional[str] = None
    expiry_time:   Optional[float] = None


@app.post("/tidal/restore")
async def restore_tidal_session(req: TidalRestoreRequest):
    """Restore a Tidal session from previously stored tokens."""
    sess = tidalapi.Session()
    expiry = datetime.fromtimestamp(req.expiry_time) if req.expiry_time else None
    try:
        sess.load_oauth_session(req.token_type, req.access_token, req.refresh_token, expiry)
        _ = sess.user.id  # Verify session is valid
    except Exception as e:
        raise HTTPException(401, f"Could not restore Tidal session: {e}")
    session_id = str(uuid.uuid4())
    tidal_sessions[session_id] = {"session": sess, "status": "authenticated"}
    return {"session_id": session_id}


# ─── Transfer ─────────────────────────────────────────────────────────────────

class TransferRequest(BaseModel):
    spotify_token:      str
    tidal_session_id:   str
    transfer_liked:     bool = True
    transfer_playlists: bool = True
    transfer_albums:    bool = True
    transfer_artists:   bool = True


@app.post("/transfer/start")
async def start_transfer(req: TransferRequest):
    """Kick off a transfer job. Returns job_id to stream progress from."""
    data = tidal_sessions.get(req.tidal_session_id)
    if not data or data.get("status") != "authenticated":
        raise HTTPException(400, "Tidal session not authenticated. Please connect Tidal first.")

    job_id = str(uuid.uuid4())
    q: queue.Queue = queue.Queue()
    cancel = threading.Event()
    transfer_jobs[job_id] = q
    cancel_flags[job_id] = cancel

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_transfer, req, data["session"], q, cancel, job_id)

    return {"job_id": job_id}


@app.post("/transfer/cancel/{job_id}")
async def cancel_transfer(job_id: str):
    """Signal a running transfer to stop after the current item."""
    flag = cancel_flags.get(job_id)
    if not flag:
        raise HTTPException(404, "Job not found")
    flag.set()
    return {"status": "cancelling"}


@app.get("/transfer/progress/{job_id}")
async def transfer_progress(job_id: str):
    """
    SSE stream of transfer progress events.
    Events: {type: log|progress|section_done|done|error, ...}
    """
    q = transfer_jobs.get(job_id)
    if not q:
        raise HTTPException(404, "Job not found")

    async def stream():
        while True:
            try:
                msg = q.get_nowait()
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") in ("done", "error", "cancelled"):
                    break
            except queue.Empty:
                yield ": heartbeat\n\n"   # Keep connection alive
                await asyncio.sleep(0.4)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # Disable nginx buffering on Railway
            "Access-Control-Allow-Origin": "*",
        },
    )


# ─── Transfer logic (runs in thread pool) ─────────────────────────────────────

def _run_transfer(req: TransferRequest, tidal: tidalapi.Session, q: queue.Queue, cancel: threading.Event, job_id: str):
    def emit(msg):    q.put(msg)
    def log(msg, level="info"): emit({"type": "log", "message": msg, "level": level})
    def cancelled(): return cancel.is_set()

    try:
        sp = spotipy.Spotify(auth=req.spotify_token)
        summary = {}

        # ── Liked Songs ──────────────────────────────────────────────────────
        if req.transfer_liked and not cancelled():
            log("📥 Fetching liked songs from Spotify…")
            tracks = _fetch_liked(sp, emit)
            log(f"🔍 Matching {len(tracks)} tracks on Tidal…")
            ok, fail, failed = _transfer_liked(tidal, tracks, emit, cancelled)
            summary["liked_songs"] = {"total": len(tracks), "ok": ok, "fail": fail, "failed_items": failed}
            emit({"type": "section_done", "section": "liked_songs", "ok": ok, "fail": fail})

        # ── Playlists ────────────────────────────────────────────────────────
        if req.transfer_playlists and not cancelled():
            log("📥 Fetching playlists from Spotify…")
            playlists = _fetch_playlists(sp, emit)
            log(f"🔍 Transferring {len(playlists)} playlists to Tidal…")
            ok, fail, failed = _transfer_playlists(tidal, playlists, emit, cancelled)
            summary["playlists"] = {"total": len(playlists), "ok": ok, "fail": fail, "failed_items": failed}
            emit({"type": "section_done", "section": "playlists", "ok": ok, "fail": fail})

        # ── Albums ───────────────────────────────────────────────────────────
        if req.transfer_albums and not cancelled():
            log("📥 Fetching saved albums from Spotify…")
            albums = _fetch_albums(sp, emit)
            log(f"🔍 Matching {len(albums)} albums on Tidal…")
            ok, fail, failed = _transfer_albums(tidal, albums, emit, cancelled)
            summary["albums"] = {"total": len(albums), "ok": ok, "fail": fail, "failed_items": failed}
            emit({"type": "section_done", "section": "albums", "ok": ok, "fail": fail})

        # ── Artists ──────────────────────────────────────────────────────────
        if req.transfer_artists and not cancelled():
            log("📥 Fetching followed artists from Spotify…")
            artists = _fetch_artists(sp, emit)
            log(f"🔍 Matching {len(artists)} artists on Tidal…")
            ok, fail, failed = _transfer_artists(tidal, artists, emit, cancelled)
            summary["artists"] = {"total": len(artists), "ok": ok, "fail": fail, "failed_items": failed}
            emit({"type": "section_done", "section": "artists", "ok": ok, "fail": fail})

        if cancelled():
            log("⛔ Transfer cancelled.")
            emit({"type": "cancelled", "summary": summary})
        else:
            log("✅ All done!")
            emit({"type": "done", "summary": summary})

    except Exception as e:
        emit({"type": "error", "message": str(e)})
    finally:
        cancel_flags.pop(job_id, None)
        transfer_jobs.pop(job_id, None)


# ─── Spotify fetchers ─────────────────────────────────────────────────────────

def _fetch_liked(sp, emit) -> list[dict]:
    tracks, results = [], sp.current_user_saved_tracks(limit=50)
    total = results["total"]
    while True:
        for item in results["items"]:
            t = item.get("track")
            if t:
                tracks.append({
                    "title": t["name"],
                    "artist": t["artists"][0]["name"],
                    "album": t["album"]["name"],
                    "isrc": t.get("external_ids", {}).get("isrc", ""),
                })
        emit({"type": "progress", "section": "fetch_liked", "done": len(tracks), "total": total})
        if not results["next"]:
            break
        results = sp.next(results)
    return tracks


def _fetch_playlists(sp, emit) -> list[dict]:
    raw, results = [], sp.current_user_playlists(limit=50)
    while True:
        raw.extend([p for p in results["items"] if p])
        if not results["next"]:
            break
        results = sp.next(results)

    playlists = []
    for i, p in enumerate(raw):
        tracks, tr = [], sp.playlist_tracks(p["id"], limit=100)
        while True:
            for item in tr["items"]:
                t = item.get("track")
                if t and t.get("id"):
                    tracks.append({
                        "title": t["name"],
                        "artist": t["artists"][0]["name"],
                        "album": t["album"]["name"],
                        "isrc": t.get("external_ids", {}).get("isrc", ""),
                    })
            if not tr["next"]:
                break
            tr = sp.next(tr)
        playlists.append({
            "name": p["name"],
            "description": p.get("description", "") or "",
            "tracks": tracks,
        })
        emit({"type": "progress", "section": "fetch_playlists", "done": i + 1, "total": len(raw)})
    return playlists


def _fetch_albums(sp, emit) -> list[dict]:
    albums, results = [], sp.current_user_saved_albums(limit=50)
    total = results["total"]
    while True:
        for item in results["items"]:
            a = item["album"]
            albums.append({
                "title": a["name"],
                "artist": a["artists"][0]["name"],
                "upc": a.get("external_ids", {}).get("upc", ""),
            })
        emit({"type": "progress", "section": "fetch_albums", "done": len(albums), "total": total})
        if not results["next"]:
            break
        results = sp.next(results)
    return albums


def _fetch_artists(sp, emit) -> list[dict]:
    artists, results = [], sp.current_user_followed_artists(limit=50)
    while True:
        for a in results["artists"]["items"]:
            artists.append({"name": a["name"]})
        if not results["artists"]["next"]:
            break
        results = sp.next(results["artists"])
    return artists


# ─── Tidal search ─────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase and strip punctuation for loose matching."""
    return re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()


def _result_matches(r: tidalapi.Track, artist_norm: str, title_norm: str) -> bool:
    """Return True if a Tidal result is a plausible match for the given artist+title."""
    try:
        r_artist = _norm(r.artist.name if r.artist else "")
        r_title  = _norm(r.name or "")
        artist_ok = artist_norm in r_artist or r_artist in artist_norm
        title_ok  = title_norm  in r_title  or r_title  in title_norm
        return artist_ok and title_ok
    except Exception:
        return False


def _find_track(tidal: tidalapi.Session, track: dict) -> Optional[tidalapi.Track]:
    """Search Tidal for a track.

    Strategy:
    1. ISRC exact match (most reliable — same recording across services).
    2. Verified name search: try up to three query forms and check top-5
       results against normalised artist+title.  Returns the first result
       that passes the check.
    3. Last resort: return the first hit of the basic query even if
       unverified (preserves old behaviour for edge cases).
    """
    def _search(query):
        try:
            res = tidal.search(query, models=[tidalapi.Track])
            return res.get("tracks", [])
        except Exception:
            return []

    try:
        # ── 1. ISRC ───────────────────────────────────────────────────────
        if track.get("isrc"):
            for r in _search(track["isrc"]):
                if getattr(r, "isrc", None) == track["isrc"]:
                    return r

        title  = track.get("title", "")
        artist = track.get("artist", "")
        album  = track.get("album", "")
        artist_norm = _norm(artist)
        title_norm  = _norm(title)

        queries = [
            f"{title} {artist}",
            f"{artist} {title}",
        ]
        if album:
            queries.append(f"{artist} {album} {title}")

        # ── 2. Verified name search ───────────────────────────────────────
        first_hits = None
        for query in queries:
            hits = _search(query)
            if first_hits is None:
                first_hits = hits
            for r in hits[:5]:
                if _result_matches(r, artist_norm, title_norm):
                    return r

        # ── 3. Album-based lookup ─────────────────────────────────────────
        # Search for the album, verify the artist, then scan its track list.
        # Catches tracks that rank poorly in cross-library text search.
        if album:
            try:
                res = tidal.search(f"{artist} {album}", models=[tidalapi.Album])
                for a in res.get("albums", [])[:3]:
                    a_artist = _norm(a.artist.name if a.artist else "")
                    if not (artist_norm in a_artist or a_artist in artist_norm):
                        continue
                    for t in a.tracks():
                        if _result_matches(t, artist_norm, title_norm):
                            return t
            except Exception:
                pass

        # ── 4. Last resort ────────────────────────────────────────────────
        return first_hits[0] if first_hits else None

    except Exception:
        return None


# ─── Tidal writers ────────────────────────────────────────────────────────────

def _transfer_liked(tidal, tracks, emit, cancelled):
    ok = fail = 0
    failed = []
    for i, track in enumerate(tracks):
        if cancelled(): break
        r = _find_track(tidal, track)
        if r:
            try:
                tidal.user.favorites.add_track(r.id)
                ok += 1
            except Exception:
                fail += 1
                failed.append({"title": track["title"], "artist": track["artist"], "album": track["album"]})
                emit({"type": "log", "message": f"✗ {track['artist']} - {track['album']} - {track['title']}", "level": "warning"})
        else:
            fail += 1
            failed.append({"title": track["title"], "artist": track["artist"], "album": track["album"]})
            emit({"type": "log", "message": f"✗ {track['artist']} - {track['album']} - {track['title']}", "level": "warning"})
        emit({"type": "progress", "section": "liked_songs",
              "done": i + 1, "total": len(tracks), "ok": ok, "fail": fail})
        time.sleep(0.12)
    return ok, fail, failed


def _tidal_playlist_tracks(tp) -> list:
    """Fetch all tracks from a Tidal playlist, handling pagination."""
    tracks, offset = [], 0
    while True:
        batch = tp.tracks(limit=100, offset=offset)
        if not batch:
            break
        tracks.extend(batch)
        if len(batch) < 100:
            break
        offset += 100
    return tracks


def _transfer_playlists(tidal, playlists, emit, cancelled):
    total_ok = total_fail = 0
    failed = []

    try:
        existing = {p.name: p for p in tidal.user.playlist_and_favorite_playlists()}
    except Exception:
        existing = {}

    for i, pl in enumerate(playlists):
        if cancelled(): break

        if pl["name"] in existing:
            # ── Sync existing playlist ────────────────────────────────────────
            tp = existing[pl["name"]]
            emit({"type": "log", "message": f"🔄 Syncing '{pl['name']}'…", "level": "info"})

            tidal_tracks = _tidal_playlist_tracks(tp)
            tidal_isrc_set  = {getattr(t, "isrc", "") for t in tidal_tracks if getattr(t, "isrc", "")}
            spotify_isrc_set = {t["isrc"] for t in pl["tracks"] if t["isrc"]}

            # Remove tracks that are in Tidal but no longer in Spotify
            to_remove = [idx for idx, t in enumerate(tidal_tracks)
                         if getattr(t, "isrc", "") and getattr(t, "isrc", "") not in spotify_isrc_set]
            for idx in sorted(to_remove, reverse=True):
                try:
                    tp.remove_by_index(idx)
                except Exception:
                    pass
                time.sleep(0.05)

            # Add tracks that are in Spotify but not yet in Tidal
            to_add = [t for t in pl["tracks"] if t["isrc"] and t["isrc"] not in tidal_isrc_set]
            ids, ok, fail = [], 0, 0
            for t in to_add:
                if cancelled(): break
                r = _find_track(tidal, t)
                if r:
                    ids.append(r.id)
                    ok += 1
                else:
                    fail += 1
                    failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                    emit({"type": "log", "message": f"✗ [{pl['name']}] {t['artist']} - {t['album']} - {t['title']}", "level": "warning"})
                time.sleep(0.08)
            for b in range(0, len(ids), 50):
                try:
                    tp.add(ids[b : b + 50])
                except Exception:
                    pass
                time.sleep(0.2)
            removed = len(to_remove)
            added   = ok
            emit({"type": "log",
                  "message": f"✅ '{pl['name']}': +{added} added, -{removed} removed",
                  "level": "info"})
        else:
            # ── Create new playlist ───────────────────────────────────────────
            try:
                tp = tidal.user.create_playlist(pl["name"], pl["description"])
            except Exception as e:
                emit({"type": "log", "message": f"⚠️ Skipped '{pl['name']}': {e}", "level": "warning"})
                continue

            ids, ok, fail = [], 0, 0
            for t in pl["tracks"]:
                if cancelled(): break
                r = _find_track(tidal, t)
                if r:
                    ids.append(r.id)
                    ok += 1
                else:
                    fail += 1
                    failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                    emit({"type": "log", "message": f"✗ [{pl['name']}] {t['artist']} - {t['album']} - {t['title']}", "level": "warning"})
                time.sleep(0.08)
            for b in range(0, len(ids), 50):
                try:
                    tp.add(ids[b : b + 50])
                except Exception:
                    pass
                time.sleep(0.2)

        total_ok += ok
        total_fail += fail
        emit({
            "type": "progress", "section": "playlists",
            "done": i + 1, "total": len(playlists),
            "ok": total_ok, "fail": total_fail,
            "current": pl["name"],
        })
    return total_ok, total_fail, failed


def _transfer_albums(tidal, albums, emit, cancelled):
    ok = fail = 0
    failed = []
    for i, album in enumerate(albums):
        if cancelled(): break
        try:
            res = tidal.search(f"{album['title']} {album['artist']}", models=[tidalapi.Album])
            hits = res.get("albums", [])
            if hits:
                tidal.user.favorites.add_album(hits[0].id)
                ok += 1
            else:
                fail += 1
                failed.append({"title": album["title"], "artist": album["artist"]})
                emit({"type": "log", "message": f"✗ {album['artist']} - {album['title']}", "level": "warning"})
        except Exception:
            fail += 1
            failed.append({"title": album["title"], "artist": album["artist"]})
            emit({"type": "log", "message": f"✗ {album['title']} — {album['artist']}", "level": "warning"})
        emit({"type": "progress", "section": "albums",
              "done": i + 1, "total": len(albums), "ok": ok, "fail": fail})
        time.sleep(0.12)
    return ok, fail, failed


def _transfer_artists(tidal, artists, emit, cancelled):
    ok = fail = 0
    failed = []
    for i, artist in enumerate(artists):
        if cancelled(): break
        try:
            res = tidal.search(artist["name"], models=[tidalapi.Artist])
            hits = res.get("artists", [])
            if hits:
                tidal.user.favorites.add_artist(hits[0].id)
                ok += 1
            else:
                fail += 1
                failed.append({"title": artist["name"]})
                emit({"type": "log", "message": f"✗ {artist['name']}", "level": "warning"})
        except Exception:
            fail += 1
            failed.append({"title": artist["name"]})
            emit({"type": "log", "message": f"✗ {artist['name']}", "level": "warning"})
        emit({"type": "progress", "section": "artists",
              "done": i + 1, "total": len(artists), "ok": ok, "fail": fail})
        time.sleep(0.12)
    return ok, fail, failed
