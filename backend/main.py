"""
Spotify → Tidal Transfer — FastAPI Backend
Deploy on Railway. Frontend (Netlify) talks to this via REST + SSE.
"""

import asyncio
import concurrent.futures
import json
import queue
import threading
import time
import uuid
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

tidal_sessions: dict[str, dict] = {}       # session_id → {session, url, status}
transfer_jobs:  dict[str, queue.Queue] = {} # job_id → progress event queue
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
            login, future = sess.login_oauth_simple()
            tidal_sessions[session_id] = {
                "session": sess,
                "url": login.verification_uri_complete,
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
    return {"status": data["status"]}


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
    transfer_jobs[job_id] = q

    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, _run_transfer, req, data["session"], q)

    return {"job_id": job_id}


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
                if msg.get("type") in ("done", "error"):
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

def _run_transfer(req: TransferRequest, tidal: tidalapi.Session, q: queue.Queue):
    def emit(msg):    q.put(msg)
    def log(msg, level="info"): emit({"type": "log", "message": msg, "level": level})

    try:
        sp = spotipy.Spotify(auth=req.spotify_token)
        summary = {}

        # ── Liked Songs ──────────────────────────────────────────────────────
        if req.transfer_liked:
            log("📥 Fetching liked songs from Spotify…")
            tracks = _fetch_liked(sp, emit)
            log(f"🔍 Matching {len(tracks)} tracks on Tidal…")
            ok, fail = _transfer_liked(tidal, tracks, emit)
            summary["liked_songs"] = {"total": len(tracks), "ok": ok, "fail": fail}
            emit({"type": "section_done", "section": "liked_songs", "ok": ok, "fail": fail})

        # ── Playlists ────────────────────────────────────────────────────────
        if req.transfer_playlists:
            log("📥 Fetching playlists from Spotify…")
            playlists = _fetch_playlists(sp, emit)
            log(f"🔍 Transferring {len(playlists)} playlists to Tidal…")
            ok, fail = _transfer_playlists(tidal, playlists, emit)
            summary["playlists"] = {"total": len(playlists), "ok": ok, "fail": fail}
            emit({"type": "section_done", "section": "playlists", "ok": ok, "fail": fail})

        # ── Albums ───────────────────────────────────────────────────────────
        if req.transfer_albums:
            log("📥 Fetching saved albums from Spotify…")
            albums = _fetch_albums(sp, emit)
            log(f"🔍 Matching {len(albums)} albums on Tidal…")
            ok, fail = _transfer_albums(tidal, albums, emit)
            summary["albums"] = {"total": len(albums), "ok": ok, "fail": fail}
            emit({"type": "section_done", "section": "albums", "ok": ok, "fail": fail})

        # ── Artists ──────────────────────────────────────────────────────────
        if req.transfer_artists:
            log("📥 Fetching followed artists from Spotify…")
            artists = _fetch_artists(sp, emit)
            log(f"🔍 Matching {len(artists)} artists on Tidal…")
            ok, fail = _transfer_artists(tidal, artists, emit)
            summary["artists"] = {"total": len(artists), "ok": ok, "fail": fail}
            emit({"type": "section_done", "section": "artists", "ok": ok, "fail": fail})

        log("✅ All done!")
        emit({"type": "done", "summary": summary})

    except Exception as e:
        emit({"type": "error", "message": str(e)})


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

def _find_track(tidal: tidalapi.Session, track: dict) -> Optional[tidalapi.Track]:
    """Search Tidal for a track, using ISRC first (cross-service ID), then name."""
    try:
        if track.get("isrc"):
            res = tidal.search(track["isrc"], models=[tidalapi.Track])
            for r in res.get("tracks", []):
                if getattr(r, "isrc", None) == track["isrc"]:
                    return r
        res = tidal.search(f"{track['title']} {track['artist']}", models=[tidalapi.Track])
        hits = res.get("tracks", [])
        return hits[0] if hits else None
    except Exception:
        return None


# ─── Tidal writers ────────────────────────────────────────────────────────────

def _transfer_liked(tidal, tracks, emit):
    ok = fail = 0
    for i, track in enumerate(tracks):
        r = _find_track(tidal, track)
        if r:
            try:
                tidal.user.favorites.add_track(r.id)
                ok += 1
            except Exception:
                fail += 1
        else:
            fail += 1
        emit({"type": "progress", "section": "liked_songs",
              "done": i + 1, "total": len(tracks), "ok": ok, "fail": fail})
        time.sleep(0.12)
    return ok, fail


def _transfer_playlists(tidal, playlists, emit):
    total_ok = total_fail = 0
    for i, pl in enumerate(playlists):
        try:
            tp = tidal.user.create_playlist(pl["name"], pl["description"])
        except Exception as e:
            emit({"type": "log", "message": f"⚠️ Skipped '{pl['name']}': {e}", "level": "warning"})
            continue

        ids, ok, fail = [], 0, 0
        for t in pl["tracks"]:
            r = _find_track(tidal, t)
            if r:
                ids.append(r.id)
                ok += 1
            else:
                fail += 1
            time.sleep(0.08)

        # Add tracks in batches of 50
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
    return total_ok, total_fail


def _transfer_albums(tidal, albums, emit):
    ok = fail = 0
    for i, album in enumerate(albums):
        try:
            res = tidal.search(f"{album['title']} {album['artist']}", models=[tidalapi.Album])
            hits = res.get("albums", [])
            if hits:
                tidal.user.favorites.add_album(hits[0].id)
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1
        emit({"type": "progress", "section": "albums",
              "done": i + 1, "total": len(albums), "ok": ok, "fail": fail})
        time.sleep(0.12)
    return ok, fail


def _transfer_artists(tidal, artists, emit):
    ok = fail = 0
    for i, artist in enumerate(artists):
        try:
            res = tidal.search(artist["name"], models=[tidalapi.Artist])
            hits = res.get("artists", [])
            if hits:
                tidal.user.favorites.add_artist(hits[0].id)
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1
        emit({"type": "progress", "section": "artists",
              "done": i + 1, "total": len(artists), "ok": ok, "fail": fail})
        time.sleep(0.12)
    return ok, fail
