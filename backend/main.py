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
import unicodedata
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
    playlist_ids:       Optional[list[str]] = None  # None = all playlists


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
            playlists = _fetch_playlists(sp, emit, playlist_ids=req.playlist_ids)
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


def _fetch_playlists(sp, emit, playlist_ids=None) -> list[dict]:
    raw, results = [], sp.current_user_playlists(limit=50)
    while True:
        raw.extend([p for p in results["items"] if p])
        if not results["next"]:
            break
        results = sp.next(results)

    # Spotify-owned playlists (Discover Weekly, Daily Mix, etc.) return 403
    # on the tracks endpoint even with playlist-read-private scope — skip them.
    user_playlists = [p for p in raw if p.get("owner", {}).get("id") != "spotify"]
    skipped = len(raw) - len(user_playlists)
    if skipped:
        emit({"type": "log",
              "message": f"⏭ Skipped {skipped} Spotify-owned playlist(s) (Discover Weekly, Daily Mix, etc.)",
              "level": "info"})

    # Apply user selection filter if provided
    if playlist_ids is not None:
        id_set = set(playlist_ids)
        user_playlists = [p for p in user_playlists if p["id"] in id_set]

    playlists = []
    for i, p in enumerate(user_playlists):
        try:
            tracks, tr = [], sp.playlist_items(p["id"], limit=100, additional_types=("track",))
            raw_item_count = 0
            while True:
                raw_item_count += len(tr["items"])
                for item in tr["items"]:
                    t = item.get("track") or item.get("item")
                    if t and t.get("id") and t.get("type", "track") == "track":
                        tracks.append({
                            "title": t["name"],
                            "artist": t["artists"][0]["name"],
                            "album": t["album"]["name"],
                            "isrc": t.get("external_ids", {}).get("isrc", ""),
                        })
                if not tr["next"]:
                    break
                tr = sp.next(tr)
            if not tracks and raw_item_count > 0:
                # Debug: show why items were filtered out
                tr2 = sp.playlist_items(p["id"], limit=5, additional_types=("track",))
                samples = []
                for item in tr2["items"][:3]:
                    t = item.get("track")
                    samples.append({
                        "track_is_none": t is None,
                        "id": t.get("id") if t else None,
                        "type": t.get("type") if t else None,
                        "name": t.get("name") if t else None,
                        "item_keys": list(item.keys()),
                    })
                owner = p.get("owner", {})
                emit({"type": "log",
                      "message": f"🔍 Debug '{p['name']}': {raw_item_count} raw items, 0 passed filter. "
                                 f"Owner: {owner.get('id')} / {owner.get('display_name')}. Samples: {samples}",
                      "level": "warning"})
            playlists.append({
                "name": p["name"],
                "description": p.get("description", "") or "",
                "tracks": tracks,
            })
        except spotipy.SpotifyException as e:
            if e.http_status == 403:
                emit({"type": "log",
                      "message": f"⏭ Skipped '{p['name']}': not owned by you (Spotify API restriction)",
                      "level": "info"})
            else:
                emit({"type": "log",
                      "message": f"⚠ Could not fetch playlist '{p['name']}': {e}",
                      "level": "warning"})
        except Exception as e:
            emit({"type": "log",
                  "message": f"⚠ Could not fetch playlist '{p['name']}': {e}",
                  "level": "warning"})
        emit({"type": "progress", "section": "fetch_playlists", "done": i + 1, "total": len(user_playlists)})
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
    """Lowercase, convert accents to ASCII equivalents, strip punctuation."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    # Replace punctuation/separators with spaces (so "Tim-Dom-Dom" → "tim dom dom")
    s = re.sub(r"[^a-z0-9 ]+", " ", s.lower())
    return re.sub(r" +", " ", s).strip()


def _ascii(s: str) -> str:
    """Strip accents only — used to build ASCII fallback search queries."""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").strip()


# Patterns commonly appended to track titles on one service but not the other.
_TITLE_NOISE = re.compile(
    r"\s*[\(\[]"                               # opening ( or [
    r"(?:remaster(?:ed)?|live|acoustic"
    r"|radio edit|single version|album version"
    r"|mono|stereo|demo|bonus track"
    r"|feat\.?|ft\.?|\d{4})"                  # keyword or year anchor
    r"[^\)\]]*"                                # consume rest inside brackets
    r"[\)\]]"                                  # closing ) or ]
    r"|\s+-\s+(?:remaster(?:ed)?|live|acoustic|radio edit|single|mono|stereo|demo).*$",
    re.IGNORECASE,
)


def _clean(s: str) -> str:
    """Strip common version/remaster suffixes then normalise."""
    return _norm(_TITLE_NOISE.sub("", s).strip())


def _titles_match(a: str, b: str) -> bool:
    """True if two raw title strings likely refer to the same track.

    Cleans version noise first, then requires either exact equality or
    a substring match where the shorter is ≥ 85 % the length of the longer
    (prevents 'Love' from matching 'Love Song').
    """
    ca, cb = _clean(a), _clean(b)
    if ca == cb:
        return True
    short, long_ = (ca, cb) if len(ca) <= len(cb) else (cb, ca)
    return bool(short) and short in long_ and len(short) / len(long_) >= 0.85


def _result_matches(r: tidalapi.Track, artist_norm: str, title_norm: str) -> bool:
    """Return True if a Tidal result is a plausible match for the given artist+title."""
    try:
        all_artists = getattr(r, "artists", None) or ([r.artist] if r.artist else [])
        artist_ok = any(
            artist_norm in _norm(a.name) or _norm(a.name) in artist_norm
            for a in all_artists
        )
        title_ok = _titles_match(r.name or "", title_norm)
        return artist_ok and title_ok
    except Exception:
        return False


def _find_track(tidal: tidalapi.Session, track: dict) -> Optional[tidalapi.Track]:
    """Search Tidal for a track.

    Strategy (each step only runs if previous found nothing):
    1. ISRC via dedicated v2 endpoint — most reliable.
    2. Raw text search — title+artist+album and title+artist queries
       (+ ASCII fallbacks), top 10 results verified.
    3. Cleaned text search — same queries with remaster/feat. noise stripped
       from the query itself (only when cleaning changes the title).
    4. Album text search — search artist+album, walk tracks by title.
    5. Artist discography — search artist, browse get_albums()+get_ep_singles(),
       match album then track by title (last resort, expensive).
    """
    def _search_tracks(query):
        try:
            return tidal.search(query, models=[tidalapi.Track]).get("tracks", [])
        except Exception:
            return []

    def _search_albums(query):
        try:
            return tidal.search(query, models=[tidalapi.Album]).get("albums", [])
        except Exception:
            return []

    def _search_artists(query):
        try:
            return tidal.search(query, models=[tidalapi.Artist]).get("artists", [])
        except Exception:
            return []

    def _check(hits, artist_norm, title_norm, album_norm=""):
        first_match = None
        for r in hits[:10]:
            if _result_matches(r, artist_norm, title_norm):
                if album_norm and getattr(r, "album", None) and _titles_match(r.album.name or "", album_norm):
                    return r  # exact album match — best possible
                if first_match is None:
                    first_match = r
        return first_match

    try:
        title  = track.get("title", "")
        artist = track.get("artist", "")
        album  = track.get("album", "")
        artist_norm = _norm(artist)
        title_norm  = _norm(title)
        album_norm  = _norm(album)
        ta, ar, al  = _ascii(title), _ascii(artist), _ascii(album)
        has_accents = (ta != title or ar != artist)

        # ── 1. ISRC — dedicated v2 endpoint ──────────────────────────────
        isrc_fallback = None
        if track.get("isrc"):
            try:
                results = tidal.get_tracks_by_isrc(track["isrc"])
                if results:
                    # Prefer the version from the matching album
                    if album_norm:
                        for r in results:
                            if getattr(r, "album", None) and _titles_match(r.album.name or "", album_norm):
                                return r
                    # Wrong album or no album info — save as fallback, try text search first
                    if album_norm:
                        isrc_fallback = results[0]
                    else:
                        return results[0]
            except Exception:
                pass

        # ── 2. Raw text search (with album for disambiguation) ────────────
        raw_queries = []
        if album:
            raw_queries += [f"{title} {artist} {album}", f"{artist} {album} {title}"]
        raw_queries += [f"{title} {artist}", f"{artist} {title}"]
        if has_accents:
            if album:
                raw_queries += [f"{ta} {ar} {al}", f"{ar} {al} {ta}"]
            raw_queries += [f"{ta} {ar}", f"{ar} {ta}"]
        for q in raw_queries:
            r = _check(_search_tracks(q), artist_norm, title_norm, album_norm)
            if r:
                return r

        # ── 2b. Normalized query (punctuation → spaces) ─────────────────
        #    Handles cases where hyphens/punctuation in titles confuse search
        norm_title = _norm(title)
        norm_artist = _norm(artist)
        if norm_title != title.lower().strip() or norm_artist != artist.lower().strip():
            norm_queries = [f"{norm_title} {norm_artist}", f"{norm_artist} {norm_title}"]
            for q in norm_queries:
                r = _check(_search_tracks(q), artist_norm, title_norm, album_norm)
                if r:
                    return r

        # ── 3. Cleaned text search (strip remaster/feat. from query) ──────
        clean_title = _clean(title)
        if clean_title != title_norm:  # cleaning actually changed something
            clean_queries = [f"{clean_title} {artist}", f"{artist} {clean_title}"]
            if has_accents:
                clean_queries += [f"{_ascii(clean_title)} {ar}", f"{ar} {_ascii(clean_title)}"]
            for q in clean_queries:
                r = _check(_search_tracks(q), artist_norm, title_norm, album_norm)
                if r:
                    return r

        # ── 4. Album text search → walk tracks ───────────────────────────
        if album:
            alb_queries = [f"{artist} {album}"]
            if has_accents or al != album:
                alb_queries.append(f"{ar} {al}")
            for aq in alb_queries:
                for a in _search_albums(aq)[:5]:
                    try:
                        for t in a.tracks():
                            if title_norm and _titles_match(t.name or "", title_norm):
                                return t
                    except Exception:
                        continue

        # ── 5. Artist discography browse (last resort) ────────────────────
        art_queries = [artist]
        if has_accents:
            art_queries.append(ar)
        for aq in art_queries:
            for art in _search_artists(aq)[:3]:
                try:
                    discography = []
                    try:
                        discography += art.get_albums()
                    except Exception:
                        pass
                    try:
                        discography += art.get_ep_singles()
                    except Exception:
                        pass
                    for alb in discography:
                        if album and not _titles_match(alb.name or "", album):
                            continue
                        try:
                            for t in alb.tracks():
                                if _titles_match(t.name or "", title_norm):
                                    return t
                        except Exception:
                            continue
                except Exception:
                    continue

        # No album-specific match found — use ISRC result as last resort
        return isrc_fallback

    except Exception:
        return isrc_fallback


# ─── Tidal writers ────────────────────────────────────────────────────────────

def _transfer_liked(tidal, tracks, emit, cancelled):
    ok = fail = 0
    failed = []
    for i, track in enumerate(tracks):
        if cancelled():
            break
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
    track_total = sum(len(pl["tracks"]) for pl in playlists)
    tracks_done = 0

    try:
        existing = {p.name: p for p in tidal.user.playlists()}
    except Exception:
        existing = {}

    for i, pl in enumerate(playlists):
        if cancelled():
            break

        n_spotify = len(pl["tracks"])
        emit({"type": "log",
              "message": f"📋 '{pl['name']}': {n_spotify} track(s) fetched from Spotify",
              "level": "info"})

        if pl["name"] in existing:
            # ── Sync existing playlist ────────────────────────────────────────
            tp = existing[pl["name"]]
            emit({"type": "log", "message": f"🔄 Syncing '{pl['name']}'…", "level": "info"})

            # _tidal_playlist_tracks calls tp.tracks() which sets tp._etag — required for add()
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

            # Add missing tracks one at a time (mirrors _transfer_liked approach)
            to_add = [t for t in pl["tracks"] if not (t["isrc"] and t["isrc"] in tidal_isrc_set)]
            emit({"type": "log", "message": f"🔗 '{pl['name']}' (sync): etag={'set' if tp._etag else 'MISSING'}, {len(to_add)} to add", "level": "info"})
            ok = fail = 0
            for j, t in enumerate(to_add):
                if cancelled():
                    break
                r = _find_track(tidal, t)
                if r:
                    try:
                        added_ids = tp.add([r.id])
                        if added_ids:
                            ok += 1
                        else:
                            fail += 1
                            failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                            emit({"type": "log", "message": f"⚠️ Tidal skipped '{t['title']}' (id={r.id}) — duplicate or unavailable in your region", "level": "warning"})
                    except Exception as e:
                        fail += 1
                        failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                        emit({"type": "log", "message": f"⚠️ Could not add '{t['title']}' (id={r.id}): {e}", "level": "warning"})
                else:
                    fail += 1
                    failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                    emit({"type": "log", "message": f"✗ [{pl['name']}] {t['artist']} - {t['title']}", "level": "warning"})
                emit({"type": "progress", "section": "playlists",
                      "done": i, "total": len(playlists),
                      "ok": total_ok + ok, "fail": total_fail + fail,
                      "track_done": tracks_done + j + 1, "track_total": track_total,
                      "current": pl["name"]})
                time.sleep(0.1)

            tracks_done += len(to_add)
            removed = len(to_remove)
            emit({"type": "log",
                  "message": f"✅ '{pl['name']}': +{ok} added, -{removed} removed, {fail} failed",
                  "level": "info"})

        else:
            # ── Create new playlist ───────────────────────────────────────────
            # create_playlist() internally calls factory() → UserPlaylist(session, id) → v1 GET.
            # That GET can fail with a race condition for brand-new v2 playlists.
            # We separate the PUT (create) from the GET (access) so a race-condition
            # failure doesn't silently skip track-adding for a playlist that was created.
            playlist_id = None
            try:
                tp_raw = tidal.user.create_playlist(pl["name"], pl["description"])
                playlist_id = tp_raw.id
                emit({"type": "log", "message": f"✔ Created playlist '{pl['name']}' id={playlist_id}", "level": "info"})
            except Exception as e:
                # May be a race-condition ObjectNotFound in factory(); the PUT likely
                # succeeded. Extract the id if we can, otherwise re-list playlists.
                emit({"type": "log", "message": f"⚠️ create_playlist raised: {e}", "level": "warning"})

            if playlist_id is None:
                # Re-list to find the just-created playlist by name
                try:
                    refreshed = {p.name: p for p in tidal.user.playlists()}
                    if pl["name"] in refreshed:
                        tp = refreshed[pl["name"]]
                        playlist_id = tp.id
                    else:
                        emit({"type": "log", "message": f"⚠️ Skipped '{pl['name']}': could not locate after creation", "level": "warning"})
                        continue
                except Exception as e2:
                    emit({"type": "log", "message": f"⚠️ Skipped '{pl['name']}': {e2}", "level": "warning"})
                    continue
            else:
                # Ensure tp is a UserPlaylist; call tracks(limit=1) to confirm
                # accessibility via v1 API and set _etag (needed by add()).
                try:
                    tp = tidalapi.UserPlaylist(tidal, playlist_id)
                except Exception:
                    # v1 propagation delay — wait and retry once
                    time.sleep(1.0)
                    try:
                        tp = tidalapi.UserPlaylist(tidal, playlist_id)
                    except Exception as e:
                        emit({"type": "log", "message": f"⚠️ Could not access '{pl['name']}' after creation: {e}", "level": "warning"})
                        continue

            # Add tracks one at a time (mirrors _transfer_liked approach)
            emit({"type": "log", "message": f"🔗 '{pl['name']}': etag={'set' if tp._etag else 'MISSING'}, num_tracks={tp.num_tracks}", "level": "info"})
            ok = fail = 0
            for j, t in enumerate(pl["tracks"]):
                if cancelled():
                    break
                r = _find_track(tidal, t)
                if r:
                    try:
                        added_ids = tp.add([r.id])
                        if added_ids:
                            ok += 1
                        else:
                            # add() returned empty — Tidal silently skipped this track
                            fail += 1
                            failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                            emit({"type": "log", "message": f"⚠️ Tidal skipped '{t['title']}' (id={r.id}) — duplicate or unavailable in your region", "level": "warning"})
                    except Exception as e:
                        fail += 1
                        failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                        emit({"type": "log", "message": f"⚠️ Could not add '{t['title']}' (id={r.id}): {e}", "level": "warning"})
                else:
                    fail += 1
                    failed.append({"playlist": pl["name"], "title": t["title"], "artist": t["artist"], "album": t["album"]})
                    emit({"type": "log", "message": f"✗ [{pl['name']}] {t['artist']} - {t['title']}", "level": "warning"})
                emit({"type": "progress", "section": "playlists",
                      "done": i, "total": len(playlists),
                      "ok": total_ok + ok, "fail": total_fail + fail,
                      "track_done": tracks_done + j + 1, "track_total": track_total,
                      "current": pl["name"]})
                time.sleep(0.1)

            tracks_done += len(pl["tracks"])
            emit({"type": "log",
                  "message": f"✅ '{pl['name']}': {ok}/{n_spotify} track(s) added, {fail} failed",
                  "level": "info"})

        total_ok += ok
        total_fail += fail
        emit({
            "type": "progress", "section": "playlists",
            "done": i + 1, "total": len(playlists),
            "ok": total_ok, "fail": total_fail,
            "track_done": tracks_done, "track_total": track_total,
            "current": pl["name"],
        })
    return total_ok, total_fail, failed


def _transfer_albums(tidal, albums, emit, cancelled):
    ok = fail = 0
    failed = []
    for i, album in enumerate(albums):
        if cancelled():
            break
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
        if cancelled():
            break
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


# ─── Cleanup endpoints ───────────────────────────────────────────────────────

def _fetch_paged(fetch_fn, limit=100):
    items, offset = [], 0
    while True:
        batch = fetch_fn(limit=limit, offset=offset)
        if not batch:
            break
        items.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
    return items


def _item_to_dict(item):
    if isinstance(item, tidalapi.Track):
        return {"name": item.name, "artist": getattr(item.artist, "name", "Unknown")}
    if isinstance(item, tidalapi.Album):
        return {"name": item.name, "artist": getattr(item.artist, "name", "Unknown")}
    if isinstance(item, tidalapi.Artist):
        return {"name": item.name}
    if isinstance(item, (tidalapi.Playlist, tidalapi.UserPlaylist)):
        return {"name": item.name, "count": getattr(item, "num_tracks", None)}
    return {"name": str(item)}


@app.get("/tidal/cleanup/preview")
async def cleanup_preview(session_id: str, type: str):
    if session_id not in tidal_sessions:
        raise HTTPException(404, "Unknown session")
    tidal = tidal_sessions[session_id]
    if not tidal.check_login():
        raise HTTPException(401, "Tidal session expired")

    loop = asyncio.get_event_loop()

    def fetch():
        if type == "tracks":
            return _fetch_paged(tidal.user.favorites.tracks)
        elif type == "albums":
            return _fetch_paged(tidal.user.favorites.albums)
        elif type == "artists":
            return _fetch_paged(tidal.user.favorites.artists)
        elif type == "playlists":
            return list(tidal.user.playlists())
        else:
            return []

    items = await loop.run_in_executor(executor, fetch)
    return {"items": [_item_to_dict(i) for i in items], "total": len(items)}


class CleanupRequest(BaseModel):
    session_id: str
    type: str  # tracks | albums | artists | playlists
    playlist_names: Optional[list[str]] = None


@app.post("/tidal/cleanup/run")
async def cleanup_run(req: CleanupRequest):
    if req.session_id not in tidal_sessions:
        raise HTTPException(404, "Unknown session")
    tidal = tidal_sessions[req.session_id]
    if not tidal.check_login():
        raise HTTPException(401, "Tidal session expired")

    q: queue.Queue = queue.Queue()

    def emit(data):
        q.put(data)

    def run():
        try:
            if req.type == "tracks":
                items = _fetch_paged(tidal.user.favorites.tracks)
                emit({"type": "log", "message": f"Found {len(items)} favorite track(s)", "level": "info"})
                ok = fail = 0
                for i, t in enumerate(items):
                    try:
                        tidal.user.favorites.remove_track(t.id)
                        ok += 1
                    except Exception:
                        fail += 1
                    emit({"type": "progress", "done": i + 1, "total": len(items), "ok": ok, "fail": fail})
                    time.sleep(0.08)
                emit({"type": "done", "ok": ok, "fail": fail})

            elif req.type == "albums":
                items = _fetch_paged(tidal.user.favorites.albums)
                emit({"type": "log", "message": f"Found {len(items)} saved album(s)", "level": "info"})
                ok = fail = 0
                for i, a in enumerate(items):
                    try:
                        tidal.user.favorites.remove_album(a.id)
                        ok += 1
                    except Exception:
                        fail += 1
                    emit({"type": "progress", "done": i + 1, "total": len(items), "ok": ok, "fail": fail})
                    time.sleep(0.08)
                emit({"type": "done", "ok": ok, "fail": fail})

            elif req.type == "artists":
                items = _fetch_paged(tidal.user.favorites.artists)
                emit({"type": "log", "message": f"Found {len(items)} followed artist(s)", "level": "info"})
                ok = fail = 0
                for i, a in enumerate(items):
                    try:
                        tidal.user.favorites.remove_artist(a.id)
                        ok += 1
                    except Exception:
                        fail += 1
                    emit({"type": "progress", "done": i + 1, "total": len(items), "ok": ok, "fail": fail})
                    time.sleep(0.08)
                emit({"type": "done", "ok": ok, "fail": fail})

            elif req.type == "playlists":
                items = list(tidal.user.playlists())
                if req.playlist_names:
                    name_set = {n.lower() for n in req.playlist_names}
                    items = [p for p in items if p.name.lower() in name_set]
                emit({"type": "log", "message": f"Found {len(items)} playlist(s)", "level": "info"})
                ok = fail = 0
                for i, p in enumerate(items):
                    try:
                        p.delete()
                        ok += 1
                    except Exception:
                        fail += 1
                    emit({"type": "progress", "done": i + 1, "total": len(items), "ok": ok, "fail": fail})
                    time.sleep(0.08)
                emit({"type": "done", "ok": ok, "fail": fail})

            else:
                emit({"type": "error", "message": f"Unknown type: {req.type}"})

        except Exception as e:
            emit({"type": "error", "message": str(e)})
        finally:
            q.put(None)

    executor.submit(run)

    async def stream():
        while True:
            try:
                data = await asyncio.get_event_loop().run_in_executor(None, lambda: q.get(timeout=30))
            except Exception:
                break
            if data is None:
                break
            yield f"data: {json.dumps(data)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"})
