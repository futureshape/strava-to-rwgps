#!/usr/bin/env python3
"""Upload cycling activity files to RideWithGPS as trips using CSV metadata.

Features:
  * Authenticates with RideWithGPS (email/password -> auth_token) using API key
  * Parses `activities.csv` for Activity ID, Name, Description, Media
  * Uploads each FIT/GPX(/gz) file found under `activities/cycling`
  * Sets trip name & description from CSV
  * Skips already uploaded activities (tracked in `.uploaded_rwgps.log`)
  * Dry-run mode to preview actions
  * Graceful error handling & progress summary

Media Upload:
  RWGPS public docs for photo/media upload are not clearly published. A stub
  is included. Provide endpoint/parameter details and the `upload_media_for_trip`
  function can be completed. Currently it logs intended uploads.

Environment variables (can be placed in a `.env` file):
  RWGPS_API_KEY (required)
  RWGPS_EMAIL (required if RWGPS_AUTH_TOKEN not supplied)
  RWGPS_PASSWORD (required if RWGPS_AUTH_TOKEN not supplied)
  RWGPS_AUTH_TOKEN (optional cache; script will refresh if missing/invalid)
    RWGPS_VERSION (default 2)
    RWGPS_PHOTO_VERSION (default 3 for /photos.json endpoint)
  RWGPS_BASE_URL (default https://ridewithgps.com)
  RWGPS_DRY_RUN (true/false)

Usage examples:
  python upload_rwgps.py               # normal run
  python upload_rwgps.py --dry-run     # no network mutations
  python upload_rwgps.py --only 123,456

"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Iterable

import requests
from dotenv import load_dotenv
import mimetypes


# Helper must be defined before it's used inside RWGPSClient methods when script executes main immediately.
def _guess_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or 'application/octet-stream'


# ---------- Configuration ----------
PROJECT_ROOT = Path(__file__).parent
CSV_PATH = PROJECT_ROOT / "activities.csv"
CYCLING_DIR = PROJECT_ROOT / "activities" / "cycling"
UPLOADED_LOG = PROJECT_ROOT / ".uploaded_rwgps.log"

# Sentinel values for special outcomes
DUPLICATE_TRIP = -2  # queued task indicated duplicate; treat as skipped


@dataclass
class ActivityMeta:
    activity_id: str
    name: str
    description: str
    media_paths: List[Path]


class RWGPSClient:
    def __init__(self, api_key: str, base_url: str, version: str = "2", auth_token: Optional[str] = None,
                 email: Optional[str] = None, password: Optional[str] = None, dry_run: bool = False,
                 photo_version: str = "3", poll_debug: bool = False):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.version = version
        self.photo_version = photo_version  # photos API seems to require v3 per captured request
        self.auth_token = auth_token
        self.email = email
        self.password = password
        self.dry_run = dry_run
        self.poll_debug = poll_debug

    # ---- Authentication ----
    def ensure_auth(self):
        if self.auth_token:
            return
        if not (self.email and self.password):
            raise RuntimeError("Email/password required to obtain auth_token.")
        url = f"{self.base_url}/users/current.json"
        params = {
            "email": self.email,
            "password": self.password,
            "apikey": self.api_key,
            "version": self.version,
        }
        if self.dry_run:
            print(f"[DRY-RUN] Would authenticate: GET {url} (email={self.email})")
            self.auth_token = "DUMMY_TOKEN"
            return
        r = requests.get(url, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Auth failed {r.status_code}: {r.text[:500]}")
        data = r.json()
        self.auth_token = data.get("user", {}).get("auth_token")
        if not self.auth_token:
            raise RuntimeError("Auth token not found in response")

    # ---- Trip Upload ----
    def upload_trip_from_file(self, file_path: Path, name: str, description: str,
                               poll: bool = True, poll_interval: float = 2.0,
                               poll_timeout: float = 300.0) -> Optional[int]:
        """Upload a FIT/GPX file and poll queued task until trip is created.

        RideWithGPS returns a task_id (no direct trip id). We poll
        /queued_tasks/status.json until the associated trip object appears
        or timeout/failure occurs. Returns trip ID or None.
        """
        self.ensure_auth()
        url = f"{self.base_url}/trips.json"
        params = {
            "apikey": self.api_key,
            "auth_token": self.auth_token,
            "version": self.version,
        }
        # Attempt field names for trip attributes. Adjust if API returns validation errors.
        data = {
            "trip[name]": name,
            "trip[description]": description,
        }
        # Support gz if needed (server likely handles) else decompress and send plain file.
        send_path = file_path
        temp_bytes = None
        if file_path.suffix == '.gz':
            with gzip.open(file_path, 'rb') as f:
                temp_bytes = f.read()
            files = {"file": (file_path.stem, temp_bytes)}
        else:
            files = {"file": (file_path.name, open(file_path, 'rb'))}

        if self.dry_run:
            print(f"[DRY-RUN] Would POST {url} name='{name}' description len={len(description)} file={file_path.name}")
            return -1
        try:
            r = requests.post(url, params=params, data=data, files=files, timeout=300)
        finally:
            # Close file handle if not bytes
            fh = files["file"][1]
            try:
                close_meth = getattr(fh, 'close', None)
                if callable(close_meth):
                    close_meth()
            except Exception:
                pass
        if r.status_code not in (200, 201, 202):
            print(f"Upload failed for {file_path.name}: {r.status_code} {r.text[:300]}")
            return None
        try:
            resp = r.json()
        except Exception:
            print(f"Unexpected non-JSON response for {file_path.name}: {r.text[:300]}")
            return None

        task_id = resp.get('task_id') if isinstance(resp, dict) else None
        if task_id and poll:
            print(f"Upload queued for {file_path.name}, task_id={task_id}; polling for trip id...")
            return self.poll_task_for_trip(task_id, poll_interval, poll_timeout, file_path.name)
        print(f"No task id returned for {file_path.name}. Response keys: {list(resp.keys()) if isinstance(resp, dict) else 'unknown'}")
        return None

    def poll_task_for_trip(self, task_id: int, interval: float, timeout: float, label: str) -> Optional[int]:
        """Poll queued_tasks/status.json for a resulting trip ID."""
        deadline = time.time() + timeout
        status_url = f"{self.base_url}/queued_tasks/status.json"
        params_base = {
            "ids": str(task_id),
            "include_objects": "true",
            "apikey": self.api_key,
            "auth_token": self.auth_token,
            "version": self.version,
        }
        last_status = None
        poll_count = 0
        while time.time() < deadline:
            if self.dry_run:
                print(f"[DRY-RUN] Would poll {status_url}?ids={task_id}")
                return -1
            poll_count += 1
            if self.poll_debug:
                # Mask token for display
                token_masked = None
                if self.auth_token:
                    token_masked = self.auth_token[:6] + "..." + self.auth_token[-4:]
                print(f"[POLL {poll_count}] GET {status_url} params={{ids:{task_id}, include_objects:true, apikey:***, auth_token:{token_masked}, version:{self.version}}}")
            try:
                r = requests.get(status_url, params=params_base, timeout=30)
            except Exception as e:
                print(f"Polling error task {task_id}: {e}")
                time.sleep(interval)
                continue
            if r.status_code != 200:
                print(f"Polling non-200 for task {task_id}: {r.status_code} {r.text[:180]}")
                time.sleep(interval)
                continue
            if self.poll_debug:
                snippet = r.text[:250].replace('\n', ' ')
                print(f"[POLL {poll_count}] status={r.status_code} body_snippet={snippet}")
            try:
                data = r.json()
            except Exception:
                print(f"Polling JSON parse error task {task_id}: {r.text[:180]}")
                time.sleep(interval)
                continue
            qtasks = data.get('queued_tasks') or []
            if not qtasks:
                if self.poll_debug:
                    print(f"[POLL {poll_count}] No queued_tasks array yet")
                time.sleep(interval)
                continue
            task = qtasks[0]
            last_status = task.get('status')
            response_code = task.get('response_code')
            if self.poll_debug:
                print(f"[POLL {poll_count}] task_status={last_status} response_code={response_code} message={task.get('message')} progress={task.get('progress')}")
            if response_code == 'success':
                # find trip object
                for obj in task.get('associated_objects', []):
                    if obj.get('type') == 'trip':
                        trip = obj.get('trip') or {}
                        trip_id = trip.get('id')
                        if trip_id:
                            print(f"Task {task_id} complete -> trip_id={trip_id} ({label})")
                            return trip_id
                print(f"Task {task_id} success but no trip found yet; continuing...")
            elif response_code == 'duplicate':
                print(f"Task {task_id} marked duplicate; skipping upload for {label}.")
                return DUPLICATE_TRIP
            elif response_code in ('error', 'failed'):
                print(f"Task {task_id} failed: {task.get('message')}")
                return None
            else:
                # Still processing
                pass
            time.sleep(interval)
        print(f"Timed out waiting for task {task_id} (last status={last_status})")
        return None

    def upload_photo(self, trip_id: int, photo_path: Path) -> bool:
        """Upload a single photo to a trip.

        Inferred from provided curl:
          POST {base}/photos.json
          multipart form fields:
            file        -> binary image (filename preserved)
            parent_type -> 'trip'
            parent_id   -> trip id
          Headers observed: x-rwgps-api-key, x-rwgps-api-version (3)

        Returns True on (200/201/202), else False.
        """
        if not photo_path.exists():
            print(f"Photo missing, skipping: {photo_path}")
            return False
        self.ensure_auth()
        url = f"{self.base_url}/photos.json"
        headers = {
            "x-rwgps-api-key": self.api_key,
            "x-rwgps-api-version": self.photo_version,
            "Accept": "application/json",
        }
        params = {
            "apikey": self.api_key,
            "auth_token": self.auth_token,
        }
        mime = _guess_mime(photo_path)
        files = {"file": (photo_path.name, open(photo_path, 'rb'), mime)}
        data = {"parent_type": "trip", "parent_id": str(trip_id)}
        if self.dry_run:
            print(f"[DRY-RUN] Would POST {url} photo={photo_path.name} -> trip {trip_id}")
            return True
        try:
            r = requests.post(url, headers=headers, params=params, data=data, files=files, timeout=120)
        finally:
            fh = files["file"][1]
            try:
                fh.close()
            except Exception:
                pass
        if r.status_code not in (200, 201, 202):
            snippet = r.text[:200].replace('\n', ' ')
            print(f"Photo upload failed ({r.status_code}) {photo_path.name}: {snippet}")
            return False
        print(f"Uploaded photo {photo_path.name} -> trip {trip_id}")
        return True

    def upload_media_for_trip(self, trip_id: int, media_files: Iterable[Path]):
        media_list = list(media_files)
        if not media_list:
            return
        print(f"Uploading {len(media_list)} photos for trip {trip_id}...")
        success = 0
        for m in media_list:
            try:
                if self.upload_photo(trip_id, m):
                    success += 1
            except Exception as e:
                print(f"Error uploading photo {m}: {e}")
        print(f"Photos uploaded: {success}/{len(media_list)}")


# ---------- CSV Parsing ----------
def load_activity_metadata(csv_path: Path, media_base: Path) -> Dict[str, ActivityMeta]:
    """Build a lookup of metadata by both Activity ID and file-based ID.

    The CSV 'Filename' column may contain compressed extensions (e.g. .fit.gz)
    while actual files on disk are uncompressed (.fit). We index both the
    Activity ID and the stripped filename stem (without extensions) so the
    script can match either form.
    """
    if not csv_path.exists():
        raise FileNotFoundError(csv_path)
    mapping: Dict[str, ActivityMeta] = {}
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            act_id = (row.get('Activity ID') or '').strip()
            if not act_id:
                continue
            name = (row.get('Activity Name') or '').strip()
            description = (row.get('Activity Description') or '').strip()
            media_field = (row.get('Media') or '').strip()
            media_paths: List[Path] = []
            if media_field:
                for part in media_field.split('|'):
                    p = media_base / part
                    media_paths.append(p)

            meta = ActivityMeta(
                activity_id=act_id,
                name=name or f"Activity {act_id}",
                description=description,
                media_paths=media_paths,
            )
            # Index by activity id
            mapping.setdefault(act_id, meta)
            # Index by filename-derived id
            filename = (row.get('Filename') or '').strip()
            if filename:
                # Extract base name (remove directories)
                base = Path(filename).name
                # Remove up to two extensions (.fit.gz -> base id)
                for ext in ('.fit.gz', '.gpx.gz', '.tcx.gz', '.fit', '.gpx', '.tcx'):
                    if base.endswith(ext):
                        file_id = base[:-len(ext)]
                        if file_id:
                            mapping.setdefault(file_id, meta)
                        break
    return mapping


def load_uploaded_set() -> set[str]:
    if not UPLOADED_LOG.exists():
        return set()
    return {line.strip() for line in UPLOADED_LOG.read_text().splitlines() if line.strip()}


def append_uploaded(activity_id: str):
    with open(UPLOADED_LOG, 'a', encoding='utf-8') as f:
        f.write(activity_id + '\n')


# ---------- Main Logic ----------
def infer_activity_id_from_filename(filename: str) -> str:
    base = filename
    # Strip double extensions like .fit.gz or .gpx.gz
    for ext in ('.fit.gz', '.gpx.gz', '.tcx.gz'):
        if base.endswith(ext):
            return base[:-len(ext)]
    for ext in ('.fit', '.gpx', '.tcx'):
        if base.endswith(ext):
            return base[:-len(ext)]
    return base


def discover_activity_files(directory: Path) -> List[Path]:
    exts = {'.fit', '.gpx', '.tcx', '.gz'}
    files = []
    if not directory.exists():
        print(f"Directory missing: {directory}")
        return files
    for p in sorted(directory.iterdir()):
        if p.is_file() and (p.suffix in exts or any(str(p.name).endswith(suf) for suf in ('.fit.gz', '.gpx.gz', '.tcx.gz'))):
            files.append(p)
    return files


def parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).lower() in {'1', 'true', 'yes', 'on'}


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Upload cycling activities to RideWithGPS")
    parser.add_argument('--dry-run', action='store_true', help='Do not perform network mutations.')
    parser.add_argument('--only', help='Comma-separated list of activity IDs to restrict uploads.')
    parser.add_argument('--force', action='store_true', help='Re-upload even if already logged as uploaded.')
    parser.add_argument('--poll-interval', type=float, default=float(os.getenv('RWGPS_TASK_POLL_INTERVAL', '2.0')),
                        help='Seconds between queued task polls (default env RWGPS_TASK_POLL_INTERVAL or 2.0).')
    parser.add_argument('--poll-timeout', type=float, default=float(os.getenv('RWGPS_TASK_POLL_TIMEOUT', '300')),
                        help='Max seconds to wait for queued task (default env RWGPS_TASK_POLL_TIMEOUT or 300).')
    parser.add_argument('--poll-debug', action='store_true', help='Verbose debug output for queued task polling.')
    args = parser.parse_args()

    dry_env = parse_bool(os.getenv('RWGPS_DRY_RUN')) or False
    dry_run = args.dry_run or dry_env

    api_key = os.getenv('RWGPS_API_KEY') or ''
    email = os.getenv('RWGPS_EMAIL') or None
    password = os.getenv('RWGPS_PASSWORD') or None
    auth_token = os.getenv('RWGPS_AUTH_TOKEN') or None
    version = os.getenv('RWGPS_VERSION', '2')
    photo_version = os.getenv('RWGPS_PHOTO_VERSION', '3')
    base_url = os.getenv('RWGPS_BASE_URL', 'https://ridewithgps.com')

    if not api_key:
        print('ERROR: RWGPS_API_KEY must be set (env or .env file).')
        return 2

    client = RWGPSClient(api_key=api_key, base_url=base_url, version=version, auth_token=auth_token,
                         email=email, password=password, dry_run=dry_run, photo_version=photo_version,
                         poll_debug=args.poll_debug)

    activity_meta = load_activity_metadata(CSV_PATH, PROJECT_ROOT)
    print(f"Loaded metadata for {len(activity_meta)} activities from CSV.")

    target_ids = None
    if args.only:
        target_ids = {x.strip() for x in args.only.split(',') if x.strip()}
        print(f"Restricting to {len(target_ids)} specified activities.")

    already_uploaded = load_uploaded_set()
    print(f"Already uploaded: {len(already_uploaded)}")

    files = discover_activity_files(CYCLING_DIR)
    if not files:
        print("No activity files found to process.")
        return 0
    print(f"Found {len(files)} potential files in {CYCLING_DIR}.")

    successes = 0
    skipped = 0
    errors = 0
    for fpath in files:
        act_id = infer_activity_id_from_filename(fpath.name)
        if target_ids and act_id not in target_ids:
            continue
        if (not args.force) and (act_id in already_uploaded):
            print(f"Skip {fpath.name} (already uploaded)")
            skipped += 1
            continue
        meta = activity_meta.get(act_id)
        if not meta:
            print(f"No CSV metadata for {act_id}, skipping file {fpath.name}")
            skipped += 1
            continue
        try:
            trip_id = client.upload_trip_from_file(
                fpath,
                meta.name,
                meta.description,
                poll=True,
                poll_interval=args.poll_interval,
                poll_timeout=args.poll_timeout,
            )
        except Exception as e:
            print(f"Error uploading {fpath.name}: {e}")
            errors += 1
            continue
        if trip_id == DUPLICATE_TRIP:
            print(f"Duplicate detected for {fpath.name}; skipping media upload.")
            # Consider the activity handled to avoid future attempts
            if not dry_run:
                append_uploaded(act_id)
            skipped += 1
        elif trip_id is not None:
            client.upload_media_for_trip(trip_id, [p for p in meta.media_paths if p.exists()])
            if not dry_run and trip_id != -1:
                append_uploaded(act_id)
            successes += 1
        else:
            errors += 1

    print("""\nSummary:
  Successes: {s}
  Skipped: {sk}
  Errors: {er}""".format(s=successes, sk=skipped, er=errors))
    return 0 if errors == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
