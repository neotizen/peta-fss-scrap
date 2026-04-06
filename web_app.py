from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool

import build_disclosure_pdf as disclosure_pdf


BASE_DIR = Path(__file__).resolve().parent
TEMPLATES = Jinja2Templates(directory=str(BASE_DIR / "templates"))
app = FastAPI(title="DART Disclosure PDF Builder")
JOB_RETENTION_SECONDS = 3600
JOB_OUTPUT_DIR = Path(tempfile.gettempdir()) / "peta-fss-scrap-jobs"
JOB_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CURRENT_MACHINE_ID = os.getenv("FLY_MACHINE_ID")
CURRENT_APP_NAME = os.getenv("FLY_APP_NAME")
MAX_ACTIVE_BACKGROUND_JOBS = 1


@dataclass(slots=True)
class JobRecord:
    job_id: str
    company_name: str
    start_date: str
    end_date: str
    limit_text: str
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    logs: list[str] = field(default_factory=list)
    error_message: str | None = None
    output_name: str | None = None
    output_path: str | None = None
    http_status: int = 202
    cancel_requested: bool = False


class JobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, JobRecord] = {}
        self._pending_cancellations: dict[str, tuple[str, float]] = {}
        self._processes: dict[str, subprocess.Popen[str]] = {}

    @staticmethod
    def _append_log_locked(job: JobRecord, message: str) -> None:
        cleaned = message.strip()
        if not cleaned:
            return
        job.logs.append(cleaned)
        job.updated_at = time.time()

    def _delete_output_file(self, output_path: str | None) -> None:
        if not output_path:
            return
        path = Path(output_path)
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        parent = path.parent
        if parent != JOB_OUTPUT_DIR and JOB_OUTPUT_DIR in parent.parents:
            try:
                parent.rmdir()
            except OSError:
                pass

    def _terminate_process_locked(self, job_id: str) -> None:
        process = self._processes.get(job_id)
        if process is None or process.poll() is not None:
            return
        try:
            if os.name == "nt":
                process.terminate()
            else:
                os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except OSError:
            try:
                process.terminate()
            except OSError:
                pass

    def cleanup_expired_jobs(self) -> None:
        threshold = time.time() - JOB_RETENTION_SECONDS
        with self._lock:
            expired_job_ids = [
                job_id
                for job_id, job in self._jobs.items()
                if job.updated_at < threshold and job.status in {"succeeded", "failed", "cancelled"}
            ]
            for job_id in expired_job_ids:
                job = self._jobs.pop(job_id)
                self._delete_output_file(job.output_path)
                self._processes.pop(job_id, None)
            expired_pending_ids = [
                job_id
                for job_id, (_, created_at) in self._pending_cancellations.items()
                if created_at < threshold
            ]
            for job_id in expired_pending_ids:
                self._pending_cancellations.pop(job_id, None)

    def create_job(
        self,
        *,
        company_name: str,
        start_date: str,
        end_date: str,
        limit_text: str,
        job_id: str | None = None,
    ) -> JobRecord:
        self.cleanup_expired_jobs()
        with self._lock:
            resolved_job_id = (job_id or uuid.uuid4().hex).strip() or uuid.uuid4().hex
            existing = self._jobs.get(resolved_job_id)
            if existing is not None:
                return existing
            pending_cancel = self._pending_cancellations.pop(resolved_job_id, None)
            job = JobRecord(
                job_id=resolved_job_id,
                company_name=company_name,
                start_date=start_date,
                end_date=end_date,
                limit_text=limit_text,
            )
            if pending_cancel is not None:
                message, _ = pending_cancel
                job.status = "cancelled"
                job.http_status = 200
                job.cancel_requested = True
                job.error_message = "작업이 취소되었습니다."
                self._append_log_locked(job, message)
                self._append_log_locked(job, "작업이 취소되었습니다.")
            self._jobs[job.job_id] = job
        return job

    def get_job(self, job_id: str) -> JobRecord | None:
        self.cleanup_expired_jobs()
        with self._lock:
            return self._jobs.get(job_id)

    def append_log(self, job_id: str, message: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            self._append_log_locked(job, message)

    def mark_running(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.cancel_requested or job.status == "cancelled":
                return False
            job.status = "running"
            job.http_status = 202
            job.updated_at = time.time()
            return True

    def mark_succeeded(self, job_id: str, *, output_name: str, output_path: Path) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            self._processes.pop(job_id, None)
            if job.cancel_requested or job.status == "cancelled":
                return False
            job.status = "succeeded"
            job.cancel_requested = False
            job.error_message = None
            job.output_name = output_name
            job.output_path = str(output_path)
            job.http_status = 200
            job.updated_at = time.time()
            return True

    def mark_failed(self, job_id: str, *, error_message: str, http_status: int) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            self._processes.pop(job_id, None)
            if job.status == "cancelled" or (job.cancel_requested and job.status == "cancelling"):
                return False
            job.status = "failed"
            job.error_message = error_message
            job.http_status = http_status
            job.updated_at = time.time()
            return True

    def request_cancel(self, job_id: str, *, message: str) -> JobRecord | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                self._pending_cancellations[job_id] = (message, time.time())
                return None

            if job.status in {"succeeded", "failed", "cancelled"}:
                return job

            if not job.cancel_requested:
                job.cancel_requested = True
                self._append_log_locked(job, message)

            if job.status == "queued":
                job.status = "cancelled"
                job.http_status = 200
                job.error_message = "작업이 취소되었습니다."
                self._append_log_locked(job, "작업이 취소되었습니다.")
            else:
                job.status = "cancelling"
                job.http_status = 202
                job.updated_at = time.time()
                self._terminate_process_locked(job_id)
            return job

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                return job.cancel_requested
            return job_id in self._pending_cancellations

    def mark_cancelled(self, job_id: str, *, message: str = "작업이 취소되었습니다.") -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            self._processes.pop(job_id, None)
            job.cancel_requested = True
            job.status = "cancelled"
            job.error_message = message
            job.output_name = None
            job.output_path = None
            job.http_status = 200
            if not job.logs or job.logs[-1] != message:
                self._append_log_locked(job, message)
            else:
                job.updated_at = time.time()
            return True

    def register_process(self, job_id: str, process: subprocess.Popen[str]) -> None:
        with self._lock:
            self._processes[job_id] = process

    def clear_process(self, job_id: str) -> None:
        with self._lock:
            self._processes.pop(job_id, None)

    def has_other_active_jobs(self, job_id: str) -> bool:
        with self._lock:
            active_count = sum(
                1
                for current_job_id, job in self._jobs.items()
                if current_job_id != job_id and job.status in {"queued", "running", "cancelling"}
            )
            return active_count >= MAX_ACTIVE_BACKGROUND_JOBS


class JobLogStream:
    def __init__(self, job_manager: JobManager, job_id: str) -> None:
        self.job_manager = job_manager
        self.job_id = job_id
        self.buffer = ""

    def write(self, chunk: str) -> int:
        if not chunk:
            return 0
        self.buffer += chunk.replace("\r\n", "\n")
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            self.job_manager.append_log(self.job_id, line)
        return len(chunk)

    def flush(self) -> None:
        if self.buffer:
            self.job_manager.append_log(self.job_id, self.buffer)
            self.buffer = ""


JOB_MANAGER = JobManager()


def parse_limit(limit_text: str) -> int | None:
    normalized = limit_text.strip()
    if not normalized:
        return None
    try:
        limit = int(normalized)
    except ValueError as error:
        raise ValueError("처리 건수는 비워 두거나 1 이상의 정수를 입력해 주세요.") from error
    if limit < 1:
        raise ValueError("처리 건수는 1 이상의 정수여야 합니다.")
    return limit


def validate_form_inputs(company_name: str, start_date: str, end_date: str) -> None:
    if not company_name.strip():
        raise ValueError("회사명을 입력해 주세요.")
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError("조회 시작일 형식은 YYYY-MM-DD여야 합니다.") from error
    try:
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError("조회 종료일 형식은 YYYY-MM-DD여야 합니다.") from error
    if start_date > end_date:
        raise ValueError("조회 종료일은 조회 시작일보다 빠를 수 없습니다.")


def build_download_header(filename: str) -> str:
    fallback_name = "dart-disclosure.pdf"
    return f"attachment; filename={fallback_name}; filename*=UTF-8''{quote(filename)}"


def status_code_for_runtime_error(error: RuntimeError) -> int:
    return 404 if str(error) == "조건에 맞는 공시가 없습니다." else 502


def job_output_path(job_id: str) -> Path:
    return JOB_OUTPUT_DIR / f"{job_id}.pdf"


def job_output_dir(job_id: str) -> Path:
    return JOB_OUTPUT_DIR / job_id


def build_generation_command(
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    limit: int | None,
    output_dir: Path,
    verbose: bool = False,
) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(disclosure_pdf.APP_DIR / "build_disclosure_pdf.py"),
        "--company-name",
        company_name,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--output-dir",
        str(output_dir),
        "--include-attachments",
        "--no-prompt",
    ]
    if limit is not None:
        command.extend(["--limit", str(limit)])
    if verbose:
        command.append("--verbose")
    return command


def parse_completed_output_path(line: str) -> Path | None:
    prefix = "완료: "
    if not line.startswith(prefix):
        return None
    completed_path = line[len(prefix) :].strip()
    if not completed_path:
        return None
    return Path(completed_path)


def extract_error_message(lines: list[str]) -> str:
    filtered = [
        line
        for line in lines
        if line
        and not line.startswith("[HTTP]")
        and not line.startswith("[CACHE]")
        and not line.startswith("저장 파일 업데이트: ")
        and not line.startswith("완료: ")
        and not (line.startswith("[") and "/" in line and "] " in line)
        and not line.startswith("공시 ")
    ]
    message = "\n".join(filtered[-8:]).strip()
    if message:
        return message
    if lines:
        return lines[-1]
    return "PDF 생성 subprocess가 비정상 종료되었습니다."


def run_generation_subprocess_once(
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    limit: int | None,
    output_dir: Path,
    job_id: str | None = None,
    log_consumer: Callable[[str], None] | None = None,
) -> tuple[Path, list[str]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    child_env = os.environ.copy()
    if CURRENT_MACHINE_ID or CURRENT_APP_NAME:
        child_env["DART_HTML_PDF_RENDERER"] = disclosure_pdf.HTML_PDF_RENDERER_FITZ
    popen_kwargs: dict[str, object] = {
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "text": True,
        "bufsize": 1,
        "cwd": str(disclosure_pdf.APP_DIR),
        "env": child_env,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
    else:
        popen_kwargs["start_new_session"] = True

    process = subprocess.Popen(
        build_generation_command(
            company_name=company_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            output_dir=output_dir,
        ),
        **popen_kwargs,
    )
    if job_id is not None:
        JOB_MANAGER.register_process(job_id, process)

    output_lines: list[str] = []
    completed_output_path: Path | None = None
    stream = process.stdout
    if stream is None:
        raise RuntimeError("PDF 생성 subprocess 표준출력을 열지 못했습니다.")

    try:
        for raw_line in stream:
            line = raw_line.rstrip("\r\n")
            if not line:
                continue
            output_lines.append(line)
            if log_consumer is not None:
                log_consumer(line)
            completed_path = parse_completed_output_path(line)
            if completed_path is not None:
                completed_output_path = completed_path

        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(extract_error_message(output_lines))

        if completed_output_path is None:
            generated_outputs = sorted(output_dir.glob("*.pdf"))
            if generated_outputs:
                completed_output_path = generated_outputs[-1]
        if completed_output_path is None or not completed_output_path.exists():
            raise RuntimeError("생성된 PDF 파일을 찾지 못했습니다.")
        return completed_output_path, output_lines
    finally:
        if job_id is not None:
            JOB_MANAGER.clear_process(job_id)


def cancellation_request_message(reason: str | None) -> str:
    if reason == "pagehide":
        return "브라우저 종료/새로고침으로 취소 요청을 받았습니다."
    return "취소 요청을 받았습니다. 현재 단계가 끝나는 대로 작업을 중단합니다."


def build_machine_bound_url(path: str, *, machine_id: str | None) -> str:
    if not machine_id:
        return path
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}machine_id={quote(machine_id)}"


def build_replay_response(target_machine_id: str | None) -> Response | None:
    if not target_machine_id or not CURRENT_MACHINE_ID or target_machine_id == CURRENT_MACHINE_ID:
        return None

    replay_target = f"instance={target_machine_id}"
    if CURRENT_APP_NAME:
        replay_target = f"{replay_target};app={CURRENT_APP_NAME}"

    return Response(
        status_code=409,
        headers={
            "fly-replay": replay_target,
            "Cache-Control": "no-store",
        },
    )


def pending_cancel_payload(job_id: str, *, request_message: str) -> dict[str, object]:
    status_url = build_machine_bound_url(f"/jobs/{job_id}", machine_id=CURRENT_MACHINE_ID)
    cancel_url = build_machine_bound_url(f"/jobs/{job_id}/cancel", machine_id=CURRENT_MACHINE_ID)
    return {
        "job_id": job_id,
        "status": "cancelled",
        "logs": [request_message, "작업이 취소되었습니다."],
        "error_message": "작업이 취소되었습니다.",
        "output_name": None,
        "machine_id": CURRENT_MACHINE_ID,
        "status_url": status_url,
        "download_url": None,
        "cancel_url": cancel_url,
        "http_status": 200,
        "cancel_requested": True,
    }


def job_to_payload(job: JobRecord) -> dict[str, object]:
    status_url = build_machine_bound_url(f"/jobs/{job.job_id}", machine_id=CURRENT_MACHINE_ID)
    cancel_url = (
        build_machine_bound_url(f"/jobs/{job.job_id}/cancel", machine_id=CURRENT_MACHINE_ID)
        if job.status in {"queued", "running", "cancelling"}
        else None
    )
    return {
        "job_id": job.job_id,
        "status": job.status,
        "logs": job.logs,
        "error_message": job.error_message,
        "output_name": job.output_name,
        "machine_id": CURRENT_MACHINE_ID,
        "status_url": status_url,
        "cancel_url": cancel_url,
        "download_url": (
            build_machine_bound_url(f"/jobs/{job.job_id}/download", machine_id=CURRENT_MACHINE_ID)
            if job.status == "succeeded"
            else None
        ),
        "http_status": job.http_status,
        "cancel_requested": job.cancel_requested,
    }


def run_generation_job(
    *,
    job_id: str,
    company_name: str,
    start_date: str,
    end_date: str,
    limit: int | None,
) -> None:
    output_dir = job_output_dir(job_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    if not JOB_MANAGER.mark_running(job_id):
        return
    JOB_MANAGER.append_log(job_id, "공시 목록을 조회하고 있습니다...")
    proxy_message = disclosure_pdf.proxy_status_message()
    if proxy_message:
        JOB_MANAGER.append_log(job_id, proxy_message)

    try:
        completed_output_path, _ = run_generation_subprocess_once(
            company_name=company_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            output_dir=output_dir,
            job_id=job_id,
            log_consumer=lambda line: JOB_MANAGER.append_log(job_id, line),
        )

        if JOB_MANAGER.is_cancel_requested(job_id):
            if completed_output_path is not None:
                completed_output_path.unlink(missing_ok=True)
            JOB_MANAGER.mark_cancelled(job_id)
            return

        if not JOB_MANAGER.mark_succeeded(
            job_id,
            output_name=completed_output_path.name,
            output_path=completed_output_path,
        ):
            completed_output_path.unlink(missing_ok=True)
            JOB_MANAGER.mark_cancelled(job_id)
            return
    except RuntimeError as error:
        if JOB_MANAGER.is_cancel_requested(job_id):
            JOB_MANAGER.mark_cancelled(job_id)
            return
        message = disclosure_pdf.to_user_friendly_error_message(error)
        for line in message.splitlines():
            JOB_MANAGER.append_log(job_id, line)
        JOB_MANAGER.mark_failed(
            job_id,
            error_message=message,
            http_status=status_code_for_runtime_error(error),
        )
    except Exception as error:
        traceback.print_exc()
        if JOB_MANAGER.is_cancel_requested(job_id):
            JOB_MANAGER.mark_cancelled(job_id)
            return
        message = f"서버 내부 오류가 발생했습니다: {error}"
        JOB_MANAGER.append_log(job_id, message)
        JOB_MANAGER.mark_failed(job_id, error_message=message, http_status=500)
    finally:
        JOB_MANAGER.clear_process(job_id)


def render_home_page(
    request: Request,
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    limit: str = "",
    error_message: str | None = None,
    status_code: int = 200,
) -> Response:
    return TEMPLATES.TemplateResponse(
        request,
        "home.html",
        {
            "company_name": company_name,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "error_message": error_message,
            "current_machine_id": CURRENT_MACHINE_ID or "",
        },
        status_code=status_code,
    )


def default_form_values() -> dict[str, str]:
    config = disclosure_pdf.load_app_config()
    return {
        "company_name": config.company_name,
        "start_date": disclosure_pdf.DEFAULT_START_DATE,
        "end_date": disclosure_pdf.DEFAULT_END_DATE,
        "limit": "",
    }


def generate_pdf_bytes(
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    limit: int | None,
) -> tuple[bytes, str]:
    with tempfile.TemporaryDirectory(prefix="dart-generate-") as temp_dir_name:
        output_path, _ = run_generation_subprocess_once(
            company_name=company_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            output_dir=Path(temp_dir_name),
        )
        return output_path.read_bytes(), output_path.name


def start_generation_job(
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    limit_text: str,
    limit: int | None,
    job_id: str | None = None,
) -> JobRecord:
    if job_id:
        existing_job = JOB_MANAGER.get_job(job_id)
        if existing_job is not None:
            return existing_job
    job = JOB_MANAGER.create_job(
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        limit_text=limit_text,
        job_id=job_id,
    )
    if job.status == "cancelled":
        return job
    if JOB_MANAGER.has_other_active_jobs(job.job_id):
        JOB_MANAGER.mark_failed(
            job.job_id,
            error_message="현재 다른 PDF 생성 작업이 진행 중입니다. 완료되거나 취소된 뒤 다시 시도해 주세요.",
            http_status=429,
        )
        snapshot = JOB_MANAGER.get_job(job.job_id)
        assert snapshot is not None
        return snapshot
    JOB_MANAGER.append_log(job.job_id, "작업을 생성했습니다.")
    thread = threading.Thread(
        target=run_generation_job,
        kwargs={
            "job_id": job.job_id,
            "company_name": company_name,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
        },
        daemon=True,
    )
    thread.start()
    return job


@app.get("/")
async def home(request: Request) -> Response:
    return render_home_page(request, **default_form_values())


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"ok": True, "proxy_configured": bool(disclosure_pdf.active_proxy_map())})


@app.post("/jobs")
async def create_job(
    company_name: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    limit: str = Form(""),
    job_id: str | None = Form(None),
    machine_id: str | None = None,
) -> JSONResponse:
    replay_response = build_replay_response(machine_id)
    if replay_response is not None:
        return replay_response

    try:
        validate_form_inputs(company_name, start_date, end_date)
        parsed_limit = parse_limit(limit)
        job = start_generation_job(
            company_name=company_name.strip(),
            start_date=start_date,
            end_date=end_date,
            limit_text=limit,
            limit=parsed_limit,
            job_id=job_id,
        )
    except ValueError as error:
        return JSONResponse({"error_message": str(error)}, status_code=400)

    snapshot = JOB_MANAGER.get_job(job.job_id)
    assert snapshot is not None
    return JSONResponse(job_to_payload(snapshot), status_code=snapshot.http_status)


@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str, machine_id: str | None = None) -> Response:
    replay_response = build_replay_response(machine_id)
    if replay_response is not None:
        return replay_response

    job = JOB_MANAGER.get_job(job_id)
    if job is None:
        return JSONResponse({"error_message": "작업을 찾지 못했습니다."}, status_code=404)
    return JSONResponse(job_to_payload(job), status_code=200)


@app.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: str,
    machine_id: str | None = None,
    reason: str | None = None,
) -> Response:
    replay_response = build_replay_response(machine_id)
    if replay_response is not None:
        return replay_response

    request_message = cancellation_request_message(reason)
    snapshot = JOB_MANAGER.request_cancel(
        job_id,
        message=request_message,
    )
    if snapshot is None:
        return JSONResponse(pending_cancel_payload(job_id, request_message=request_message), status_code=202)
    return JSONResponse(job_to_payload(snapshot), status_code=200)


@app.get("/jobs/{job_id}/download")
async def download_job_result(job_id: str, machine_id: str | None = None) -> Response:
    replay_response = build_replay_response(machine_id)
    if replay_response is not None:
        return replay_response

    job = JOB_MANAGER.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="작업을 찾지 못했습니다.")
    if job.status != "succeeded":
        raise HTTPException(status_code=409, detail="아직 PDF가 준비되지 않았습니다.")
    if not job.output_path or not Path(job.output_path).exists():
        raise HTTPException(status_code=410, detail="다운로드 가능한 결과 파일이 없습니다.")

    return FileResponse(
        job.output_path,
        media_type="application/pdf",
        headers={
            "Content-Disposition": build_download_header(job.output_name or "dart-disclosure.pdf"),
            "Cache-Control": "no-store",
        },
    )


@app.post("/generate")
async def generate(
    request: Request,
    company_name: str = Form(...),
    start_date: str = Form(...),
    end_date: str = Form(...),
    limit: str = Form(""),
) -> Response:
    form_values = {
        "company_name": company_name,
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
    }
    try:
        validate_form_inputs(company_name, start_date, end_date)
        parsed_limit = parse_limit(limit)
        pdf_bytes, output_name = await run_in_threadpool(
            generate_pdf_bytes,
            company_name=company_name.strip(),
            start_date=start_date,
            end_date=end_date,
            limit=parsed_limit,
        )
    except ValueError as error:
        return render_home_page(
            request,
            **form_values,
            error_message=str(error),
            status_code=400,
        )
    except RuntimeError as error:
        status_code = status_code_for_runtime_error(error)
        return render_home_page(
            request,
            **form_values,
            error_message=disclosure_pdf.to_user_friendly_error_message(error),
            status_code=status_code,
        )

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": build_download_header(output_name),
            "Cache-Control": "no-store",
        },
    )
