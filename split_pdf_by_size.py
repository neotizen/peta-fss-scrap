#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Iterable

try:
    from pypdf import PdfReader, PdfWriter
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    PdfReader = None
    PdfWriter = None

try:
    import fitz
except ModuleNotFoundError:  # pragma: no cover - fallback import
    fitz = None


DEFAULT_SPLIT_SIZE_MB = 199.0
PART_SUFFIX_RE = re.compile(r"-part\d{3}\.pdf$", re.IGNORECASE)
MB_BYTES = 1_000_000


def normalize_prompt_path(raw: str) -> str:
    cleaned = raw.strip()
    if cleaned[:1] in {"'", '"'}:
        cleaned = cleaned[1:]
    if cleaned[-1:] in {"'", '"'}:
        cleaned = cleaned[:-1]
    return cleaned.strip()


def parse_split_size_mb(raw: str | None, default_mb: float = DEFAULT_SPLIT_SIZE_MB) -> float:
    if raw is None:
        return default_mb

    cleaned = normalize_prompt_path(raw)
    if cleaned == "":
        return default_mb

    try:
        value = float(cleaned)
    except ValueError as exc:
        raise ValueError(f"잘못된 분할 크기입니다: {raw}") from exc

    if value <= 0:
        raise ValueError(f"분할 크기는 0보다 커야 합니다: {raw}")
    return value


def make_part_output_path(source_pdf: Path, part_number: int, output_dir: Path | None = None) -> Path:
    target_dir = output_dir or source_pdf.parent
    return target_dir / f"{source_pdf.stem}-part{part_number:03d}{source_pdf.suffix}"


def estimate_writer_size_bytes(writer: PdfWriter) -> int:
    bio = io.BytesIO()
    writer.write(bio)
    return bio.tell()


def build_writer_for_range(reader: PdfReader, start_page: int, end_page: int) -> PdfWriter:
    writer = PdfWriter()
    for page_number in range(start_page, end_page + 1):
        writer.add_page(reader.pages[page_number])
    return writer


def estimate_range_size_bytes(reader: PdfReader, start_page: int, end_page: int) -> int:
    writer = build_writer_for_range(reader, start_page, end_page)
    return estimate_writer_size_bytes(writer)


def save_range_to_pdf(reader: PdfReader, start_page: int, end_page: int, output_pdf: Path) -> int:
    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    writer = build_writer_for_range(reader, start_page, end_page)
    with output_pdf.open("wb") as file_obj:
        writer.write(file_obj)
    return output_pdf.stat().st_size


def save_bytes_atomic(output_path: Path, data: bytes) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=output_path.parent,
            prefix=f"{output_path.stem}-",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_output_path = Path(temp_file.name)
            temp_file.write(data)
        os.replace(temp_output_path, output_path)
        temp_output_path = None
        return output_path.stat().st_size
    finally:
        if temp_output_path is not None:
            temp_output_path.unlink(missing_ok=True)


def build_fitz_range_bytes(source_doc: fitz.Document, start_page: int, end_page: int) -> bytes:
    part_doc = fitz.open()
    try:
        part_doc.insert_pdf(source_doc, from_page=start_page, to_page=end_page)
        return part_doc.tobytes(
            garbage=4,
            deflate=True,
            clean=True,
        )
    finally:
        part_doc.close()


def cleanup_existing_parts(source_pdf: Path, output_dir: Path | None = None) -> None:
    target_dir = output_dir or source_pdf.parent
    for part_path in sorted(target_dir.glob(f"{source_pdf.stem}-part*.pdf")):
        if part_path.is_file():
            part_path.unlink()


def is_split_artifact(path: Path) -> bool:
    return bool(PART_SUFFIX_RE.search(path.name))


def collect_target_pdfs(target: Path, recursive: bool) -> list[Path]:
    if not target.exists():
        raise RuntimeError(f"입력 경로를 찾을 수 없습니다: {target}")

    if target.is_file():
        if target.suffix.lower() != ".pdf":
            raise RuntimeError(f"PDF 파일만 처리할 수 있습니다: {target}")
        return [target]

    if not target.is_dir():
        raise RuntimeError(f"파일 또는 폴더 경로를 입력해 주세요: {target}")

    iterator = target.rglob("*.pdf") if recursive else target.glob("*.pdf")
    pdfs = [path for path in iterator if path.is_file() and not is_split_artifact(path)]
    return sorted(pdfs, key=lambda item: str(item).lower())


def ensure_pdf_backend() -> str:
    if PdfReader is not None and PdfWriter is not None:
        return "pypdf"
    if fitz is not None:
        return "fitz"
    raise RuntimeError(
        "PDF 분할에 필요한 라이브러리를 찾지 못했습니다. "
        "pypdf 또는 PyMuPDF(fitz)가 필요합니다."
    )


def ensure_unique_output_stems(targets: list[Path], output_dir: Path | None) -> None:
    if output_dir is None or len(targets) < 2:
        return

    seen: dict[str, Path] = {}
    duplicates: list[str] = []
    for target in targets:
        key = target.stem.lower()
        other = seen.get(key)
        if other is None:
            seen[key] = target
            continue
        duplicates.append(f"{other.name} / {target.name}")

    if duplicates:
        joined = ", ".join(duplicates[:5])
        raise RuntimeError(
            "같은 출력 폴더에 저장하면 분할 파일명이 충돌할 수 있습니다: "
            f"{joined}"
        )


def split_pdf_by_size_pypdf(
    source_pdf: Path,
    *,
    max_bytes: int,
    output_dir: Path | None = None,
) -> list[Path]:
    reader = PdfReader(str(source_pdf), strict=False)
    if reader.is_encrypted and reader.decrypt("") == 0:
        raise RuntimeError(f"암호화된 PDF를 열 수 없습니다: {source_pdf}")

    parts: list[Path] = []
    size_cache: dict[tuple[int, int], int] = {}
    total_pages = len(reader.pages)
    start_page = 0
    part_number = 1

    def cached_size(end_page: int) -> int:
        key = (start_page, end_page)
        if key not in size_cache:
            size_cache[key] = estimate_range_size_bytes(reader, start_page, end_page)
        return size_cache[key]

    while start_page < total_pages:
        low = start_page
        high = total_pages - 1
        best_end = start_page
        best_size = cached_size(start_page)

        while low <= high:
            mid = (low + high) // 2
            size = cached_size(mid)
            if size <= max_bytes:
                best_end = mid
                best_size = size
                low = mid + 1
            else:
                high = mid - 1

        part_end = best_end
        part_path = make_part_output_path(source_pdf, part_number, output_dir)
        part_size = save_range_to_pdf(reader, start_page, part_end, part_path)
        parts.append(part_path)

        if part_end == start_page and best_size > max_bytes:
            print(
                f"WARN\t{source_pdf.name}\t단일 파트가 기준 초과\t"
                f"{part_path.name}\t{part_size / MB_BYTES:.2f}MB"
            )

        start_page = part_end + 1
        part_number += 1

    return parts


def split_pdf_by_size_fitz(
    source_pdf: Path,
    *,
    max_bytes: int,
    output_dir: Path | None = None,
) -> list[Path]:
    with fitz.open(source_pdf) as source_doc:
        total_pages = source_doc.page_count
        size_cache: dict[tuple[int, int], int] = {}
        parts: list[Path] = []
        start_page = 0
        part_number = 1

        def cached_size(end_page: int) -> int:
            key = (start_page, end_page)
            if key not in size_cache:
                size_cache[key] = len(build_fitz_range_bytes(source_doc, start_page, end_page))
            return size_cache[key]

        while start_page < total_pages:
            low = start_page
            high = total_pages - 1
            best_end = start_page
            best_bytes = build_fitz_range_bytes(source_doc, start_page, start_page)

            while low <= high:
                mid = (low + high) // 2
                candidate_bytes = build_fitz_range_bytes(source_doc, start_page, mid)
                size_cache[(start_page, mid)] = len(candidate_bytes)
                if len(candidate_bytes) <= max_bytes:
                    best_end = mid
                    best_bytes = candidate_bytes
                    low = mid + 1
                else:
                    high = mid - 1

            part_path = make_part_output_path(source_pdf, part_number, output_dir)
            part_size = save_bytes_atomic(part_path, best_bytes)
            parts.append(part_path)

            if best_end == start_page and part_size > max_bytes:
                print(
                    f"WARN\t{source_pdf.name}\t단일 파트가 기준 초과\t"
                    f"{part_path.name}\t{part_size / MB_BYTES:.2f}MB"
                )

            start_page = best_end + 1
            part_number += 1

    return parts


def split_pdf_by_size(
    source_pdf: Path,
    max_size_mb: float = DEFAULT_SPLIT_SIZE_MB,
    output_dir: Path | None = None,
) -> list[Path]:
    source_pdf = source_pdf.expanduser().resolve()
    if not source_pdf.exists():
        raise RuntimeError(f"입력 PDF를 찾을 수 없습니다: {source_pdf}")
    if not source_pdf.is_file():
        raise RuntimeError(f"입력 경로가 파일이 아닙니다: {source_pdf}")
    if source_pdf.suffix.lower() != ".pdf":
        raise RuntimeError(f"PDF 파일만 처리할 수 있습니다: {source_pdf}")

    target_output_dir = output_dir.expanduser().resolve() if output_dir is not None else None
    max_bytes = int(max_size_mb * MB_BYTES)
    original_size = source_pdf.stat().st_size
    if original_size <= max_bytes:
        return []

    cleanup_existing_parts(source_pdf, target_output_dir)
    backend = ensure_pdf_backend()
    if backend == "pypdf":
        return split_pdf_by_size_pypdf(
            source_pdf,
            max_bytes=max_bytes,
            output_dir=target_output_dir,
        )
    return split_pdf_by_size_fitz(
        source_pdf,
        max_bytes=max_bytes,
        output_dir=target_output_dir,
    )


def split_pdf_file(
    source_pdf: Path,
    *,
    max_mb: float = DEFAULT_SPLIT_SIZE_MB,
    output_dir: Path | None = None,
) -> list[Path]:
    return split_pdf_by_size(
        Path(source_pdf),
        max_size_mb=max_mb,
        output_dir=output_dir,
    )


def prompt_path(prompt: str) -> Path:
    while True:
        raw = input(f"{prompt}: ").strip()
        if raw == "":
            print("경로를 입력해 주세요.")
            continue
        return Path(normalize_prompt_path(raw)).expanduser().resolve()


def prompt_split_size_mb(default_mb: float = DEFAULT_SPLIT_SIZE_MB) -> float:
    while True:
        raw = input(f"PDF 분할 크기(MB) [Enter={int(default_mb)}]: ").strip()
        try:
            return parse_split_size_mb(raw, default_mb=default_mb)
        except ValueError as exc:
            print(str(exc))


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="생성된 PDF를 파일 크기 기준으로 후처리 분할합니다."
    )
    parser.add_argument("input_path", nargs="?", default=None, help="대상 PDF 파일 또는 폴더")
    parser.add_argument("--input", dest="input_option", type=Path, default=None, help="대상 PDF 파일 또는 폴더")
    parser.add_argument(
        "--size-mb",
        "--max-mb",
        dest="size_mb",
        default=None,
        help="분할 기준 크기(MB). 기본값은 199",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="분할 파일 저장 폴더 (기본값: 원본 PDF와 같은 폴더)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="입력 경로가 폴더일 때 하위 폴더까지 재귀적으로 처리",
    )
    parser.add_argument(
        "--non-interactive",
        "--no-prompt",
        dest="non_interactive",
        action="store_true",
        help="필수 값 누락 시 프롬프트를 띄우지 않고 실패",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def resolve_args(args: argparse.Namespace) -> argparse.Namespace:
    interactive = not args.non_interactive and sys.stdin.isatty()
    raw_target = args.input_option if args.input_option is not None else args.input_path

    if raw_target is not None:
        args.input = Path(raw_target).expanduser().resolve()
    elif interactive:
        args.input = prompt_path("분할할 PDF 파일 또는 폴더 경로")
    else:
        raise RuntimeError("--input 이 필요합니다.")

    if args.size_mb is not None:
        args.size_mb = parse_split_size_mb(args.size_mb)
    elif interactive:
        args.size_mb = prompt_split_size_mb()
    else:
        args.size_mb = parse_split_size_mb(None)

    if args.output_dir is not None:
        args.output_dir = args.output_dir.expanduser().resolve()
    return args


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        args = resolve_args(args)
        targets = collect_target_pdfs(args.input, recursive=args.recursive)
        if not targets:
            raise RuntimeError(f"처리할 PDF가 없습니다: {args.input}")
        ensure_unique_output_stems(targets, args.output_dir)

        print(f"TARGET\t{args.input}")
        print(f"SIZE\t{args.size_mb:g}MB")

        split_count = 0
        skipped_count = 0
        error_count = 0

        for pdf_path in targets:
            try:
                parts = split_pdf_by_size(
                    pdf_path,
                    max_size_mb=args.size_mb,
                    output_dir=args.output_dir,
                )
                if parts:
                    split_count += 1
                    print(
                        f"SPLIT\t{pdf_path.name}\t"
                        f"{pdf_path.stat().st_size / MB_BYTES:.2f}MB\t"
                        f"{', '.join(part.name for part in parts)}"
                    )
                else:
                    skipped_count += 1
                    print(
                        f"SKIP\t{pdf_path.name}\t"
                        f"{pdf_path.stat().st_size / MB_BYTES:.2f}MB"
                    )
            except Exception as exc:  # pragma: no cover - file dependent
                error_count += 1
                print(f"FAIL\t{pdf_path}\t{type(exc).__name__}: {exc}")

        print(
            f"\nSUMMARY\ttargets={len(targets)}\tsplit={split_count}\t"
            f"skipped={skipped_count}\tfailed={error_count}"
        )
        return 1 if error_count else 0
    except KeyboardInterrupt:  # pragma: no cover - interactive runtime
        print("\n사용자가 작업을 취소했습니다.")
        return 130
    except Exception as exc:
        print(f"ERROR\t{type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
