import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class OcrItem:
    x: float
    y: float
    text: str
    score: float


def _find_latest_manifest(manifest_dir: Path) -> Path:
    candidates = sorted(manifest_dir.glob("documents_*.csv"), reverse=True)
    if not candidates:
        raise FileNotFoundError(f"No manifest files found in {manifest_dir}")
    return candidates[0]


def _now_z() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalize_text(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _normalize_for_match(s: str) -> str:
    s = s.upper()
    s = s.replace("–", "-")
    s = s.replace("—", "-")
    s = re.sub(r"[^A-Z0-9:/\- ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _group_items_to_lines(items: list[OcrItem], y_threshold: float = 16.0) -> list[list[OcrItem]]:
    # Simple line grouping by y coordinate.
    items_sorted = sorted(items, key=lambda it: (it.y, it.x))
    lines: list[list[OcrItem]] = []
    for it in items_sorted:
        placed = False
        for line in lines:
            y_avg = sum(x.y for x in line) / max(1, len(line))
            if abs(it.y - y_avg) <= y_threshold:
                line.append(it)
                placed = True
                break
        if not placed:
            lines.append([it])

    # sort within each line by x and sort lines by y
    lines = [sorted(line, key=lambda it: it.x) for line in lines]
    lines = sorted(lines, key=lambda line: sum(it.y for it in line) / max(1, len(line)))
    return lines


def _lines_to_text(lines: list[list[OcrItem]]) -> list[str]:
    out: list[str] = []
    for line in lines:
        txt = _normalize_text(" ".join([it.text for it in line if it.text.strip()]))
        if txt:
            out.append(txt)
    return out


def _extract_value_from_lines(lines_text: list[str], label: str) -> tuple[str | None, float | None]:
    """Extract value for a label using best-effort heuristics.

    Returns: (value, confidence)
    """
    label_norm = _normalize_for_match(label)
    label_tokens = [t for t in label_norm.split(" ") if t]

    best_idx = None
    for i, line in enumerate(lines_text):
        lnorm = _normalize_for_match(line)
        # require all label tokens to appear (order-insensitive)
        if all(tok in lnorm for tok in label_tokens):
            best_idx = i
            break

    if best_idx is None:
        return None, None

    raw = lines_text[best_idx]
    # Prefer content after ':'
    if ":" in raw:
        after = raw.split(":", 1)[1].strip()
        if after:
            return after, 0.90
        # If the value is empty, do not blindly take the next line if it looks like another label.
        if best_idx + 1 < len(lines_text):
            nxt_norm = _normalize_for_match(lines_text[best_idx + 1])
            if re.match(r"^[A-Z0-9 /\-]+:\s*", nxt_norm):
                return None, None

    # Otherwise remove label tokens from the normalized string (rough)
    lnorm = _normalize_for_match(raw)
    for tok in label_tokens:
        lnorm = lnorm.replace(tok, " ")
    lnorm = _normalize_text(lnorm.replace(" : ", " ").replace(":", " "))
    if lnorm:
        return lnorm, 0.75

    # Fallback to next non-empty line
    for j in range(best_idx + 1, min(best_idx + 3, len(lines_text))):
        nxt = lines_text[j].strip()
        if nxt:
            return nxt, 0.60

    return None, None


def _postprocess_fields(fields: dict) -> dict:
    out = dict(fields)

    def _normalize_date(raw: str) -> str:
        s = _normalize_text(raw)
        # Common OCR issue: missing a slash between month and year (e.g. 14/042008).
        s = re.sub(r"(\d{2})/(\d{2})(\d{4})", r"\1/\2/\3", s)
        # Another common issue: missing separators (e.g. 14042008).
        s = re.sub(r"(\d{2})(\d{2})(\d{4})", r"\1/\2/\3", s)
        return s

    # Demo ID number
    if out.get("demo_id_no"):
        m = re.search(r"DEMO-\d{6,12}", out["demo_id_no"].upper())
        if m:
            out["demo_id_no"] = m.group(0)

    # Dates
    date_re = re.compile(r"\d{2}/\d{2}/\d{4}")

    def _expand_2digit_year(two_digit_year: int) -> int:
        pivot = datetime.now(timezone.utc).year % 100
        # If yy is greater than pivot (e.g. 97 > 26), assume 19yy; else 20yy.
        return (1900 + two_digit_year) if two_digit_year > pivot else (2000 + two_digit_year)

    for k in ["date_of_birth", "issue_date", "expiry_date"]:
        v = out.get(k)
        if not v:
            continue
        v = _normalize_date(v)
        m = date_re.search(v)
        if m:
            out[k] = m.group(0)
            continue

        # Be conservative: only expand 2-digit years for DOB.
        if k != "date_of_birth":
            continue

        m2 = re.search(r"(\d{2})/(\d{2})/(\d{2})", v)
        if m2:
            dd, mm, yy = m2.group(1), m2.group(2), int(m2.group(3))
            out[k] = f"{dd}/{mm}/{_expand_2digit_year(yy):04d}"
            continue

        m3 = re.search(r"(\d{2})(\d{2})/(\d{2})", v)
        if m3:
            dd, mm, yy = m3.group(1), m3.group(2), int(m3.group(3))
            out[k] = f"{dd}/{mm}/{_expand_2digit_year(yy):04d}"

    # Sex
    sex = out.get("sex")
    if sex:
        s = _normalize_for_match(sex)
        if "NU" in s or "NỮ" in sex.upper():
            out["sex"] = "Nữ"
        elif "NAM" in s:
            out["sex"] = "Nam"

    # Clean whitespace for all strings
    for k, v in list(out.items()):
        if isinstance(v, str):
            out[k] = _normalize_text(v)

    return out


def ocr_id_card(
    path: Path,
    *,
    lang: str = "en",
    doc_orientation: bool = False,
    unwarp: bool = False,
    textline_orientation: bool = False,
) -> tuple[list[str], str, float]:
    try:
        from paddleocr import PaddleOCR
    except Exception as e:
        raise RuntimeError(
            "PaddleOCR is not installed. Use .venv_ocr or install paddleocr dependencies."
        ) from e

    # Work around PaddlePaddle 3.x Windows CPU runtime issues (PIR + oneDNN).
    # These flags must be set before model execution.
    try:
        import paddle

        paddle.set_flags(
            {
                "FLAGS_use_mkldnn": False,
                "FLAGS_use_onednn": False,
                "FLAGS_enable_pir_api": False,
                "FLAGS_enable_pir_in_executor": False,
            }
        )
    except Exception:
        # Best-effort: if flags are unavailable on a given build, continue.
        pass

    # Avoid slow/fragile online checks each run.
    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

    # Instantiate per process (ok for batch sizes here).
    # IMPORTANT: disable doc orientation/unwarp/textline orientation by default.
    # On some Windows + paddlepaddle builds, these extra PaddleX models can trigger
    # PIR/oneDNN runtime errors.
    ocr = PaddleOCR(
        lang=lang,
        use_doc_orientation_classify=doc_orientation,
        use_doc_unwarping=unwarp,
        use_textline_orientation=textline_orientation,
    )

    if hasattr(ocr, "predict"):
        result = ocr.predict(str(path))
    else:
        result = ocr.ocr(str(path))

    items: list[OcrItem] = []
    scores: list[float] = []

    # The return structure can vary across PaddleOCR versions/modes.
    # We support:
    # - PaddleOCR.ocr(): list[page] -> list[line] -> (bbox, (text, score))
    # - PaddleOCR.predict(): list[OCRResult] where OCRResult.json['res'] contains
    #   rec_texts/rec_scores/dt_polys/rec_boxes.
    pages = result if isinstance(result, list) else []
    for page in pages:
        # PaddleOCR.predict() path (OCRResult)
        if hasattr(page, "json"):
            try:
                payload = page.json
                res = payload.get("res") if isinstance(payload, dict) else None
                if isinstance(res, dict):
                    texts = res.get("rec_texts") or []
                    rec_scores = res.get("rec_scores") or []
                    polys = res.get("dt_polys") or res.get("rec_polys") or []
                    boxes = res.get("rec_boxes") or []

                    for idx, txt in enumerate(texts):
                        score = rec_scores[idx] if idx < len(rec_scores) else 0.0
                        poly = polys[idx] if idx < len(polys) else None
                        box = boxes[idx] if idx < len(boxes) else None

                        bbox: list[list[float]] | None = None
                        if isinstance(poly, list) and len(poly) >= 4:
                            bbox = [[float(p[0]), float(p[1])] for p in poly[:4]]
                        elif isinstance(box, list) and len(box) == 4:
                            x1, y1, x2, y2 = [float(v) for v in box]
                            bbox = [[x1, y1], [x2, y1], [x2, y2], [x1, y2]]

                        if not bbox:
                            continue

                        xs = [p[0] for p in bbox]
                        ys = [p[1] for p in bbox]
                        x = float(sum(xs) / len(xs))
                        y = float(sum(ys) / len(ys))
                        txt = txt or ""
                        try:
                            score_f = float(score)
                        except Exception:
                            score_f = 0.0
                        items.append(OcrItem(x=x, y=y, text=txt, score=score_f))
                        scores.append(score_f)
                continue
            except Exception:
                # Fall through to try other formats
                pass

        if not isinstance(page, list):
            continue
        for line in page:
            if not isinstance(line, (list, tuple)) or len(line) != 2:
                continue
            bbox, rec = line
            if not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
                continue
            if not isinstance(rec, (list, tuple)) or len(rec) != 2:
                continue
            txt, score = rec
            try:
                xs = [p[0] for p in bbox]
                ys = [p[1] for p in bbox]
                x = float(sum(xs) / len(xs))
                y = float(sum(ys) / len(ys))
            except Exception:
                continue
            txt = txt or ""
            try:
                score_f = float(score)
            except Exception:
                score_f = 0.0
            items.append(OcrItem(x=x, y=y, text=txt, score=score_f))
            scores.append(score_f)

    lines = _group_items_to_lines(items)
    lines_text = _lines_to_text(lines)
    full_text = "\n".join(lines_text)
    avg_score = sum(scores) / len(scores) if scores else 0.0
    return lines_text, full_text, avg_score


def main() -> None:
    p = argparse.ArgumentParser(description="OCR + extraction for DEMO CCCD (id_card) documents")
    p.add_argument("--manifest", default="", help="Path to documents_YYYY-MM-DD.csv (combined manifest)")
    p.add_argument("--run-date", default="", help="Filter by run_date (YYYY-MM-DD)")
    p.add_argument("--limit", type=int, default=0, help="Limit number of documents processed")
    p.add_argument("--lang", default="en", help="PaddleOCR language (default: en)")
    p.add_argument(
        "--doc-orientation",
        action="store_true",
        help="Enable document orientation classifier (may be unstable on some Windows CPU builds)",
    )
    p.add_argument(
        "--unwarp",
        action="store_true",
        help="Enable document unwarping model (may be unstable on some Windows CPU builds)",
    )
    p.add_argument(
        "--textline-orientation",
        action="store_true",
        help="Enable textline orientation (replacement for deprecated angle classifier)",
    )
    p.add_argument(
        "--no-angle-cls",
        action="store_true",
        help="(Deprecated) Kept for compatibility; prefer --textline-orientation",
    )
    p.add_argument(
        "--out",
        default="",
        help="Output CSV path (default: data/unstructured/extracted/id_card_extractions_<run_date>.csv)",
    )
    args = p.parse_args()

    manifest_path = Path(args.manifest) if args.manifest else _find_latest_manifest(PROJECT_ROOT / "data" / "unstructured" / "manifests")
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    run_date_filter = args.run_date.strip()

    rows: list[dict] = []
    with manifest_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if (r.get("doc_type") or "").lower() != "id_card":
                continue
            if run_date_filter and (r.get("run_date") or "") != run_date_filter:
                continue
            rows.append(r)

    if not rows:
        raise RuntimeError("No id_card rows found in manifest (after filters).")

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    effective_run_date = run_date_filter or (rows[0].get("run_date") or "") or "unknown"
    out_path = Path(args.out) if args.out else (
        PROJECT_ROOT / "data" / "unstructured" / "extracted" / f"id_card_extractions_{effective_run_date}.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "document_id",
        "entity_type",
        "entity_id",
        "doc_type",
        "file_path",
        "sha256",
        "run_date",
        "ocr_engine",
        "ocr_lang",
        "ocr_avg_score",
        "ocr_text",
        "full_name",
        "demo_id_no",
        "date_of_birth",
        "sex",
        "nationality",
        "place_of_origin",
        "place_of_residence",
        "issue_date",
        "expiry_date",
        "extraction_confidence",
        "processed_at",
        "status",
        "error",
    ]

    processed_at = _now_z()

    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames)
        w.writeheader()

        ok, fail = 0, 0
        for r in rows:
            doc_id = r.get("document_id")
            file_path = r.get("file_path")
            try:
                img_path = Path(file_path) if file_path and (":\\" in file_path or file_path.startswith("/")) else (PROJECT_ROOT / (file_path or ""))
                if not img_path.exists():
                    raise FileNotFoundError(f"Image not found: {img_path}")

                lines_text, full_text, avg_score = ocr_id_card(
                    img_path,
                    lang=args.lang,
                    doc_orientation=bool(args.doc_orientation),
                    unwarp=bool(args.unwarp),
                    textline_orientation=bool(args.textline_orientation and (not args.no_angle_cls)),
                )

                # Extraction by labels (DEMO template)
                extracted: dict[str, str | None] = {}
                confs: list[float] = []

                label_map = {
                    "full_name": "FULL NAME",
                    "demo_id_no": "DEMO ID NO",
                    "date_of_birth": "DATE OF BIRTH",
                    "sex": "SEX",
                    "nationality": "NATIONALITY",
                    "place_of_origin": "PLACE OF ORIGIN",
                    "place_of_residence": "PLACE OF RESIDENCE",
                    "issue_date": "ISSUE DATE",
                    "expiry_date": "EXPIRY DATE",
                }

                for key, label in label_map.items():
                    val, c = _extract_value_from_lines(lines_text, label)
                    extracted[key] = val
                    if c is not None:
                        confs.append(float(c))

                # Fallback: some templates show sex as a standalone line (e.g. "Nam"/"Nữ")
                if not extracted.get("sex"):
                    for line in lines_text:
                        norm = _normalize_for_match(line)
                        if norm == "NAM":
                            extracted["sex"] = "Nam"
                            confs.append(0.70)
                            break
                        if norm in {"NU", "NU "} or "NỮ" in line.upper():
                            extracted["sex"] = "Nữ"
                            confs.append(0.70)
                            break

                extracted = _postprocess_fields(extracted)
                extraction_conf = sum(confs) / len(confs) if confs else 0.0

                out_row = {
                    "document_id": doc_id,
                    "entity_type": r.get("entity_type"),
                    "entity_id": r.get("entity_id"),
                    "doc_type": r.get("doc_type"),
                    "file_path": file_path,
                    "sha256": r.get("sha256"),
                    "run_date": r.get("run_date"),
                    "ocr_engine": "paddleocr",
                    "ocr_lang": args.lang,
                    "ocr_avg_score": f"{avg_score:.4f}",
                    "ocr_text": json.dumps({"lines": lines_text, "text": full_text}, ensure_ascii=False),
                    "full_name": extracted.get("full_name"),
                    "demo_id_no": extracted.get("demo_id_no"),
                    "date_of_birth": extracted.get("date_of_birth"),
                    "sex": extracted.get("sex"),
                    "nationality": extracted.get("nationality"),
                    "place_of_origin": extracted.get("place_of_origin"),
                    "place_of_residence": extracted.get("place_of_residence"),
                    "issue_date": extracted.get("issue_date"),
                    "expiry_date": extracted.get("expiry_date"),
                    "extraction_confidence": f"{extraction_conf:.4f}",
                    "processed_at": processed_at,
                    "status": "ok",
                    "error": "",
                }
                w.writerow(out_row)
                ok += 1

            except Exception as e:
                w.writerow(
                    {
                        "document_id": doc_id,
                        "entity_type": r.get("entity_type"),
                        "entity_id": r.get("entity_id"),
                        "doc_type": r.get("doc_type"),
                        "file_path": file_path,
                        "sha256": r.get("sha256"),
                        "run_date": r.get("run_date"),
                        "ocr_engine": "paddleocr",
                        "ocr_lang": args.lang,
                        "ocr_avg_score": "",
                        "ocr_text": "",
                        "full_name": "",
                        "demo_id_no": "",
                        "date_of_birth": "",
                        "sex": "",
                        "nationality": "",
                        "place_of_origin": "",
                        "place_of_residence": "",
                        "issue_date": "",
                        "expiry_date": "",
                        "extraction_confidence": "",
                        "processed_at": processed_at,
                        "status": "error",
                        "error": str(e),
                    }
                )
                fail += 1

        print(f"manifest={manifest_path.as_posix()}")
        print(f"out={out_path.as_posix()}")
        print(f"ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
