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
    """Extract value for a label using best-effort heuristics."""
    label_norm = _normalize_for_match(label)
    label_tokens = [t for t in label_norm.split(" ") if t]

    best_idx = None
    for i, line in enumerate(lines_text):
        lnorm = _normalize_for_match(line)
        if all(tok in lnorm for tok in label_tokens):
            best_idx = i
            break

    if best_idx is None:
        return None, None

    raw = lines_text[best_idx]
    if ":" in raw:
        after = raw.split(":", 1)[1].strip()
        if after:
            return after, 0.90
        if best_idx + 1 < len(lines_text):
            nxt_norm = _normalize_for_match(lines_text[best_idx + 1])
            if re.match(r"^[A-Z0-9 /\-]+:\s*", nxt_norm):
                return None, None

    lnorm = _normalize_for_match(raw)
    for tok in label_tokens:
        lnorm = lnorm.replace(tok, " ")
    lnorm = _normalize_text(lnorm.replace(" : ", " ").replace(":", " "))
    if lnorm:
        return lnorm, 0.75

    for j in range(best_idx + 1, min(best_idx + 3, len(lines_text))):
        nxt = lines_text[j].strip()
        if nxt:
            return nxt, 0.60

    return None, None


def _postprocess_fields(fields: dict) -> dict:
    out = dict(fields)

    def _normalize_date(raw: str) -> str:
        s = _normalize_text(raw)
        s = re.sub(r"(\d{2})/(\d{2})(\d{4})", r"\1/\2/\3", s)
        s = re.sub(r"(\d{2})(\d{2})(\d{4})", r"\1/\2/\3", s)
        return s

    # Account number cleanup (digits only)
    if out.get("account_number"):
        acc_num = re.sub(r'[^0-9]', '', out["account_number"])
        if acc_num:
            out["account_number"] = acc_num

    # Dates
    date_re = re.compile(r"\d{2}/\d{2}/\d{4}")

    def _expand_2digit_year(two_digit_year: int) -> int:
        pivot = datetime.now(timezone.utc).year % 100
        return (1900 + two_digit_year) if two_digit_year > pivot else (2000 + two_digit_year)

    for k in ["opening_date"]:  # Only opening_date for savings book
        v = out.get(k)
        if not v:
            continue
        v = _normalize_date(v)
        m = date_re.search(v)
        if m:
            out[k] = m.group(0)
            continue

        m2 = re.search(r"(\d{2})/(\d{2})/(\d{2})", v)
        if m2:
            dd, mm, yy = m2.group(1), m2.group(2), int(m2.group(3))
            out[k] = f"{dd}/{mm}/{_expand_2digit_year(yy):04d}"

    # Balance cleanup (remove currency symbols)
    if out.get("balance"):
        balance_str = re.sub(r'[^\d.,]', '', str(out["balance"]))
        try:
            # Convert to decimal string
            if ',' in balance_str and '.' in balance_str:
                # Vietnamese format: 1.234,56 → 1234.56
                balance_str = balance_str.replace('.', '').replace(',', '.')
            elif ',' in balance_str:
                # US format: 1,234.56 (keep comma as thousand separator? remove)
                balance_str = balance_str.replace(',', '')
            out["balance"] = float(balance_str)
        except:
            out["balance"] = None

    # Interest rate cleanup
    if out.get("interest_rate"):
        rate_str = re.sub(r'[^\d.,%]', '', str(out["interest_rate"]))
        try:
            rate_str = rate_str.replace('%', '').replace(',', '.')
            out["interest_rate"] = float(rate_str)
        except:
            out["interest_rate"] = None

    # Clean whitespace for all strings
    for k, v in list(out.items()):
        if isinstance(v, str):
            out[k] = _normalize_text(v)

    return out


def ocr_savings_book(
    path: Path,
    *,
    lang: str = "en",
    doc_orientation: bool = False,
    unwarp: bool = False,
    textline_orientation: bool = False,
) -> tuple[list[str], str, float]:
    """OCR savings book - similar to id_card but with different label extraction"""
    try:
        from paddleocr import PaddleOCR
    except Exception as e:
        raise RuntimeError(
            "PaddleOCR is not installed. Use .venv_ocr or install paddleocr dependencies."
        ) from e

    try:
        import paddle
        paddle.set_flags({
            "FLAGS_use_mkldnn": False,
            "FLAGS_use_onednn": False,
            "FLAGS_enable_pir_api": False,
            "FLAGS_enable_pir_in_executor": False,
        })
    except Exception:
        pass

    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")

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

    pages = result if isinstance(result, list) else []
    for page in pages:
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
    p = argparse.ArgumentParser(description="OCR + extraction for Savings Book documents")
    p.add_argument("--input-dir", required=True, help="Directory containing images (with user_id subfolders)")
    p.add_argument("--run-date", required=True, help="Run date (YYYY-MM-DD)")
    p.add_argument("--limit", type=int, default=0, help="Limit number of documents processed")
    p.add_argument("--lang", default="en", help="PaddleOCR language (default: en)")
    p.add_argument(
        "--doc-orientation",
        action="store_true",
        help="Enable document orientation classifier",
    )
    p.add_argument(
        "--unwarp",
        action="store_true",
        help="Enable document unwarping model",
    )
    p.add_argument(
        "--textline-orientation",
        action="store_true",
        help="Enable textline orientation",
    )
    p.add_argument(
        "--no-angle-cls",
        action="store_true",
        help="(Deprecated)",
    )
    p.add_argument(
        "--out",
        default="",
        help="Output CSV path (default: data/unstructured/extracted/savings_book_extractions_<run_date>.csv)",
    )
    args = p.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    run_date = args.run_date.strip()

    # Find all images
    image_files = list(input_dir.rglob("*.jpg")) + \
                  list(input_dir.rglob("*.png")) + \
                  list(input_dir.rglob("*.jpeg")) + \
                  list(input_dir.rglob("*.tiff")) + \
                  list(input_dir.rglob("*.tif"))

    if not image_files:
        raise RuntimeError(f"No image files found in {input_dir}")

    if args.limit and args.limit > 0:
        image_files = image_files[:args.limit]

    print(f"Found {len(image_files)} images in {input_dir}")

    out_path = Path(args.out) if args.out else (
        PROJECT_ROOT / "data" / "unstructured" / "extracted" / f"savings_book_extractions_{run_date}.csv"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    processed_at = _now_z()

    # Output columns for savings book
    fieldnames = [
        "document_id",
        "file_path",
        "run_date",
        "user_id",
        "ocr_engine",
        "ocr_lang",
        "ocr_avg_score",
        "ocr_text",
        "account_number",
        "account_holder",
        "account_type",
        "opening_date",
        "balance",
        "interest_rate",
        "extraction_confidence",
        "processed_at",
        "status",
        "error_message",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f_out:
        w = csv.DictWriter(f_out, fieldnames=fieldnames)
        w.writeheader()

        ok, fail = 0, 0
        for img_path in image_files:
            document_id = img_path.stem

            # Extract user_id from path
            user_id = None
            for part in img_path.parts:
                if part.startswith("user_id="):
                    try:
                        user_id = int(part.split("=")[1])
                    except:
                        user_id = None
                    break

            try:
                lines_text, full_text, avg_score = ocr_savings_book(
                    img_path,
                    lang=args.lang,
                    doc_orientation=bool(args.doc_orientation),
                    unwarp=bool(args.unwarp),
                    textline_orientation=bool(args.textline_orientation and (not args.no_angle_cls)),
                )

                # Extraction for savings book
                extracted: dict[str, str | None] = {}
                confs: list[float] = []

                label_map = {
                    "account_number": "SỐ TÀI KHOẢN",
                    "account_holder": "CHỦ TÀI KHOẢN",
                    "account_type": "LOẠI TÀI KHOẢN",
                    "opening_date": "NGÀY MỞ SỔ",
                    "balance": "SỐ DƯ",
                    "interest_rate": "LÃI SUẤT",
                }

                for key, label in label_map.items():
                    val, c = _extract_value_from_lines(lines_text, label)
                    extracted[key] = val
                    if c is not None:
                        confs.append(float(c))

                extracted = _postprocess_fields(extracted)
                extraction_conf = sum(confs) / len(confs) if confs else 0.0

                out_row = {
                    "document_id": document_id,
                    "file_path": str(img_path),
                    "run_date": run_date,
                    "user_id": user_id,
                    "ocr_engine": "paddleocr",
                    "ocr_lang": args.lang,
                    "ocr_avg_score": f"{avg_score:.4f}",
                    "ocr_text": json.dumps({"lines": lines_text, "text": full_text}, ensure_ascii=False),
                    "account_number": extracted.get("account_number"),
                    "account_holder": extracted.get("account_holder"),
                    "account_type": extracted.get("account_type"),
                    "opening_date": extracted.get("opening_date"),
                    "balance": extracted.get("balance"),
                    "interest_rate": extracted.get("interest_rate"),
                    "extraction_confidence": f"{extraction_conf:.4f}",
                    "processed_at": processed_at,
                    "status": "ok",
                    "error_message": "",
                }
                w.writerow(out_row)
                ok += 1

            except Exception as e:
                w.writerow(
                    {
                        "document_id": document_id,
                        "file_path": str(img_path),
                        "run_date": run_date,
                        "user_id": user_id,
                        "ocr_engine": "paddleocr",
                        "ocr_lang": args.lang,
                        "ocr_avg_score": "",
                        "ocr_text": "",
                        "account_number": "",
                        "account_holder": "",
                        "account_type": "",
                        "opening_date": "",
                        "balance": "",
                        "interest_rate": "",
                        "extraction_confidence": "",
                        "processed_at": processed_at,
                        "status": "error",
                        "error_message": str(e),
                    }
                )
                fail += 1

    print(f"\nInput directory: {input_dir.as_posix()}")
    print(f"Output: {out_path.as_posix()}")
    print(f"Success: {ok} | Failed: {fail}")


if __name__ == "__main__":
    main()
