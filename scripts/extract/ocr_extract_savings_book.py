"""
OCR Extraction Pipeline cho Savings Book (Sổ Tiết Kiệm) – Transaction Page

Pipeline:
1. Load ảnh
2. Preprocess (deskew, warp, CLAHE/sharpen)
3. OCR per ROI (7 fields trên trang giao dịch)
4. Parse & Structure
5. Confidence Scoring
6. Save CSV

Fields extracted:
  transaction_date | description | transaction_code
  transaction_amount | balance | interest_rate | signature

Usage:
    python scripts/extract/ocr_extract_savings_book.py --run-date 2026-05-29
    python scripts/extract/ocr_extract_savings_book.py --input-dir data/... --run-date 2026-05-29 --limit 10
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import cv2
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from paddleocr import PaddleOCR
except ImportError:
    PaddleOCR = None  # type: ignore

OCR_LANG = "en"

# ---------------------------------------------------------------------------
# ROI definitions – fractions relative to warped image (out_w=1200)
# Source: ocr_extract_savings_book.ipynb
# ---------------------------------------------------------------------------

@dataclass
class ROI:
    name: str
    x1: float
    y1: float
    x2: float
    y2: float


SAVINGS_BOOK_ROIS = [
    ROI("transaction_date",   0.007, 0.088, 0.167, 0.115),
    ROI("description",        0.007, 0.115, 0.167, 0.142),
    ROI("transaction_code",   0.167, 0.088, 0.248, 0.142),
    ROI("transaction_amount", 0.248, 0.088, 0.569, 0.115),
    ROI("balance",            0.569, 0.088, 0.757, 0.115),
    ROI("interest_rate",      0.757, 0.088, 0.857, 0.115),
    ROI("signature",          0.857, 0.108, 0.993, 0.162),
]

_WATERMARK_PATTERNS = [
    r"\bSAMPLE\b",
    r"\bDEMO\s*-?\s*NOT\s+A\s+REAL\s+BANK\s+DOCUMENT\b",
    r"\bNOT\s+A\s+REAL\s+BANK\s+DOCUMENT\b",
    r"\bFOR\s+TESTING\s+ONLY\b",
    r"\bDEMO\s+BANK\b",
]


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------

def normalize_width(bgr: np.ndarray, out_w: int = 1200) -> np.ndarray:
    h, w = bgr.shape[:2]
    if w == out_w:
        return bgr
    out_h = int(round(h * out_w / max(1, w)))
    interp = cv2.INTER_AREA if out_w < w else cv2.INTER_CUBIC
    return cv2.resize(bgr, (out_w, out_h), interpolation=interp)


def rotate_bound(bgr: np.ndarray, angle: float, border=(245, 245, 245)) -> np.ndarray:
    h, w = bgr.shape[:2]
    cx, cy = w / 2.0, h / 2.0
    m = cv2.getRotationMatrix2D((cx, cy), angle, 1.0)
    cos, sin = abs(m[0, 0]), abs(m[0, 1])
    new_w = int(h * sin + w * cos)
    new_h = int(h * cos + w * sin)
    m[0, 2] += new_w / 2.0 - cx
    m[1, 2] += new_h / 2.0 - cy
    return cv2.warpAffine(bgr, m, (new_w, new_h), flags=cv2.INTER_CUBIC, borderValue=border)


def estimate_skew_angle(bgr: np.ndarray) -> float:
    gray = cv2.GaussianBlur(cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY), (3, 3), 0)
    edges = cv2.Canny(gray, 50, 150, apertureSize=3)
    lines = cv2.HoughLinesP(
        edges, 1, np.pi / 180,
        threshold=160,
        minLineLength=max(120, bgr.shape[1] // 5),
        maxLineGap=20,
    )
    if lines is None:
        return 0.0
    angles = []
    for x1, y1, x2, y2 in lines[:, 0, :]:
        a = np.degrees(np.arctan2(y2 - y1, x2 - x1))
        if -15 <= a <= 15:
            angles.append(a)
    return float(np.median(angles)) if angles else 0.0


def deskew(bgr: np.ndarray, min_abs: float = 0.20) -> tuple[np.ndarray, float]:
    angle = estimate_skew_angle(bgr)
    if abs(angle) < min_abs:
        return bgr, angle
    return rotate_bound(bgr, angle=-angle), angle


def order_points(pts: np.ndarray) -> np.ndarray:
    rect = np.zeros((4, 2), dtype="float32")
    s = pts.sum(axis=1)
    diff = np.diff(pts, axis=1)
    rect[0] = pts[np.argmin(s)]
    rect[2] = pts[np.argmax(s)]
    rect[1] = pts[np.argmin(diff)]
    rect[3] = pts[np.argmax(diff)]
    return rect


def find_document_quad(bgr: np.ndarray) -> np.ndarray | None:
    blur = cv2.GaussianBlur(cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY), (5, 5), 0)
    edges = cv2.Canny(blur, 40, 120)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    img_area = bgr.shape[0] * bgr.shape[1]
    for c in sorted(contours, key=cv2.contourArea, reverse=True)[:10]:
        if cv2.contourArea(c) < img_area * 0.25:
            continue
        approx = cv2.approxPolyDP(c, 0.02 * cv2.arcLength(c, True), True)
        if len(approx) == 4:
            return order_points(approx.reshape(4, 2).astype("float32"))
    return None


def warp_document(bgr: np.ndarray, quad: np.ndarray, out_w: int = 1200) -> np.ndarray:
    rect = order_points(quad)
    tl, tr, br, bl = rect
    max_w = int(max(np.linalg.norm(br - bl), np.linalg.norm(tr - tl)))
    max_h = int(max(np.linalg.norm(tr - br), np.linalg.norm(tl - bl)))
    dst = np.array([[0, 0], [max_w - 1, 0], [max_w - 1, max_h - 1], [0, max_h - 1]], dtype="float32")
    warped = cv2.warpPerspective(
        bgr, cv2.getPerspectiveTransform(rect, dst),
        (max_w, max_h), borderValue=(245, 245, 245),
    )
    return normalize_width(warped, out_w=out_w)


def preprocess_for_ocr(bgr: np.ndarray) -> dict[str, np.ndarray]:
    gray    = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    denoise = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
    clahe   = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(denoise)
    sharpen = cv2.addWeighted(clahe, 1.5, cv2.GaussianBlur(clahe, (0, 0), 1.0), -0.5, 0)
    return {"bgr": bgr, "gray": gray, "clahe": clahe, "sharpen": sharpen}


def scale_rois(rois: list[ROI], w: int, h: int) -> dict[str, tuple[int, int, int, int]]:
    return {r.name: (
        max(0, min(w - 1, int(round(r.x1 * w)))),
        max(0, min(h - 1, int(round(r.y1 * h)))),
        max(0, min(w,     int(round(r.x2 * w)))),
        max(0, min(h,     int(round(r.y2 * h)))),
    ) for r in rois}


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").replace("|", "I")).strip()


def strip_watermark(text: str) -> str:
    for p in _WATERMARK_PATTERNS:
        text = re.sub(p, " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def parse_field(field: str, text: str) -> str:
    text = strip_watermark(clean_text(text))
    if field == "transaction_date":
        m = re.search(r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{2,4}", text)
        return m.group(0).replace("-", "/").replace(".", "/") if m else text
    if field == "transaction_code":
        m = re.search(r"\b[A-Z]{2,8}\b", text.upper())
        return m.group(0) if m else text.upper()
    if field in {"transaction_amount", "balance"}:
        m = re.search(r"\d[\d,\.]*", text)
        return m.group(0) if m else text
    if field == "interest_rate":
        m = re.search(r"\d+(?:[\.,]\d+)?\s*%", text)
        return m.group(0).replace(",", ".") if m else text
    if field == "signature":
        text = re.sub(r"\b(DEMO|Signature)\b", " ", text, flags=re.IGNORECASE)
        return clean_text(text)
    return text


def is_plausible(field: str, text: str) -> bool:
    text = clean_text(text)
    if not text:
        return False
    if field == "transaction_date":
        return bool(re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", text))
    if field == "transaction_code":
        return bool(re.fullmatch(r"[A-Z]{2,8}", text))
    if field in {"transaction_amount", "balance"}:
        return bool(re.search(r"\d", text))
    if field == "interest_rate":
        return "%" in text and bool(re.search(r"\d", text))
    return len(text) >= 2


def extract_user_id(path: Path) -> int | None:
    for part in path.parts:
        if part.startswith("user_id="):
            try:
                return int(part.split("=", 1)[1])
            except Exception:
                return None
    return None


# ---------------------------------------------------------------------------
# OCR helpers
# ---------------------------------------------------------------------------

def _box_to_xyxy(box) -> tuple | None:
    arr = np.asarray(box, dtype=np.float32)
    if arr.size == 4 and arr.ndim == 1:
        return tuple(arr.tolist())
    if arr.ndim >= 2 and arr.shape[-1] >= 2:
        pts = arr.reshape(-1, arr.shape[-1])[:, :2]
        return (
            float(np.min(pts[:, 0])), float(np.min(pts[:, 1])),
            float(np.max(pts[:, 0])), float(np.max(pts[:, 1])),
        )
    return None


def ocr_predict_items(ocr_engine, img: np.ndarray) -> tuple[list[dict], str, float | None]:
    bgr_img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR) if img.ndim == 2 else img
    results = ocr_engine.predict(
        bgr_img,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )
    if not results:
        return [], "", None
    j = getattr(results[0], "json", None)
    if not isinstance(j, dict):
        return [], "", None
    res    = j.get("res", {})
    texts  = res.get("rec_texts",  []) or []
    scores = res.get("rec_scores", []) or []
    boxes  = next(
        (res.get(k) for k in ("rec_boxes", "rec_polys", "dt_boxes", "dt_polys") if res.get(k)),
        [],
    )
    items = []
    for i, text in enumerate(texts):
        if i >= len(boxes):
            continue
        xyxy = _box_to_xyxy(boxes[i])
        if xyxy is None:
            continue
        score = float(scores[i] if i < len(scores) and scores[i] is not None else 0.0)
        x1, y1, x2, y2 = xyxy
        items.append({"text": str(text).strip(), "score": score,
                      "x1": x1, "y1": y1, "x2": x2, "y2": y2})
    full_text  = " ".join(it["text"] for it in items).strip()
    mean_score = float(np.mean([it["score"] for it in items])) if items else None
    return items, full_text, mean_score


def ocr_roi_variants(pp: dict[str, np.ndarray], box: tuple) -> list[tuple[str, np.ndarray]]:
    x1, y1, x2, y2 = box
    variants = []
    for key in ("sharpen", "clahe", "gray"):
        base = pp.get(key)
        if base is None:
            continue
        roi = base[y1:y2, x1:x2]
        if roi.size:
            variants.append((key, roi))
            variants.append((f"{key}_x2", cv2.resize(roi, None, fx=2.0, fy=2.0,
                                                      interpolation=cv2.INTER_CUBIC)))
    return variants


def choose_best_ocr(field_name: str, candidates: list[tuple]) -> tuple:
    best      = ("", None, "", 0)
    best_key  = (-1, -1.0, -1, -1)
    for src, raw, score, n_items in candidates:
        parsed = parse_field(field_name, raw)
        plaus  = 1 if is_plausible(field_name, parsed) else 0
        s      = float(score) if score is not None else -1.0
        key    = (plaus, s, len(parsed), n_items)
        if key > best_key:
            best_key = key
            best     = (raw, score, src, n_items)
    return best


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def process_directory(
    input_dir: Path,
    run_date: str,
    output_csv: Path,
    lang: str = OCR_LANG,
    limit: int | None = None,
) -> bool:
    if PaddleOCR is None:
        raise RuntimeError("PaddleOCR not installed. Run: pip install paddlepaddle paddleocr")

    extensions = ("*.jpg", "*.jpeg", "*.png", "*.tif", "*.tiff", "*.bmp")
    img_paths: list[Path] = []
    for ext in extensions:
        img_paths.extend(input_dir.rglob(ext))
    img_paths = sorted(img_paths)
    total_found = len(img_paths)
    if limit:
        img_paths = img_paths[:limit]

    if not img_paths:
        print(f"ERROR: No images found in {input_dir}")
        return False

    print(f"Found {total_found} images; processing {len(img_paths)}")
    print(f"Initializing PaddleOCR (lang={lang})...")
    ocr = PaddleOCR(lang=lang, ocr_version="PP-OCRv3", use_textline_orientation=False)

    rows: list[dict] = []

    for i, img_path in enumerate(img_paths, 1):
        print(f"\n[{i}/{len(img_paths)}] {img_path}")
        try:
            bgr = cv2.imread(str(img_path))
            if bgr is None:
                raise ValueError(f"Cannot read image: {img_path}")

            # Step 2: Preprocess
            deskewed, _ = deskew(bgr)
            quad = find_document_quad(deskewed)
            warped = (warp_document(deskewed, quad, out_w=1200)
                      if quad is not None
                      else normalize_width(deskewed, out_w=1200))
            pp = preprocess_for_ocr(warped)
            roi_boxes = scale_rois(SAVINGS_BOOK_ROIS, warped.shape[1], warped.shape[0])

            # Step 3: OCR per ROI
            field_raw: dict[str, dict] = {}
            for field_name, box in roi_boxes.items():
                candidates = []
                for src, roi in ocr_roi_variants(pp, box):
                    items_list, full_text, mean_score = ocr_predict_items(ocr, roi)
                    candidates.append((src, full_text, mean_score, len(items_list)))
                raw, score, src, _ = choose_best_ocr(field_name, candidates)
                field_raw[field_name] = {"raw_text": raw, "score": score, "src": src}

            # Step 4: Parse & Structure
            parsed = {fn: parse_field(fn, payload["raw_text"])
                      for fn, payload in field_raw.items()}

            # Step 5: Confidence
            scores = [float(p["score"]) for p in field_raw.values() if p.get("score") is not None]
            plaus  = sum(1 for fn, pf in parsed.items() if is_plausible(fn, pf))
            total  = len(field_raw)
            ocr_conf   = float(np.mean(scores)) if scores else 0.0
            parse_conf = plaus / max(1, total)
            final_conf = round(0.70 * ocr_conf + 0.30 * parse_conf, 4)

            row = {
                "file_name":          img_path.name,
                "file_path":          str(img_path),
                "run_date":           run_date,
                "user_id":            extract_user_id(img_path),
                "transaction_date":   parsed.get("transaction_date"),
                "description":        parsed.get("description"),
                "transaction_code":   parsed.get("transaction_code"),
                "transaction_amount": parsed.get("transaction_amount"),
                "balance":            parsed.get("balance"),
                "interest_rate":      parsed.get("interest_rate"),
                "signature":          parsed.get("signature"),
                "final_confidence":   final_conf,
                "ocr_confidence":     round(ocr_conf, 4),
                "parse_confidence":   round(parse_conf, 4),
                "plausible_fields":   plaus,
            }
            rows.append(row)
            print(f"  OK: date={row['transaction_date']} | code={row['transaction_code']} "
                  f"| amount={row['transaction_amount']} | balance={row['balance']} "
                  f"| conf={final_conf:.3f}")

        except Exception as e:
            import traceback
            print(f"  ERROR: {e}")
            traceback.print_exc()
            rows.append({
                "file_name": img_path.name,
                "file_path": str(img_path),
                "run_date":  run_date,
                "user_id":   extract_user_id(img_path),
                "final_confidence": 0.0,
                "ocr_confidence": 0.0,
                "parse_confidence": 0.0,
                "plausible_fields": 0,
            })

    df = pd.DataFrame(rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"\nSaved {len(df)} records → {output_csv}")

    ok = df["final_confidence"].notna().sum()
    avg_conf = df["final_confidence"].mean()
    print(f"Processed: {ok}/{len(df)} | Avg confidence: {avg_conf:.3f}")
    return True


def main():
    parser = argparse.ArgumentParser(description="OCR Savings Book – Transaction Page")
    parser.add_argument("--input-dir", default=None)
    parser.add_argument("--run-date",  default=None,  help="YYYY-MM-DD")
    parser.add_argument("--out",       default=None,  help="Output CSV path")
    parser.add_argument("--lang",      default=OCR_LANG)
    parser.add_argument("--limit",     type=int, default=0)
    args = parser.parse_args()

    run_date = args.run_date or __import__("datetime").date.today().isoformat()

    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else PROJECT_ROOT / "data" / "unstructured" / "documents"
          / "doc_type=savings_book" / "run_date=2026-05-29"
    )
    if not input_dir.exists():
        print(f"ERROR: Input dir not found: {input_dir}")
        return 1

    output_csv = (
        Path(args.out)
        if args.out
        else PROJECT_ROOT / "data" / "unstructured" / "extracted"
          / f"savings_book_roi_extractions_{run_date}.csv"
    )

    print("=" * 60)
    print("OCR SAVINGS BOOK – TRANSACTION PAGE")
    print("=" * 60)
    print(f"Input : {input_dir}")
    print(f"Date  : {run_date}")
    print(f"Output: {output_csv}")
    print(f"Limit : {args.limit or 'all'}")

    success = process_directory(
        input_dir=input_dir,
        run_date=run_date,
        output_csv=output_csv,
        lang=args.lang,
        limit=args.limit or None,
    )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
