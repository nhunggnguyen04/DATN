"""
OCR Extraction Pipeline cho CCCD (Căn Cước Công Dân / ID Card)

Pipeline:
1. Load ảnh
2. Preprocess (deskew, warp, orient, CLAHE/sharpen)
3. OCR – label-anchored extraction + ROI fallback
4. Parse & Structure
5. Confidence Scoring
6. Save CSV

Fields extracted:
  full_name | id_number | date_of_birth | sex | nationality
  place_of_origin | place_of_residence | issue_date | expiry_date

Usage:
    python scripts/extract/ocr_extract_id_card.py --run-date 2026-05-29
    python scripts/extract/ocr_extract_id_card.py --input-dir data/... --run-date 2026-05-29 --limit 10
"""

from __future__ import annotations

import argparse
import re
import sys
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
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
# ROI definitions – fractions relative to warped image (out_w=1000)
# Source: ocr_extract_id_card.ipynb
# ---------------------------------------------------------------------------

@dataclass
class ROI:
    name: str
    x1: float
    y1: float
    x2: float
    y2: float


FRONT_ROIS = [
    ROI("full_name",           0.510, 0.200, 0.925, 0.265),
    ROI("id_number",           0.510, 0.285, 0.925, 0.330),
    ROI("date_of_birth",       0.510, 0.355, 0.725, 0.400),
    ROI("sex",                 0.510, 0.425, 0.635, 0.470),
    ROI("nationality",         0.510, 0.495, 0.775, 0.540),
    ROI("place_of_origin",     0.510, 0.565, 0.925, 0.610),
    ROI("place_of_residence",  0.510, 0.630, 0.965, 0.690),
    ROI("issue_date",          0.510, 0.700, 0.710, 0.745),
    ROI("expiry_date",         0.510, 0.765, 0.710, 0.810),
]

FIELD_LABELS = {
    "full_name":          ["FULL NAME"],
    "id_number":          ["DEMO ID NO", "ID NO"],
    "date_of_birth":      ["DATE OF BIRTH", "BIRTH"],
    "sex":                ["SEX"],
    "nationality":        ["NATIONALITY"],
    "place_of_origin":    ["PLACE OF ORIGIN", "ORIGIN"],
    "place_of_residence": ["PLACE OF RESIDENCE", "RESIDENCE"],
    "issue_date":         ["ISSUE DATE"],
    "expiry_date":        ["EXPIRY DATE", "EXPIRY"],
}

_WATERMARK_PATTERNS = [
    r"\bNOT\s+A\s+REAL\s+ID\b", r"\bREAL\s+ID\b", r"\bEAL\s*ID\b",
    r"\bEALID\b", r"\bRALID\b", r"\bALID\b", r"\bDEMO\b", r"\bSAMPLE\b",
]


# ---------------------------------------------------------------------------
# Image helpers
# ---------------------------------------------------------------------------

def normalize_width(bgr: np.ndarray, out_w: int = 1000) -> np.ndarray:
    h, w = bgr.shape[:2]
    if w == out_w:
        return bgr
    out_h = max(1, int(h * out_w / max(1, w)))
    return cv2.resize(bgr, (out_w, out_h),
                      interpolation=cv2.INTER_AREA if out_w < w else cv2.INTER_CUBIC)


def _rotate_bound(bgr: np.ndarray, angle: float, border=(255, 255, 255)) -> np.ndarray:
    h, w = bgr.shape[:2]
    cx, cy = w / 2.0, h / 2.0
    M = cv2.getRotationMatrix2D((cx, cy), angle, 1.0)
    cos, sin = abs(M[0, 0]), abs(M[0, 1])
    new_w = int(h * sin + w * cos)
    new_h = int(h * cos + w * sin)
    M[0, 2] += new_w / 2.0 - cx
    M[1, 2] += new_h / 2.0 - cy
    return cv2.warpAffine(bgr, M, (new_w, new_h), flags=cv2.INTER_CUBIC,
                          borderMode=cv2.BORDER_CONSTANT, borderValue=border)


def _skew_from_edges(edges: np.ndarray, min_len: int) -> float | None:
    lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=50,
                            minLineLength=min_len, maxLineGap=20)
    if lines is None:
        return None
    angles, weights = [], []
    for x1, y1, x2, y2 in lines[:, 0]:
        angle = float(np.degrees(np.arctan2(y2 - y1, x2 - x1)))
        angle = ((angle + 90.0) % 180.0) - 90.0
        if angle > 45.0:
            angle -= 90.0
        elif angle < -45.0:
            angle += 90.0
        if abs(angle) <= 30.0:
            angles.append(angle)
            weights.append(float(np.hypot(x2 - x1, y2 - y1)))
    if not angles:
        return None
    order = np.argsort(angles)
    cumsum = np.cumsum(np.asarray(weights, dtype=np.float32)[order])
    return float(np.asarray(angles, dtype=np.float32)[order][
        int(np.searchsorted(cumsum, cumsum[-1] * 0.5))])


def _estimate_skew(bgr: np.ndarray) -> float | None:
    h, w = bgr.shape[:2]
    min_len = max(80, int(min(h, w) * 0.25))
    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    blue_mask = cv2.inRange(hsv, np.array([85, 45, 20]), np.array([135, 255, 190]))
    blue_mask = cv2.morphologyEx(blue_mask, cv2.MORPH_CLOSE,
                                  np.ones((7, 7), np.uint8), iterations=2)
    angle = _skew_from_edges(cv2.Canny(blue_mask, 50, 150),
                              max(40, int(min(h, w) * 0.18)))
    if angle is not None:
        return angle
    gray = cv2.GaussianBlur(cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY), (5, 5), 0)
    edges = cv2.dilate(cv2.Canny(gray, 50, 150), np.ones((3, 3), np.uint8), iterations=1)
    return _skew_from_edges(edges, min_len)


def deskew(bgr: np.ndarray, min_abs: float = 0.75) -> tuple[np.ndarray, float]:
    angle = _estimate_skew(bgr)
    if angle is None or abs(angle) < min_abs:
        return bgr, 0.0
    candidates = [(0.0, bgr, angle)]
    for rot in (-angle, angle):
        rotated = _rotate_bound(bgr, rot)
        residual = _estimate_skew(rotated)
        candidates.append((rot, rotated, residual if residual is not None else 999.0))
    best_rot, best_img, _ = min(candidates, key=lambda t: abs(t[2]))
    return best_img, float(best_rot)


def _order_points(pts: np.ndarray) -> np.ndarray:
    rect = np.zeros((4, 2), dtype=np.float32)
    s = pts.sum(axis=1)
    rect[0], rect[2] = pts[np.argmin(s)], pts[np.argmax(s)]
    diff = np.diff(pts, axis=1)
    rect[1], rect[3] = pts[np.argmin(diff)], pts[np.argmax(diff)]
    return rect


def _is_card_quad(pts: np.ndarray, area_img: float) -> bool:
    rect = _order_points(pts)
    tl, tr, br, bl = rect
    w = max(np.linalg.norm(br - bl), np.linalg.norm(tr - tl))
    h = max(np.linalg.norm(tr - br), np.linalg.norm(tl - bl))
    aspect = max(w, h) / max(1.0, min(w, h))
    return cv2.contourArea(pts) / max(1.0, area_img) >= 0.25 and 1.25 <= aspect <= 1.95


def _find_quad(edges: np.ndarray, area_img: float,
               offset: tuple[int, int] = (0, 0)) -> np.ndarray | None:
    ox, oy = offset
    cnts, _ = cv2.findContours(edges, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
    for c in sorted(cnts, key=cv2.contourArea, reverse=True)[:40]:
        approx = cv2.approxPolyDP(c, 0.02 * cv2.arcLength(c, True), True)
        if len(approx) == 4:
            pts = approx.reshape(4, 2).astype(np.float32)
            pts[:, 0] -= ox
            pts[:, 1] -= oy
            if _is_card_quad(pts, area_img):
                return _order_points(pts)
    for c in sorted(cnts, key=cv2.contourArea, reverse=True)[:40]:
        if cv2.contourArea(c) <= 0:
            continue
        box = cv2.boxPoints(cv2.minAreaRect(c)).astype(np.float32)
        box[:, 0] -= ox
        box[:, 1] -= oy
        if _is_card_quad(box, area_img):
            return _order_points(box)
    return None


def find_document_quad(bgr: np.ndarray) -> np.ndarray | None:
    h, w = bgr.shape[:2]
    pad = max(20, int(min(h, w) * 0.04))
    padded = cv2.copyMakeBorder(bgr, pad, pad, pad, pad,
                                cv2.BORDER_CONSTANT, value=(255, 255, 255))
    gray = cv2.GaussianBlur(cv2.cvtColor(padded, cv2.COLOR_BGR2GRAY), (5, 5), 0)
    edges = cv2.dilate(
        cv2.morphologyEx(cv2.Canny(gray, 35, 120), cv2.MORPH_CLOSE,
                         np.ones((9, 9), np.uint8), iterations=2),
        np.ones((3, 3), np.uint8), iterations=1)
    quad = _find_quad(edges, h * w, offset=(pad, pad))
    if quad is not None:
        return quad
    gray = cv2.GaussianBlur(cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY), (5, 5), 0)
    edges = cv2.dilate(cv2.Canny(gray, 50, 150), np.ones((3, 3), np.uint8), iterations=1)
    return _find_quad(edges, h * w)


def warp_document(bgr: np.ndarray, quad: np.ndarray, out_w: int = 1000) -> np.ndarray:
    tl, tr, br, bl = quad
    max_w = int(max(np.linalg.norm(br - bl), np.linalg.norm(tr - tl)))
    max_h = int(max(np.linalg.norm(tr - br), np.linalg.norm(tl - bl)))
    out_h = int(max_h * out_w / max(1, max_w))
    dst = np.array([[0, 0], [out_w - 1, 0], [out_w - 1, out_h - 1], [0, out_h - 1]],
                   dtype=np.float32)
    return cv2.warpPerspective(bgr, cv2.getPerspectiveTransform(quad, dst), (out_w, out_h))


def is_plausible_front_card_image(bgr: np.ndarray) -> bool:
    h, w = bgr.shape[:2]
    return 0.50 <= h / max(1, w) <= 0.85


def normalize_front_orientation(bgr: np.ndarray, out_w: int = 1000) -> tuple[np.ndarray, int]:
    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    blue_mask = cv2.inRange(hsv, np.array([85, 45, 20]), np.array([135, 255, 180]))
    bh = max(1, int(blue_mask.shape[0] * 0.22))
    bw = max(1, int(blue_mask.shape[1] * 0.22))
    scores = {
        "top":    float((blue_mask[:bh] > 0).mean()),
        "bottom": float((blue_mask[-bh:] > 0).mean()),
        "left":   float((blue_mask[:, :bw] > 0).mean()),
        "right":  float((blue_mask[:, -bw:] > 0).mean()),
    }
    side = max(scores, key=scores.get)
    rot = 0
    if scores[side] > 0.03:
        if side == "bottom":
            bgr = cv2.rotate(bgr, cv2.ROTATE_180)
            rot = 180
        elif side == "left":
            bgr = cv2.rotate(bgr, cv2.ROTATE_90_CLOCKWISE)
            rot = 90
        elif side == "right":
            bgr = cv2.rotate(bgr, cv2.ROTATE_90_COUNTERCLOCKWISE)
            rot = 270
    return normalize_width(bgr, out_w=out_w), rot


def normalize_card_geometry(bgr: np.ndarray, out_w: int = 1000) -> np.ndarray:
    bgr = normalize_width(bgr, out_w=out_w)
    bgr, _ = deskew(bgr, min_abs=0.20)
    quad = find_document_quad(bgr)
    return warp_document(bgr, quad, out_w=out_w) if quad is not None else normalize_width(bgr, out_w=out_w)


def preprocess_for_ocr(bgr: np.ndarray) -> dict[str, np.ndarray]:
    gray    = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    clahe   = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
    denoise = cv2.fastNlMeansDenoising(clahe, None, h=10, templateWindowSize=7, searchWindowSize=21)
    sharpen = cv2.filter2D(denoise, -1,
                           np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]], dtype=np.float32))
    thresh  = cv2.adaptiveThreshold(sharpen, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                    cv2.THRESH_BINARY, 35, 10)
    return {"bgr": bgr, "gray": gray, "clahe": clahe,
            "denoise": denoise, "sharpen": sharpen, "thresh": thresh}


def scale_rois(rois: list[ROI], w: int, h: int) -> dict[str, tuple[int, int, int, int]]:
    return {r.name: (int(r.x1 * w), int(r.y1 * h),
                     int(r.x2 * w), int(r.y2 * h)) for r in rois}


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFC", str(text).strip())
    return re.sub(r"\s+", " ", re.sub(r"[`']", "", text.replace("|", "I"))).strip()


def strip_watermark(text: str) -> str:
    for p in _WATERMARK_PATTERNS:
        text = re.sub(p, " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def _accentless(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in decomposed
                   if unicodedata.category(ch) != "Mn"
                   ).replace("đ", "d").replace("Đ", "D").lower()


def _drop_watermark_tokens(text: str) -> str:
    kept = []
    for tok in clean_text(text).split():
        key = _accentless(tok).upper()
        if re.fullmatch(r"[HILN]{3,}", key) or re.fullmatch(r"(?:EALID|RALID|ALID)", key):
            continue
        kept.append(tok)
    return " ".join(kept).strip()


def parse_field(field: str, text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""
    if field in {"full_name", "place_of_origin", "place_of_residence"}:
        text = strip_watermark(text)
    key = _accentless(text)
    if field == "nationality" and "vietnam" in key.replace(" ", ""):
        return "Vietnam"
    if field == "sex":
        if re.search(r"\b(?:male|m)\b", key):
            return "Male"
        if re.search(r"\b(?:female|f)\b", key):
            return "Female"
    if field == "full_name":
        text = _drop_watermark_tokens(text)
        return " ".join(t[:1].upper() + t[1:].lower() for t in text.split())
    if field in {"place_of_origin", "place_of_residence"}:
        return re.sub(r"\s+", " ", text).strip()
    return text


def is_plausible(field: str, text: str) -> bool:
    text = clean_text(text)
    if not text:
        return False
    key = _accentless(text)
    if field == "id_number":
        return bool(re.search(r"(?:demo[-\s]*)?\d{6,}", text, flags=re.IGNORECASE))
    if field in {"date_of_birth", "issue_date", "expiry_date"}:
        return bool(re.search(r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{4}", text))
    if field == "sex":
        return bool(re.search(r"\b(?:male|female|m|f)\b", key))
    if field == "nationality":
        return "vietnam" in key.replace(" ", "")
    if field in {"full_name", "place_of_origin", "place_of_residence"}:
        return len(text) >= 2
    return True


def extract_user_id(path: Path) -> int | None:
    for part in path.parts:
        if part.startswith("user_id="):
            try:
                return int(part.split("=", 1)[1])
            except Exception:
                return None
    return None


# ---------------------------------------------------------------------------
# OCR dataclass & helpers
# ---------------------------------------------------------------------------

@dataclass
class OCRBox:
    text: str
    score: float
    x1: float
    y1: float
    x2: float
    y2: float

    @property
    def cx(self) -> float:
        return (self.x1 + self.x2) / 2.0

    @property
    def cy(self) -> float:
        return (self.y1 + self.y2) / 2.0

    @property
    def h(self) -> float:
        return max(1.0, self.y2 - self.y1)


def _box_to_xyxy(box) -> tuple | None:
    arr = np.asarray(box, dtype=np.float32)
    if arr.size == 4 and arr.ndim == 1:
        return tuple(arr.tolist())
    if arr.ndim >= 2 and arr.shape[-1] >= 2:
        pts = arr.reshape(-1, arr.shape[-1])[:, :2]
        return (float(np.min(pts[:, 0])), float(np.min(pts[:, 1])),
                float(np.max(pts[:, 0])), float(np.max(pts[:, 1])))
    return None


def ocr_predict_text(ocr_engine, img: np.ndarray) -> tuple[str, float | None]:
    bgr_img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR) if img.ndim == 2 else img
    results = ocr_engine.predict(bgr_img, use_doc_orientation_classify=False,
                                 use_doc_unwarping=False, use_textline_orientation=False)
    if not results:
        return "", None
    j = getattr(results[0], "json", None)
    if not isinstance(j, dict):
        return "", None
    res    = j.get("res", {})
    texts  = res.get("rec_texts",  []) or []
    scores = [float(s) for s in (res.get("rec_scores") or []) if s is not None]
    return " ".join(str(t) for t in texts).strip(), (float(np.mean(scores)) if scores else None)


def ocr_predict_boxes(ocr_engine, img: np.ndarray) -> list[OCRBox]:
    bgr_img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR) if img.ndim == 2 else img
    results = ocr_engine.predict(bgr_img, use_doc_orientation_classify=False,
                                 use_doc_unwarping=False, use_textline_orientation=False)
    if not results:
        return []
    j = getattr(results[0], "json", None)
    if not isinstance(j, dict):
        return []
    res    = j.get("res", {})
    texts  = res.get("rec_texts",  []) or []
    scores = res.get("rec_scores", []) or []
    boxes  = next(
        (res.get(k) for k in ("rec_boxes", "rec_polys", "dt_boxes", "dt_polys") if res.get(k)),
        [],
    )
    out: list[OCRBox] = []
    for i, text in enumerate(texts):
        if i >= len(boxes):
            continue
        xyxy = _box_to_xyxy(boxes[i])
        if xyxy is None:
            continue
        out.append(OCRBox(str(text).strip(),
                          float(scores[i] if i < len(scores) and scores[i] is not None else 0.0),
                          *xyxy))
    return out


# ---------------------------------------------------------------------------
# Label-anchored extraction
# ---------------------------------------------------------------------------

def _match_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _label_sim(text: str, label: str) -> float:
    tk, lk = _match_key(text), _match_key(label)
    if not tk or not lk:
        return 0.0
    return 1.0 if lk in tk else SequenceMatcher(None, tk, lk).ratio()


def _is_label(text: str) -> bool:
    return any(_label_sim(text, lbl) >= 0.78 for lbls in FIELD_LABELS.values() for lbl in lbls)


def _is_watermark(text: str) -> bool:
    key = _match_key(text)
    return any(p in key for p in ["notarealid", "realid", "sample", "alid"])


def _find_label_box(boxes: list[OCRBox], field: str) -> OCRBox | None:
    best, best_score = None, 0.0
    for box in boxes:
        score = max(_label_sim(box.text, lbl) for lbl in FIELD_LABELS[field])
        if score > best_score:
            best, best_score = box, score
    return best if best_score >= 0.68 else None


def _voverlap(a: OCRBox, b: OCRBox) -> float:
    return max(0.0, min(a.y2, b.y2) - max(a.y1, b.y1)) / max(1.0, min(a.h, b.h))


def extract_by_labels(boxes: list[OCRBox]) -> tuple[dict[str, str], dict[str, float]]:
    if not boxes:
        return {}, {}
    median_h = float(np.median([b.h for b in boxes]))
    raw: dict[str, str] = {}
    scores: dict[str, float] = {}
    for field in FIELD_LABELS:
        label_box = _find_label_box(boxes, field)
        if label_box is None:
            continue
        candidates = [
            b for b in boxes
            if b is not label_box
            and not _is_label(b.text)
            and not _is_watermark(b.text)
            and b.x1 >= label_box.x2 - 8
            and (_voverlap(label_box, b) >= 0.20
                 or abs(b.cy - label_box.cy) <= max(label_box.h, b.h, median_h) * 0.85)
        ]
        if not candidates:
            continue
        candidates.sort(key=lambda b: (b.x1, b.y1))
        if field == "id_number":
            candidates = [b for b in candidates
                          if re.search(r"(?:demo[-\s]*)?\d{6,}", b.text, re.IGNORECASE)][:1]
        elif field in {"date_of_birth", "issue_date", "expiry_date"}:
            candidates = [b for b in candidates
                          if re.search(r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{4}", b.text)][:1]
        if not candidates:
            continue
        text = " ".join(b.text for b in candidates).strip()
        if text:
            raw[field]    = text
            scores[field] = float(np.mean([b.score for b in candidates]))
    return raw, scores


# ---------------------------------------------------------------------------
# ROI fallback (especially for full_name)
# ---------------------------------------------------------------------------

def _binary_for_ocr(img: np.ndarray) -> np.ndarray:
    if img.ndim != 2:
        return img
    return cv2.bitwise_not(img) if float(img.mean()) < 127.0 else img


def _name_roi_variants(pp: dict, box: tuple[int, int, int, int]) -> list[tuple[str, np.ndarray]]:
    x1, y1, x2, y2 = box
    out: list[tuple[str, np.ndarray]] = []
    gray = pp.get("gray")
    if gray is not None:
        roi = gray[y1:y2, x1:x2]
        if roi.size:
            out.append(("gray", roi))
            blur = cv2.GaussianBlur(roi, (3, 3), 0)
            _, th = cv2.threshold(blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            out.append(("gray_otsu", _binary_for_ocr(th)))
    for key in ("denoise", "sharpen", "clahe"):
        base = pp.get(key)
        if base is not None:
            roi = base[y1:y2, x1:x2]
            if roi.size:
                out.append((key, roi))
    seen: set[str] = set()
    return [(k, v) for k, v in out if not (k in seen or seen.add(k))]  # type: ignore


def _choose_best(field: str,
                 candidates: list[tuple[str, str, float | None]]) -> tuple[str, float | None, str]:
    best_text, best_score, best_src = "", None, ""
    best_key = (-1, -1, -1.0, -1)
    for src, raw_text, score in candidates:
        cleaned = (strip_watermark(clean_text(raw_text))
                   if field in {"full_name", "place_of_origin", "place_of_residence"}
                   else clean_text(raw_text))
        corrected = parse_field(field, cleaned)
        plaus = 1 if is_plausible(field, corrected) else 0
        alpha = sum(ch.isalpha() for ch in corrected)
        s     = float(score) if score is not None else -1.0
        key   = (plaus, alpha, s, len(corrected))
        if key > best_key:
            best_key = key
            best_text, best_score, best_src = raw_text, score, src
    return best_text, best_score, best_src


def ocr_name_fallback(ocr_engine, pp: dict,
                      box: tuple[int, int, int, int]) -> tuple[str, float | None, str]:
    candidates: list[tuple[str, str, float | None]] = []
    for src, roi in _name_roi_variants(pp, box):
        t, s = ocr_predict_text(ocr_engine, roi)
        candidates.append((src, t, s))
        for scale in (2.0, 3.0):
            try:
                up = cv2.resize(roi, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
                t2, s2 = ocr_predict_text(ocr_engine, up)
                candidates.append((f"{src}_x{int(scale)}", t2, s2))
            except Exception:
                pass
    return _choose_best("full_name", candidates)


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
            warped = (warp_document(deskewed, quad, out_w=1000)
                      if quad is not None
                      else normalize_width(deskewed, out_w=1000))
            warped, _ = normalize_front_orientation(warped, out_w=1000)
            if not is_plausible_front_card_image(warped):
                warped, _ = normalize_front_orientation(bgr, out_w=1000)
            warped = normalize_card_geometry(warped, out_w=1000)
            pp = preprocess_for_ocr(warped)
            roi_boxes = scale_rois(FRONT_ROIS, warped.shape[1], warped.shape[0])

            # Step 3: OCR – label-anchored first, ROI fallback when implausible
            text_boxes = ocr_predict_boxes(ocr, warped)
            anchored_raw, anchored_scores = extract_by_labels(text_boxes)

            field_raw: dict[str, dict] = {}
            for field_name, box in roi_boxes.items():
                raw   = anchored_raw.get(field_name, "")
                score = anchored_scores.get(field_name)
                src   = "label_anchored"

                if not is_plausible(field_name, parse_field(field_name, raw)):
                    if field_name == "full_name":
                        raw, score, src = ocr_name_fallback(ocr, pp, box)
                    else:
                        roi_img = pp["clahe"][box[1]:box[3], box[0]:box[2]]
                        raw, score = ocr_predict_text(ocr, roi_img)
                        src = "roi_fallback"

                field_raw[field_name] = {"raw_text": raw, "score": score, "src": src}

            # Step 4: Parse & Structure
            parsed = {fn: parse_field(fn, payload["raw_text"])
                      for fn, payload in field_raw.items()}

            # Step 5: Confidence
            scores_list = [float(p["score"]) for p in field_raw.values()
                           if p.get("score") is not None]
            plaus = sum(1 for fn, pf in parsed.items() if is_plausible(fn, pf))
            total = len(field_raw)
            ocr_conf   = float(np.mean(scores_list)) if scores_list else 0.0
            parse_conf = plaus / max(1, total)
            final_conf = round(0.70 * ocr_conf + 0.30 * parse_conf, 4)

            row = {
                "file_name":          img_path.name,
                "file_path":          str(img_path),
                "run_date":           run_date,
                "user_id":            extract_user_id(img_path),
                "full_name":          parsed.get("full_name"),
                "id_number":          parsed.get("id_number"),
                "date_of_birth":      parsed.get("date_of_birth"),
                "sex":                parsed.get("sex"),
                "nationality":        parsed.get("nationality"),
                "place_of_origin":    parsed.get("place_of_origin"),
                "place_of_residence": parsed.get("place_of_residence"),
                "issue_date":         parsed.get("issue_date"),
                "expiry_date":        parsed.get("expiry_date"),
                "final_confidence":   final_conf,
                "ocr_confidence":     round(ocr_conf, 4),
                "parse_confidence":   round(parse_conf, 4),
                "plausible_fields":   plaus,
            }
            rows.append(row)
            print(f"  OK: {row['full_name']} | {row['id_number']} "
                  f"| {row['date_of_birth']} | conf={final_conf:.3f}")

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
                "ocr_confidence":   0.0,
                "parse_confidence": 0.0,
                "plausible_fields": 0,
            })

    df = pd.DataFrame(rows)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"\nSaved {len(df)} records → {output_csv}")
    print(f"Avg confidence: {df['final_confidence'].mean():.3f}")
    return True


def main():
    parser = argparse.ArgumentParser(description="OCR ID Card (CCCD)")
    parser.add_argument("--input-dir", default=None)
    parser.add_argument("--run-date",  default=None, help="YYYY-MM-DD")
    parser.add_argument("--out",       default=None, help="Output CSV path")
    parser.add_argument("--lang",      default=OCR_LANG)
    parser.add_argument("--limit",     type=int, default=0)
    args = parser.parse_args()

    run_date = args.run_date or __import__("datetime").date.today().isoformat()

    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else PROJECT_ROOT / "data" / "unstructured" / "documents"
          / "doc_type=id_card" / "run_date=2026-05-29"
    )
    if not input_dir.exists():
        print(f"ERROR: Input dir not found: {input_dir}")
        return 1

    output_csv = (
        Path(args.out)
        if args.out
        else PROJECT_ROOT / "data" / "unstructured" / "extracted"
          / f"id_card_extractions_{run_date}.csv"
    )

    print("=" * 60)
    print("OCR ID CARD (CCCD) – EXTRACTION PIPELINE")
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
