"""
OCR Extraction Pipeline cho CCCD (ID Card)

Pipeline:
1. Input image
2. Image Quality Check
3. Document Detection / Crop CCCD
4. Image Preprocessing
5. Document Classification (front/back)
6. Field Localization (template ROIs)
7. OCR Text Extraction (PaddleOCR)
8. Post-processing / Text Cleaning
9. Field Parsing & Validation
10. Deduplication / Business Rules
11. Output to console / CSV

Usage:
    python scripts/extract/ocr_extract_id_card.py
    python scripts/extract/ocr_extract_id_card.py --run-date 2026-05-13
"""

from __future__ import annotations

import argparse
import sys
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, Tuple, List, Dict, Any
import json

import cv2
import numpy as np
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    from paddleocr import PaddleOCR
except ImportError:  # Allows --help and static checks without OCR extras installed.
    PaddleOCR = None  # type: ignore[assignment]

# -----------------------------
# Configuration & Constants
# -----------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# ROI templates for FRONT side (normalized coordinates [0-1])
FRONT_ROIS = [
    # DEMO front layout: narrow value bands to avoid bleeding into adjacent rows.
    # full_name: slightly taller to preserve diacritics/strokes
    ("full_name", 0.510, 0.200, 0.925, 0.265),
    ("id_number", 0.510, 0.285, 0.925, 0.330),
    ("date_of_birth", 0.510, 0.355, 0.725, 0.400),
    ("sex", 0.510, 0.425, 0.635, 0.470),
    ("nationality", 0.510, 0.495, 0.775, 0.540),
    ("place_of_origin", 0.510, 0.565, 0.925, 0.610),
    ("place_of_residence", 0.510, 0.630, 0.965, 0.690),
    ("issue_date", 0.510, 0.700, 0.710, 0.745),
    ("expiry_date", 0.510, 0.765, 0.710, 0.810),
]

# ROI templates for BACK side (if needed)
BACK_ROIS = [
    # Example: add back-side fields if applicable
    # ("personal_identification", 0.15, 0.20, 0.85, 0.30),
]

# OCR language
OCR_LANG = "vi"  # Vietnamese if available, else 'en'
DEFAULT_RUN_DATE = "2026-05-13"
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "unstructured" / "documents" / "doc_type=id_card" / f"run_date={DEFAULT_RUN_DATE}"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "unstructured" / "extracted"
DEFAULT_LIMIT = 10
DEFAULT_OUTPUT_CSV = DEFAULT_OUTPUT_DIR / f"id_card_extracted_run_date={DEFAULT_RUN_DATE}_first{DEFAULT_LIMIT}.csv"


# -----------------------------
# Data Models
# -----------------------------

@dataclass
class ImageMetrics:
    width: int
    height: int
    blur_var_laplacian: float
    brightness_mean: float
    contrast_std: float
    pct_black: float
    pct_white: float

    def is_quality_ok(self, min_blur: float = 100.0, max_black: float = 0.3, max_white: float = 0.3) -> bool:
        """Check if image passes basic quality thresholds."""
        return (
            self.blur_var_laplacian >= min_blur
            and self.pct_black <= max_black
            and self.pct_white <= max_white
        )


@dataclass
class OCRResult:
    image_path: str
    doc_type: str  # 'front' or 'back'
    quality_metrics: ImageMetrics
    fields: Dict[str, FieldData]
    raw_texts: Dict[str, str]
    confidence_scores: Dict[str, float]
    processed_at: str

    def to_dict(self) -> dict:
        return {
            "image_path": self.image_path,
            "doc_type": self.doc_type,
            "processed_at": self.processed_at,
            **{f"raw_{k}": v for k, v in self.raw_texts.items()},
            **{f"conf_{k}": v for k, v in self.confidence_scores.items()},
            **{f"clean_{k}": v.cleaned_value for k, v in self.fields.items()},
            **{f"norm_{k}": v.normalized_value for k, v in self.fields.items()},
            **{f"valid_{k}": v.is_valid for k, v in self.fields.items()},
        }


@dataclass
class FieldData:
    raw_text: str
    cleaned_value: str
    normalized_value: Optional[str]
    is_valid: bool
    validation_msg: str
    confidence: Optional[float]


@dataclass
class OCRTextBox:
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


# -----------------------------
# Step 1: Image Quality Check
# -----------------------------

def compute_image_quality_metrics(bgr: np.ndarray) -> ImageMetrics:
    """Compute quality metrics for an image."""
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    h, w = gray.shape

    # Blur score: variance of Laplacian (higher = sharper)
    lap = cv2.Laplacian(gray, cv2.CV_64F)
    blur_var = float(lap.var())

    # Brightness/contrast
    brightness = float(gray.mean())
    contrast = float(gray.std())

    # Saturation check (fraction near white/black)
    pct_black = float((gray < 10).mean())
    pct_white = float((gray > 245).mean())

    return ImageMetrics(
        width=w,
        height=h,
        blur_var_laplacian=blur_var,
        brightness_mean=brightness,
        contrast_std=contrast,
        pct_black=pct_black,
        pct_white=pct_white,
    )


# -----------------------------
# Step 2: Document Detection / Crop
# -----------------------------

def rotate_bound_bgr(bgr: np.ndarray, angle_degrees: float, border_value: Tuple[int, int, int] = (255, 255, 255)) -> np.ndarray:
    """Rotate image without cropping corners."""
    h, w = bgr.shape[:2]
    center = (w / 2.0, h / 2.0)
    M = cv2.getRotationMatrix2D(center, angle_degrees, 1.0)

    cos = abs(M[0, 0])
    sin = abs(M[0, 1])
    new_w = int((h * sin) + (w * cos))
    new_h = int((h * cos) + (w * sin))

    M[0, 2] += (new_w / 2.0) - center[0]
    M[1, 2] += (new_h / 2.0) - center[1]

    return cv2.warpAffine(
        bgr,
        M,
        (new_w, new_h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=border_value,
    )


def _estimate_angle_from_edges(edges: np.ndarray, min_len: int) -> Optional[float]:
    """Estimate skew angle from an edge map."""
    lines = cv2.HoughLinesP(
        edges,
        rho=1,
        theta=np.pi / 180,
        threshold=50,
        minLineLength=min_len,
        maxLineGap=20,
    )
    if lines is None:
        return None

    angles: List[float] = []
    weights: List[float] = []
    for line in lines[:, 0]:
        x1, y1, x2, y2 = [int(v) for v in line]
        dx = x2 - x1
        dy = y2 - y1
        length = float(np.hypot(dx, dy))
        if length < min_len:
            continue

        angle = float(np.degrees(np.arctan2(dy, dx)))
        angle = ((angle + 90.0) % 180.0) - 90.0
        if angle > 45.0:
            angle -= 90.0
        elif angle < -45.0:
            angle += 90.0

        if abs(angle) <= 30.0:
            angles.append(angle)
            weights.append(length)

    if not angles:
        return None

    order = np.argsort(angles)
    sorted_angles = np.asarray(angles, dtype=np.float32)[order]
    sorted_weights = np.asarray(weights, dtype=np.float32)[order]
    cumsum = np.cumsum(sorted_weights)
    midpoint = float(cumsum[-1] * 0.5)
    return float(sorted_angles[int(np.searchsorted(cumsum, midpoint))])


def estimate_skew_angle_degrees(bgr: np.ndarray) -> Optional[float]:
    """Estimate skew against the closest horizontal/vertical axis using header and border lines."""
    h, w = bgr.shape[:2]
    min_len = max(80, int(min(h, w) * 0.25))

    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    blue_mask = cv2.inRange(hsv, np.array([85, 45, 20]), np.array([135, 255, 190]))
    blue_mask = cv2.morphologyEx(blue_mask, cv2.MORPH_CLOSE, np.ones((7, 7), np.uint8), iterations=2)
    blue_edges = cv2.Canny(blue_mask, 50, 150)
    blue_angle = _estimate_angle_from_edges(blue_edges, min_len=max(40, int(min(h, w) * 0.18)))
    if blue_angle is not None:
        return blue_angle

    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(gray, 50, 150)
    edges = cv2.dilate(edges, np.ones((3, 3), np.uint8), iterations=1)
    return _estimate_angle_from_edges(edges, min_len=min_len)


def deskew_input_image(bgr: np.ndarray, min_abs_angle: float = 0.75) -> Tuple[np.ndarray, float, Dict[str, Optional[float]]]:
    """Deskew the raw input image before document detection."""
    estimated = estimate_skew_angle_degrees(bgr)
    if estimated is None or abs(estimated) < min_abs_angle:
        return bgr, 0.0, {"estimated_skew": estimated, "residual_skew": estimated}

    candidates: List[Tuple[float, np.ndarray, Optional[float]]] = [(0.0, bgr, estimated)]
    for rotation in (-estimated, estimated):
        rotated = rotate_bound_bgr(bgr, rotation)
        residual = estimate_skew_angle_degrees(rotated)
        candidates.append((rotation, rotated, residual))

    best_rotation, best_image, best_residual = min(
        candidates,
        key=lambda item: abs(item[2]) if item[2] is not None else 999.0,
    )
    return best_image, float(best_rotation), {
        "estimated_skew": estimated,
        "residual_skew": best_residual,
    }


def _order_points_clockwise(pts: np.ndarray) -> np.ndarray:
    """Order 4 points as tl, tr, br, bl."""
    rect = np.zeros((4, 2), dtype=np.float32)
    s = pts.sum(axis=1)
    rect[0] = pts[np.argmin(s)]  # tl
    rect[2] = pts[np.argmax(s)]  # br
    diff = np.diff(pts, axis=1)
    rect[1] = pts[np.argmin(diff)]  # tr
    rect[3] = pts[np.argmax(diff)]  # bl
    return rect


def _quad_aspect_ratio(pts: np.ndarray) -> float:
    rect = _order_points_clockwise(pts)
    tl, tr, br, bl = rect
    width = max(np.linalg.norm(br - bl), np.linalg.norm(tr - tl))
    height = max(np.linalg.norm(tr - br), np.linalg.norm(tl - bl))
    return float(max(width, height) / max(1.0, min(width, height)))


def _is_plausible_card_quad(pts: np.ndarray, area_img: float, min_area_ratio: float = 0.25) -> bool:
    area_ratio = cv2.contourArea(pts.astype(np.float32)) / max(1.0, area_img)
    aspect = _quad_aspect_ratio(pts)
    return area_ratio >= min_area_ratio and 1.25 <= aspect <= 1.95


def _find_quad_from_edges(edges: np.ndarray, area_img: float, offset_xy: Tuple[int, int] = (0, 0)) -> Tuple[Optional[np.ndarray], Dict[str, Any]]:
    contours, _ = cv2.findContours(edges, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)
    contours = sorted(contours, key=cv2.contourArea, reverse=True)
    debug: Dict[str, Any] = {
        "candidate_areas": [float(cv2.contourArea(c) / max(1.0, area_img)) for c in contours[:8]]
    }
    ox, oy = offset_xy

    for c in contours[:40]:
        peri = cv2.arcLength(c, True)
        approx = cv2.approxPolyDP(c, 0.02 * peri, True)

        if len(approx) == 4:
            pts = approx.reshape(4, 2).astype(np.float32)
            pts[:, 0] -= ox
            pts[:, 1] -= oy
            if _is_plausible_card_quad(pts, area_img):
                debug["quad_method"] = "approx"
                debug["quad_area_ratio"] = float(cv2.contourArea(pts) / max(1.0, area_img))
                return _order_points_clockwise(pts), debug

    for c in contours[:40]:
        if cv2.contourArea(c) <= 0:
            continue

        rect = cv2.minAreaRect(c)
        box = cv2.boxPoints(rect).astype(np.float32)
        box[:, 0] -= ox
        box[:, 1] -= oy
        if _is_plausible_card_quad(box, area_img):
            debug["quad_method"] = "min_area_rect"
            debug["quad_area_ratio"] = float(cv2.contourArea(box) / max(1.0, area_img))
            return _order_points_clockwise(box), debug

    return None, debug


def find_document_quad(bgr: np.ndarray) -> Tuple[Optional[np.ndarray], Dict[str, Any]]:
    """Detect document contour and return 4 corner points."""
    h, w = bgr.shape[:2]
    pad = max(20, int(min(h, w) * 0.04))
    padded = cv2.copyMakeBorder(
        bgr,
        pad,
        pad,
        pad,
        pad,
        cv2.BORDER_CONSTANT,
        value=(255, 255, 255),
    )
    gray = cv2.cvtColor(padded, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)

    edges = cv2.Canny(gray, 35, 120)
    edges = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, np.ones((9, 9), np.uint8), iterations=2)
    edges = cv2.dilate(edges, np.ones((3, 3), np.uint8), iterations=1)

    area_img = float(h * w)
    quad, debug = _find_quad_from_edges(edges, area_img=area_img, offset_xy=(pad, pad))
    debug["edges"] = edges[pad:pad + h, pad:pad + w]
    if quad is not None:
        debug["quad_method"] = f"padded_{debug.get('quad_method')}"
        return quad, debug

    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (5, 5), 0)

    edges = cv2.Canny(gray, 50, 150)
    edges = cv2.dilate(edges, np.ones((3, 3), np.uint8), iterations=1)

    quad, debug = _find_quad_from_edges(edges, area_img=area_img)
    debug["edges"] = edges
    if quad is not None:
        return quad, debug

    debug["quad_method"] = "none_large_enough"
    return None, debug


def warp_document(bgr: np.ndarray, quad: np.ndarray, out_w: int = 1000) -> np.ndarray:
    """Apply perspective warp to crop and normalize document."""
    (tl, tr, br, bl) = quad

    widthA = np.linalg.norm(br - bl)
    widthB = np.linalg.norm(tr - tl)
    maxW = int(max(widthA, widthB))

    heightA = np.linalg.norm(tr - br)
    heightB = np.linalg.norm(tl - bl)
    maxH = int(max(heightA, heightB))

    scale = out_w / max(1, maxW)
    out_h = int(maxH * scale)

    dst = np.array(
        [[0, 0], [out_w - 1, 0], [out_w - 1, out_h - 1], [0, out_h - 1]],
        dtype=np.float32,
    )
    M = cv2.getPerspectiveTransform(quad, dst)
    warped = cv2.warpPerspective(bgr, M, (out_w, out_h))
    return warped


def normalize_width(bgr_doc: np.ndarray, out_w: int = 1000) -> np.ndarray:
    """Resize document to a fixed width while preserving aspect ratio."""
    h, w = bgr_doc.shape[:2]
    if w == out_w:
        return bgr_doc

    out_h = max(1, int(h * (out_w / max(1, w))))
    return cv2.resize(bgr_doc, (out_w, out_h), interpolation=cv2.INTER_AREA)


def is_plausible_front_card_image(bgr_doc: np.ndarray) -> bool:
    """Check final upright front-card aspect before applying template ROIs."""
    h, w = bgr_doc.shape[:2]
    aspect = h / max(1, w)
    return 0.50 <= aspect <= 0.85


def normalize_front_orientation(bgr_doc: np.ndarray, out_w: int = 1000) -> Tuple[np.ndarray, int, Dict[str, float]]:
    """Rotate DEMO front-side ID card upright by locating the blue title band."""
    hsv = cv2.cvtColor(bgr_doc, cv2.COLOR_BGR2HSV)
    blue_mask = cv2.inRange(hsv, np.array([85, 45, 20]), np.array([135, 255, 180]))

    h, w = blue_mask.shape
    band_h = max(1, int(h * 0.22))
    band_w = max(1, int(w * 0.22))

    top_blue = float((blue_mask[:band_h] > 0).mean())
    bottom_blue = float((blue_mask[-band_h:] > 0).mean())
    left_blue = float((blue_mask[:, :band_w] > 0).mean())
    right_blue = float((blue_mask[:, -band_w:] > 0).mean())

    side_scores = {
        "top_blue": top_blue,
        "bottom_blue": bottom_blue,
        "left_blue": left_blue,
        "right_blue": right_blue,
    }
    header_side = max(side_scores, key=side_scores.get)

    rotated = bgr_doc
    rotation_degrees = 0
    if side_scores[header_side] > 0.03:
        if header_side == "bottom_blue":
            rotated = cv2.rotate(bgr_doc, cv2.ROTATE_180)
            rotation_degrees = 180
        elif header_side == "left_blue":
            rotated = cv2.rotate(bgr_doc, cv2.ROTATE_90_CLOCKWISE)
            rotation_degrees = 90
        elif header_side == "right_blue":
            rotated = cv2.rotate(bgr_doc, cv2.ROTATE_90_COUNTERCLOCKWISE)
            rotation_degrees = 270

    return normalize_width(rotated, out_w=out_w), rotation_degrees, side_scores


def normalize_card_geometry_for_rois(bgr_doc: np.ndarray, out_w: int = 1000) -> Tuple[np.ndarray, Dict[str, Optional[float]]]:
    """Apply a final small deskew before template ROI placement."""
    normalized = normalize_width(bgr_doc, out_w=out_w)
    deskewed, deskew_degrees, deskew_scores = deskew_input_image(normalized, min_abs_angle=0.20)
    quad, quad_debug = find_document_quad(deskewed)
    if quad is not None:
        deskewed = warp_document(deskewed, quad, out_w=out_w)
    else:
        deskewed = normalize_width(deskewed, out_w=out_w)

    return deskewed, {
        "roi_deskew_degrees": deskew_degrees,
        "roi_estimated_skew": deskew_scores.get("estimated_skew"),
        "roi_residual_skew": deskew_scores.get("residual_skew"),
        "roi_quad_area_ratio": quad_debug.get("quad_area_ratio"),
    }


# -----------------------------
# Step 3: Image Preprocessing
# -----------------------------

def preprocess_for_ocr(bgr_doc: np.ndarray) -> Dict[str, np.ndarray]:
    """Preprocess image for OCR: gray, CLAHE, denoise, sharpen, threshold."""
    out = {}
    gray = cv2.cvtColor(bgr_doc, cv2.COLOR_BGR2GRAY)
    out["gray"] = gray

    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    cl = clahe.apply(gray)
    out["clahe"] = cl

    dn = cv2.fastNlMeansDenoising(cl, None, h=10, templateWindowSize=7, searchWindowSize=21)
    out["denoise"] = dn

    kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]], dtype=np.float32)
    sh = cv2.filter2D(dn, -1, kernel)
    out["sharpen"] = sh

    th = cv2.adaptiveThreshold(
        sh,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        35,
        10,
    )
    out["thresh"] = th

    return out


# -----------------------------
# Step 4: Document Classification
# -----------------------------

def classify_document(warped: np.ndarray, quad: Optional[np.ndarray] = None) -> str:
    """
    Classify document as 'front' or 'back'.
    Simple heuristic: detect presence of specific text patterns or visual features.
    For now, default to 'front'. Can be enhanced with ML or template matching.
    """
    # TODO: Implement classification logic
    # - Check for text "CCCD" / "CĂN CƯC CÔNG DÂN"
    # - Check for photo region (face detection)
    # - Check for specific layout patterns
    return "front"


# -----------------------------
# Step 5: Field Localization
# -----------------------------

@dataclass
class ROI:
    name: str
    x1: float
    y1: float
    x2: float
    y2: float


def scale_rois(rois: List[ROI], w: int, h: int) -> List[Tuple[str, Tuple[int, int, int, int]]]:
    """Scale normalized ROI coordinates to actual pixel values."""
    out = []
    for r in rois:
        x1 = int(r.x1 * w)
        y1 = int(r.y1 * h)
        x2 = int(r.x2 * w)
        y2 = int(r.y2 * h)
        out.append((r.name, (x1, y1, x2, y2)))
    return out


def get_rois_for_doc_type(doc_type: str, h: int, w: int) -> Dict[str, Tuple[int, int, int, int]]:
    """Get scaled ROI boxes for document type."""
    if doc_type == "front":
        base_rois = [ROI(name, x1, y1, x2, y2) for name, x1, y1, x2, y2 in FRONT_ROIS]
    elif doc_type == "back":
        base_rois = [ROI(name, x1, y1, x2, y2) for name, x1, y1, x2, y2 in BACK_ROIS]
    else:
        raise ValueError(f"Unknown doc_type: {doc_type}")

    scaled = scale_rois(base_rois, w=w, h=h)
    return {name: box for name, box in scaled}


def roi_crop_for_field(preprocessed: Dict[str, np.ndarray], field_name: str, box: Tuple[int, int, int, int]) -> np.ndarray:
    """Pick preprocessed variant per field to reduce artefacts (e.g., watermark)."""
    x1, y1, x2, y2 = box
    # CLAHE can amplify faint watermark; gray is often cleaner for the name line.
    key = "gray" if field_name == "full_name" else "clahe"
    base = preprocessed.get(key) if isinstance(preprocessed, dict) else None
    if base is None:
        base = preprocessed["clahe"]
    return base[y1:y2, x1:x2]


def roi_crop_variants_for_field(
    preprocessed: Dict[str, np.ndarray],
    field_name: str,
    box: Tuple[int, int, int, int],
) -> List[Tuple[str, np.ndarray]]:
    """Return multiple ROI variants for a field (used for hard cases like full_name)."""
    x1, y1, x2, y2 = box

    candidates: List[Tuple[str, np.ndarray]] = []

    if field_name == "full_name":
        for key in ("gray", "denoise", "sharpen", "clahe"):
            base = preprocessed.get(key)
            if base is None:
                continue
            roi = base[y1:y2, x1:x2]
            if roi.size:
                candidates.append((key, roi))
        # Deduplicate by key order (keep first occurrence)
        seen = set()
        deduped: List[Tuple[str, np.ndarray]] = []
        for k, roi in candidates:
            if k in seen:
                continue
            seen.add(k)
            deduped.append((k, roi))
        return deduped

    return [("default", roi_crop_for_field(preprocessed, field_name, box))]


# -----------------------------
# Step 6: OCR Text Extraction
# -----------------------------

def init_ocr_engine(lang: str = OCR_LANG) -> Any:
    """Initialize PaddleOCR engine."""
    if PaddleOCR is None:
        raise RuntimeError(
            "PaddleOCR is not installed in this Python environment. "
            "Install OCR dependencies first, for example: pip install paddlepaddle==3.2.2 paddleocr"
        )
    # PP-OCRv3 tends to be more robust for missing vowels on small ROIs.
    return PaddleOCR(lang=lang, ocr_version="PP-OCRv3", use_textline_orientation=False)


def ocr_predict_text(ocr_engine: Any, img: np.ndarray) -> Tuple[str, Optional[float], int]:
    """Run OCR on a single image region. Returns (text, mean_score, n_lines)."""
    bgr_img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR) if img.ndim == 2 else img
    results = ocr_engine.predict(
        bgr_img,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )
    if not results:
        return "", None, 0

    j = getattr(results[0], "json", None)
    if not isinstance(j, dict):
        return "", None, 0

    res = j.get("res", {})
    texts = res.get("rec_texts", []) or []
    scores = res.get("rec_scores", []) or []

    text = " ".join([str(t) for t in texts]).strip()
    score_vals = [float(s) for s in scores if s is not None]
    mean_score = float(np.mean(score_vals)) if score_vals else None
    return text, mean_score, len(texts)


def _box_to_xyxy(box: Any) -> Optional[Tuple[float, float, float, float]]:
    arr = np.asarray(box, dtype=np.float32)
    if arr.size == 4 and arr.ndim == 1:
        x1, y1, x2, y2 = arr.tolist()
        return float(x1), float(y1), float(x2), float(y2)

    if arr.ndim >= 2 and arr.shape[-1] >= 2:
        pts = arr.reshape(-1, arr.shape[-1])[:, :2]
        return (
            float(np.min(pts[:, 0])),
            float(np.min(pts[:, 1])),
            float(np.max(pts[:, 0])),
            float(np.max(pts[:, 1])),
        )

    return None


def _first_nonempty_ocr_boxes(res: Dict[str, Any]) -> Any:
    for key in ("rec_boxes", "rec_polys", "dt_boxes", "dt_polys"):
        boxes = res.get(key)
        if boxes is None:
            continue
        try:
            if len(boxes) > 0:
                return boxes
        except TypeError:
            continue
    return []


def ocr_predict_text_boxes(ocr_engine: Any, img: np.ndarray) -> List[OCRTextBox]:
    """Run OCR on the whole normalized card and return recognized text boxes."""
    bgr_img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR) if img.ndim == 2 else img
    results = ocr_engine.predict(
        bgr_img,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )
    if not results:
        return []

    j = getattr(results[0], "json", None)
    if not isinstance(j, dict):
        return []

    res = j.get("res", {})
    texts = res.get("rec_texts", []) or []
    scores = res.get("rec_scores", []) or []
    boxes = _first_nonempty_ocr_boxes(res)

    out: List[OCRTextBox] = []
    for i, text in enumerate(texts):
        if i >= len(boxes):
            continue

        xyxy = _box_to_xyxy(boxes[i])
        if xyxy is None:
            continue

        score = float(scores[i]) if i < len(scores) and scores[i] is not None else 0.0
        x1, y1, x2, y2 = xyxy
        clean = str(text).strip()
        if clean:
            out.append(OCRTextBox(clean, score, x1, y1, x2, y2))

    return out


FIELD_LABELS: Dict[str, List[str]] = {
    "full_name": ["FULL NAME"],
    "id_number": ["DEMO ID NO", "ID NO"],
    "date_of_birth": ["DATE OF BIRTH", "BIRTH"],
    "sex": ["SEX"],
    "nationality": ["NATIONALITY"],
    "place_of_origin": ["PLACE OF ORIGIN", "ORIGIN"],
    "place_of_residence": ["PLACE OF RESIDENCE", "RESIDENCE"],
    "issue_date": ["ISSUE DATE"],
    "expiry_date": ["EXPIRY DATE", "EXPIRY"],
}


def _match_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _label_similarity(text: str, label: str) -> float:
    text_key = _match_key(text)
    label_key = _match_key(label)
    if not text_key or not label_key:
        return 0.0
    if label_key in text_key:
        return 1.0
    return SequenceMatcher(None, text_key, label_key).ratio()


def _contains_demo_watermark_phrase(text: str) -> bool:
    """Detect common demo watermark text that can bleed into OCR results."""
    key = _match_key(text)
    if not key:
        return False
    return (
        "notarealid" in key
        or ("real" in key and "id" in key)
        or "demo" in key
        or "sample" in key
    )


def _is_label_text(text: str) -> bool:
    return any(_label_similarity(text, label) >= 0.78 for labels in FIELD_LABELS.values() for label in labels)


def _find_label_box(text_boxes: List[OCRTextBox], field_name: str) -> Optional[OCRTextBox]:
    best: Optional[OCRTextBox] = None
    best_score = 0.0
    for box in text_boxes:
        score = max(_label_similarity(box.text, label) for label in FIELD_LABELS[field_name])
        if score > best_score:
            best = box
            best_score = score

    return best if best is not None and best_score >= 0.68 else None


def _vertical_overlap(a: OCRTextBox, b: OCRTextBox) -> float:
    overlap = max(0.0, min(a.y2, b.y2) - max(a.y1, b.y1))
    return overlap / max(1.0, min(a.h, b.h))


def extract_fields_by_label_anchors(text_boxes: List[OCRTextBox], image_shape: Tuple[int, int, int]) -> Tuple[Dict[str, str], Dict[str, float]]:
    """Extract fields from whole-card OCR boxes by locating labels and reading values to their right."""
    if not text_boxes:
        return {}, {}

    h, w = image_shape[:2]
    median_h = float(np.median([b.h for b in text_boxes])) if text_boxes else 20.0
    raw_texts: Dict[str, str] = {}
    conf_scores: Dict[str, float] = {}

    for field_name in FIELD_LABELS:
        label_box = _find_label_box(text_boxes, field_name)
        if label_box is None:
            continue

        candidates: List[OCRTextBox] = []
        for box in text_boxes:
            if box is label_box or _is_label_text(box.text):
                continue
            if _contains_demo_watermark_phrase(box.text):
                continue
            if box.x1 < label_box.x2 - 8:
                continue
            if box.x1 > w * 0.98:
                continue
            same_row = _vertical_overlap(label_box, box) >= 0.20
            near_row = abs(box.cy - label_box.cy) <= max(label_box.h, box.h, median_h) * 0.85
            if same_row or near_row:
                candidates.append(box)

        if not candidates:
            continue

        candidates = sorted(candidates, key=lambda b: (b.x1, b.y1))
        if field_name == "id_number":
            id_candidates = [
                b for b in candidates
                if re.search(r"(?:demo[-\s]*)?\d{6,}", b.text, flags=re.IGNORECASE)
            ]
            if not id_candidates:
                continue
            candidates = id_candidates[:1]
        elif field_name in {"date_of_birth", "issue_date", "expiry_date"}:
            date_candidates = [b for b in candidates if re.search(r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{4}", b.text)]
            if date_candidates:
                candidates = date_candidates[:1]
            else:
                continue
        else:
            candidates = [b for b in candidates if abs(b.cy - label_box.cy) <= max(label_box.h, b.h, median_h) * 1.10]

        text = " ".join(b.text for b in candidates).strip()
        if text:
            raw_texts[field_name] = text
            conf_scores[field_name] = float(np.mean([b.score for b in candidates]))

    return raw_texts, conf_scores


# -----------------------------
# Step 7: Post-processing / Text Cleaning
# -----------------------------

def clean_text(text: str) -> str:
    """Clean OCR raw text: remove extra spaces, normalize unicode, remove noise."""
    if not text:
        return ""

    # Strip leading/trailing whitespace
    text = text.strip()

    # Normalize unicode (compose/decompose)
    text = unicodedata.normalize("NFC", text)

    # Remove multiple spaces
    text = re.sub(r'\s+', ' ', text)

    # Remove common noise characters from OCR
    text = re.sub(r'[|l]{2,}', 'I', text)  # Replace ||| with I
    text = re.sub(r'[`\']', '', text)  # Remove backticks/quotes

    return text.strip()


def strip_demo_watermark_text(text: str) -> str:
    """Remove common demo watermark fragments (helps for full_name/address fields)."""
    if not text:
        return ""
    text = re.sub(r"\bNOT\s+A\s+REAL\s+ID\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bREAL\s+ID\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bDEMO\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bSAMPLE\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


VIETNAMESE_TOKEN_CORRECTIONS: Dict[str, str] = {
    "ba": "Bà",
    "bà": "Bà",
    "an": "An",
    "le": "Lê",
    "lé": "Lê",
    "lê": "Lê",
    "viet": "Việt",
    "viét": "Việt",
    "việt": "Việt",
    "nam": "Nam",
    "nu": "Nữ",
    "nữ": "Nữ",
    "duong": "Đường",
    "đuong": "Đường",
    "đường": "Đường",
    "thi": "Thị",
    "thị": "Thị",
    "xa": "xã",
    "xã": "xã",
    "lang": "Láng",
    "láng": "Láng",
    "phuong": "Phường",
    "phường": "Phường",
}


def _accentless_key(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text)
    no_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return no_marks.replace("đ", "d").replace("Đ", "D").lower()


def _correct_tokens(text: str) -> str:
    parts = re.split(r"(\W+)", text)
    corrected: List[str] = []
    for part in parts:
        if not part or re.fullmatch(r"\W+", part):
            corrected.append(part)
            continue

        key = part.lower()
        accentless = _accentless_key(part)
        replacement = VIETNAMESE_TOKEN_CORRECTIONS.get(key) or VIETNAMESE_TOKEN_CORRECTIONS.get(accentless)
        corrected.append(replacement if replacement else part)

    return "".join(corrected)


def correct_vietnamese_ocr_text(field_name: str, text: str) -> str:
    """Field-aware correction for common Vietnamese OCR accent mistakes."""
    if not text:
        return ""

    text = unicodedata.normalize("NFC", text)
    compact = _accentless_key(text)

    if field_name == "nationality" and "viet" in compact and "nam" in compact:
        return "Việt Nam"

    if field_name == "sex":
        if re.search(r"\bnam\b", compact):
            return "Nam"
        if re.search(r"\bnu\b", compact):
            return "Nữ"

    if field_name in {"full_name", "place_of_origin", "place_of_residence"}:
        text = _correct_tokens(text)
        if field_name == "full_name":
            return " ".join(token[:1].upper() + token[1:] for token in text.split())
        return re.sub(r"\s+", " ", text).strip()

    return text


def is_plausible_field_value(field_name: str, text: str) -> bool:
    """Fast plausibility check to decide between anchored vs ROI OCR texts."""
    text = clean_text(text)
    if not text:
        return False

    if field_name == "id_number":
        return bool(re.search(r"(?:demo[-\s]*)?\d{6,}", text, flags=re.IGNORECASE))

    if field_name in {"date_of_birth", "issue_date", "expiry_date"}:
        return bool(re.search(r"\d{1,2}[/\-.]\d{1,2}[/\-.]\d{4}", text))

    if field_name in {"full_name", "place_of_origin", "place_of_residence"}:
        return len(text) >= 2

    return True


def _choose_best_text_candidate(field_name: str, candidates: List[Tuple[str, str, Optional[float]]]) -> Tuple[str, Optional[float]]:
    """Pick best OCR candidate for a field based on heuristics + OCR score."""
    best_text = ""
    best_score: Optional[float] = None
    best_key = (-1, -1, -1.0, -1)

    for _src, raw_text, mean_score in candidates:
        cleaned = clean_text(raw_text)
        if field_name in {"full_name", "place_of_origin", "place_of_residence"}:
            cleaned = strip_demo_watermark_text(cleaned)

        corrected = correct_vietnamese_ocr_text(field_name, cleaned)
        plausible = 1 if is_plausible_field_value(field_name, corrected) else 0
        alpha_count = sum(ch.isalpha() for ch in corrected)
        score_val = float(mean_score) if mean_score is not None else -1.0
        length = len(corrected)

        key = (plausible, alpha_count, score_val, length)
        if key > best_key:
            best_key = key
            best_text = raw_text
            best_score = mean_score

    return best_text, best_score


def ocr_predict_text_multi(
    ocr_engine: Any,
    field_name: str,
    roi_variants: List[Tuple[str, np.ndarray]],
) -> Tuple[str, Optional[float], str]:
    """OCR a field using multiple ROI variants + upscaling and select the best result."""
    candidates: List[Tuple[str, str, Optional[float]]] = []

    for src, img in roi_variants:
        if img is None or img.size == 0:
            continue

        # Pass 1: as-is
        text, score, _ = ocr_predict_text(ocr_engine, img)
        candidates.append((f"{src}", text, score))

        # Pass 2: upscale (often helps missing vowels/diacritics on small ROIs)
        try:
            up = cv2.resize(img, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
            text2, score2, _ = ocr_predict_text(ocr_engine, up)
            candidates.append((f"{src}_x2", text2, score2))
        except Exception:
            pass

    best_text, best_score = _choose_best_text_candidate(field_name, candidates)
    best_src = "multi"
    if candidates:
        # Determine best source label (optional, for debugging)
        best_text_norm = best_text
        for src, t, s in candidates:
            if t == best_text_norm and s == best_score:
                best_src = src
                break

    return best_text, best_score, best_src


# -----------------------------
# Step 8: Field Parsing & Validation
# -----------------------------

def parse_and_validate_field(field_name: str, raw_text: str, confidence: Optional[float]) -> FieldData:
    """Parse and validate a specific field."""
    cleaned = clean_text(raw_text)
    if field_name in {"full_name", "place_of_origin", "place_of_residence"}:
        cleaned = strip_demo_watermark_text(cleaned)
    cleaned = correct_vietnamese_ocr_text(field_name, cleaned)
    parsed: Optional[str] = None
    is_valid = True
    msg = "OK"

    if not cleaned:
        return FieldData(raw_text, "", None, False, "EMPTY_FIELD", confidence)

    if field_name == "id_number":
        # CCCD number: exactly 12 digits
        digits = re.sub(r'\D', '', cleaned)
        if re.search(r"\bdemo[-\s]*\d{6,}\b", cleaned, flags=re.IGNORECASE):
            parsed = re.search(r"\bdemo[-\s]*\d{6,}\b", cleaned, flags=re.IGNORECASE).group(0).upper().replace(" ", "")
        elif len(digits) == 12 and digits.isdigit():
            parsed = digits
        else:
            is_valid = False
            msg = f"INVALID_ID_LENGTH (got {len(digits)} digits)"

    elif field_name in ["date_of_birth", "issue_date", "expiry_date"]:
        # Expected format: DD/MM/YYYY or similar
        parsed = parse_vietnamese_date(cleaned)
        if parsed:
            try:
                dt = datetime.strptime(parsed, "%Y-%m-%d")
                # Check reasonable ranges
                if field_name == "date_of_birth":
                    if dt.year < 1900 or dt.year > 2025:
                        is_valid = False
                        msg = "UNREASONABLE_BIRTH_YEAR"
                elif field_name == "issue_date":
                    if dt.year < 2000:
                        is_valid = False
                        msg = "ISSUE_DATE_TOO_OLD"
            except ValueError:
                is_valid = False
                msg = "INVALID_DATE_FORMAT"
        else:
            is_valid = False
            msg = "CANNOT_PARSE_DATE"

    elif field_name == "sex":
        # Normalize: "Nam" / "Nữ" / "Nam/Nữ"
        lowered = cleaned.lower()
        if "nam" in lowered and "nữ" not in lowered:
            parsed = "Nam"
        elif "nữ" in lowered and "nam" not in lowered:
            parsed = "Nữ"
        else:
            parsed = cleaned.title()
            is_valid = False
            msg = "AMBIGUOUS_SEX"

    elif field_name == "nationality":
        # Expect "Việt Nam" or similar
        if "việt" in cleaned.lower() or "viet" in cleaned.lower():
            parsed = "Việt Nam"
        else:
            parsed = cleaned.title()

    elif field_name in ["full_name", "place_of_origin", "place_of_residence"]:
        # Name/address: just cleaned, maybe remove extra spaces
        parsed = cleaned
        # Additional validation: length > 0, not just noise
        if len(parsed) < 2:
            is_valid = False
            msg = "TOO_SHORT"

    else:
        # Unknown field: pass through
        parsed = cleaned

    return FieldData(
        raw_text=raw_text,
        cleaned_value=cleaned,
        normalized_value=parsed,
        is_valid=is_valid,
        validation_msg=msg if not is_valid else "OK",
        confidence=confidence,
    )


def parse_vietnamese_date(text: str) -> Optional[str]:
    """
    Parse Vietnamese date formats to YYYY-MM-DD.
    Tries: DD/MM/YYYY, DD-MM-YYYY, DD.MM.YYYY, etc.
    """
    patterns = [
        r'(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})',  # DD/MM/YYYY
        r'(\d{4})[/\-\.](\d{1,2})[/\-\.](\d{1,2})',  # YYYY/MM/DD
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            groups = m.groups()
            if len(groups[0]) == 4:  # YYYY first
                y, mth, d = groups
            else:  # DD/MM/YYYY
                d, mth, y = groups
            try:
                dt = datetime(int(y), int(mth), int(d))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


# -----------------------------
# Step 9: Deduplication / Business Rules
# -----------------------------

def apply_business_rules(results: List[OCRResult]) -> List[OCRResult]:
    """
    Apply deduplication and business rules to a batch of results.
    - Remove duplicates (same image_path)
    - Validate cross-field consistency
    """
    seen_paths = set()
    filtered = []
    for r in results:
        if r.image_path in seen_paths:
            print(f"WARNING: Duplicate image_path skipped: {r.image_path}")
            continue
        seen_paths.add(r.image_path)

        # Cross-field rule: If ID number exists, other fields should not contradict
        id_val = r.fields.get("id_number").normalized_value if "id_number" in r.fields else None
        if id_val:
            # Check DOB consistency with ID? (CCCD encodes DOB in digits 7-12)
            # For now, just log
            pass

        filtered.append(r)
    return filtered


def infer_user_id_from_path(path: Path) -> Optional[int]:
    """Extract user_id from a path segment like user_id=123."""
    for part in path.parts:
        m = re.fullmatch(r"user_id=(\d+)", part)
        if m:
            return int(m.group(1))
    return None


def result_to_output_row(result: OCRResult, run_date: str) -> Dict[str, Any]:
    """Flatten an OCRResult into a CSV-friendly row."""
    row = result.to_dict()
    row.update(
        {
            "run_date": run_date,
            "user_id": infer_user_id_from_path(Path(result.image_path)),
            "status": "ok",
            "error": "",
            "quality_width": result.quality_metrics.width,
            "quality_height": result.quality_metrics.height,
            "quality_blur_var_laplacian": result.quality_metrics.blur_var_laplacian,
            "quality_brightness_mean": result.quality_metrics.brightness_mean,
            "quality_contrast_std": result.quality_metrics.contrast_std,
            "quality_pct_black": result.quality_metrics.pct_black,
            "quality_pct_white": result.quality_metrics.pct_white,
        }
    )
    return row


def error_to_output_row(img_path: Path, run_date: str, error: Exception) -> Dict[str, Any]:
    """Create a CSV row for an image that failed processing."""
    return {
        "image_path": str(img_path),
        "run_date": run_date,
        "user_id": infer_user_id_from_path(img_path),
        "doc_type": "front",
        "processed_at": datetime.now().isoformat(timespec="seconds"),
        "status": "error",
        "error": f"{type(error).__name__}: {error}",
    }


# -----------------------------
# Main Pipeline
# -----------------------------

def process_single_image(
    img_path: Path,
    ocr_engine: Any,
    doc_type_hint: str = "front"
) -> OCRResult:
    """Process one image through full pipeline."""
    # Load image
    bgr = cv2.imread(str(img_path))
    if bgr is None:
        raise ValueError(f"Failed to load image: {img_path}")
    bgr, deskew_degrees, deskew_scores = deskew_input_image(bgr)

    # Step 1: Quality metrics
    quality = compute_image_quality_metrics(bgr)

    # Step 2: Document detection & crop
    quad, debug_info = find_document_quad(bgr)
    if quad is not None:
        warped = warp_document(bgr, quad, out_w=1000)
    else:
        warped = bgr  # Fallback to original
    warped, rotation_degrees, orientation_scores = normalize_front_orientation(warped, out_w=1000)
    if not is_plausible_front_card_image(warped):
        warped, rotation_degrees, orientation_scores = normalize_front_orientation(bgr, out_w=1000)
    warped, roi_geometry_scores = normalize_card_geometry_for_rois(warped, out_w=1000)

    # Step 3: Preprocessing
    preprocessed = preprocess_for_ocr(warped)

    # Step 4: Document classification
    doc_type = classify_document(warped, quad)

    # Step 5: Field localization (ROIs)
    h, w = warped.shape[:2]
    roi_boxes = get_rois_for_doc_type(doc_type, h, w)
    roi_images: Dict[str, np.ndarray] = {
        name: roi_crop_for_field(preprocessed, name, box)
        for name, box in roi_boxes.items()
    }

    # Step 6: OCR strategy (optimized)
    # - Run whole-card OCR once, try label-anchored extraction first.
    # - Only run per-ROI OCR for fields that are missing/implausible/invalid.
    raw_texts: Dict[str, str] = {k: "" for k in roi_boxes.keys()}
    conf_scores: Dict[str, float] = {k: 0.0 for k in roi_boxes.keys()}

    full_text_boxes = ocr_predict_text_boxes(ocr_engine, warped)
    anchored_texts, anchored_scores = extract_fields_by_label_anchors(full_text_boxes, warped.shape)

    for field_name in roi_boxes.keys():
        anchored_text = anchored_texts.get(field_name, "")
        if is_plausible_field_value(field_name, anchored_text):
            raw_texts[field_name] = anchored_text
            conf_scores[field_name] = float(anchored_scores.get(field_name, 0.0))

    # Prefer ROI-based full_name when it gives a longer cleaned result.
    if "full_name" in roi_boxes:
        variants = roi_crop_variants_for_field(preprocessed, "full_name", roi_boxes["full_name"])
        roi_name_text, roi_name_score, _src = ocr_predict_text_multi(ocr_engine, "full_name", variants)
        anchored_name_text = raw_texts.get("full_name", "")
        if len(clean_text(strip_demo_watermark_text(roi_name_text))) > len(clean_text(strip_demo_watermark_text(anchored_name_text))):
            raw_texts["full_name"] = roi_name_text
            conf_scores["full_name"] = float(roi_name_score if roi_name_score is not None else 0.0)

    # ROI fallback only when needed
    for field_name, roi_img in roi_images.items():
        current_text = raw_texts.get(field_name, "")
        current_field = parse_and_validate_field(field_name, current_text, conf_scores.get(field_name))

        if current_field.is_valid and is_plausible_field_value(field_name, current_text):
            continue

        if field_name == "full_name":
            variants = roi_crop_variants_for_field(preprocessed, field_name, roi_boxes[field_name])
            roi_text, roi_score, _src = ocr_predict_text_multi(ocr_engine, field_name, variants)
        else:
            roi_text, roi_score, _ = ocr_predict_text(ocr_engine, roi_img)
        roi_field = parse_and_validate_field(field_name, roi_text, roi_score)

        # Prefer a valid ROI value; otherwise keep the more plausible / longer cleaned text.
        if roi_field.is_valid and is_plausible_field_value(field_name, roi_text):
            raw_texts[field_name] = roi_text
            conf_scores[field_name] = float(roi_score if roi_score is not None else 0.0)
        else:
            cur_clean = clean_text(current_text)
            roi_clean = clean_text(roi_text)
            if (not cur_clean and roi_clean) or (len(roi_clean) > len(cur_clean) and is_plausible_field_value(field_name, roi_text)):
                raw_texts[field_name] = roi_text
                conf_scores[field_name] = float(roi_score if roi_score is not None else 0.0)

    # Step 7: Post-processing & parsing
    fields: Dict[str, FieldData] = {}
    for field_name in roi_boxes.keys():
        raw = raw_texts.get(field_name, "")
        conf = conf_scores.get(field_name)
        field_data = parse_and_validate_field(field_name, raw, conf)
        fields[field_name] = field_data

    # Build result
    result = OCRResult(
        image_path=str(img_path),
        doc_type=doc_type,
        quality_metrics=quality,
        fields=fields,
        raw_texts=raw_texts,
        confidence_scores=conf_scores,
        processed_at=datetime.now().isoformat(timespec='seconds'),
    )

    return result


def process_directory(
    input_dir: Path,
    run_date: str,
    output_csv: Path,
    lang: str = OCR_LANG,
) -> bool:
    """
    Process all images in input_dir (recursively) and output CSV.
    Returns True on success.
    """
    # Find all images
    img_extensions = ("*.jpg", "*.jpeg", "*.png", "*.tif", "*.tiff")
    img_paths: List[Path] = []
    for ext in img_extensions:
        img_paths.extend(input_dir.rglob(ext))
    img_paths = sorted(img_paths)

    if not img_paths:
        print(f"ERROR: No images found in {input_dir}")
        return False

    print(f"Found {len(img_paths)} images to process")

    # Init OCR engine
    print(f"Initializing PaddleOCR (lang={lang})...")
    ocr = init_ocr_engine(lang=lang)

    # Process each image
    results: List[OCRResult] = []
    for i, img_path in enumerate(img_paths, 1):
        print(f"\n[{i}/{len(img_paths)}] Processing: {img_path.name}")
        try:
            res = process_single_image(img_path, ocr, doc_type_hint="front")
            results.append(res)
            # Print result to console immediately
            print(f"  ✓ Extracted: {res.fields.get('full_name').cleaned_value if 'full_name' in res.fields else 'N/A'}")
            print(f"    ID: {res.fields.get('id_number').normalized_value if 'id_number' in res.fields else 'N/A'}")
            print(f"    DOB: {res.fields.get('date_of_birth').normalized_value if 'date_of_birth' in res.fields else 'N/A'}")
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            import traceback
            traceback.print_exc()

    # Apply business rules
    print("\nApplying business rules...")
    results = apply_business_rules(results)

    # Convert to DataFrame for CSV output
    rows = [r.to_dict() for r in results]
    df = pd.DataFrame(rows)

    # Ensure output directory exists
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"\nSaved {len(results)} records to {output_csv}")

    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total images processed: {len(results)}")
    valid_id = sum(1 for r in results if r.fields.get("id_number", FieldData("", "", None, False, "", None)).is_valid)
    print(f"Valid ID numbers: {valid_id}/{len(results)}")
    valid_name = sum(1 for r in results if r.fields.get("full_name", FieldData("", "", None, False, "", None)).is_valid)
    print(f"Valid full names: {valid_name}/{len(results)}")

    return True


def process_id_card_directory(
    input_dir: Path,
    run_date: str,
    output_csv: Path,
    lang: str = OCR_LANG,
    limit: Optional[int] = DEFAULT_LIMIT,
) -> bool:
    """Batch process ID-card images and write one CSV row per source image."""
    img_extensions = ("*.jpg", "*.jpeg", "*.png", "*.tif", "*.tiff", "*.bmp", "*.webp")
    img_paths: List[Path] = []
    for ext in img_extensions:
        img_paths.extend(input_dir.rglob(ext))
    img_paths = sorted(img_paths)
    total_found = len(img_paths)
    if limit is not None and limit > 0:
        img_paths = img_paths[:limit]

    if not img_paths:
        print(f"ERROR: No images found in {input_dir}")
        return False

    print(f"Found {total_found} images; processing {len(img_paths)}")
    print(f"Initializing PaddleOCR (lang={lang})...")
    ocr = init_ocr_engine(lang=lang)

    results: List[OCRResult] = []
    rows: List[Dict[str, Any]] = []
    for i, img_path in enumerate(img_paths, 1):
        print(f"\n[{i}/{len(img_paths)}] Processing: {img_path}")
        try:
            res = process_single_image(img_path, ocr, doc_type_hint="front")
            results.append(res)
            rows.append(result_to_output_row(res, run_date=run_date))
            print(f"  OK: {res.fields.get('full_name').normalized_value if 'full_name' in res.fields else 'N/A'}")
            print(f"    ID: {res.fields.get('id_number').normalized_value if 'id_number' in res.fields else 'N/A'}")
            print(f"    DOB: {res.fields.get('date_of_birth').normalized_value if 'date_of_birth' in res.fields else 'N/A'}")
        except Exception as e:
            print(f"  ERROR: {e}")
            rows.append(error_to_output_row(img_path, run_date=run_date, error=e))
            import traceback
            traceback.print_exc()

    results = apply_business_rules(results)

    df = pd.DataFrame(rows)
    if "user_id" in df.columns and "image_path" in df.columns:
        df = df.sort_values(["status", "user_id", "image_path"], na_position="last")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_csv.with_suffix('.xlsx'), index=False)
    print(f"\nSaved {len(df)} records to {output_csv.with_suffix('.xlsx')}")

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total images found: {total_found}")
    print(f"Images selected: {len(img_paths)}")
    print(f"Successful images: {len(results)}")
    print(f"Failed images: {len(img_paths) - len(results)}")
    valid_id = sum(1 for r in results if r.fields.get("id_number", FieldData("", "", None, False, "", None)).is_valid)
    valid_name = sum(1 for r in results if r.fields.get("full_name", FieldData("", "", None, False, "", None)).is_valid)
    print(f"Valid ID numbers: {valid_id}/{len(results)}")
    print(f"Valid full names: {valid_name}/{len(results)}")

    return True


def main():
    parser = argparse.ArgumentParser(description='OCR Extraction for CCCD (ID Card)')
    parser.add_argument("--input-dir", default=None, help="Directory containing images")
    parser.add_argument("--run-date", default="2026-05-13", help="Run date (YYYY-MM-DD)")
    parser.add_argument("--out", default=None, help="Output path")
    parser.add_argument("--lang", default=OCR_LANG, help=f"OCR language (default: {OCR_LANG})")
    args = parser.parse_args()

    input_dir = (
        Path(args.input_dir)
        if args.input_dir
        else PROJECT_ROOT / "data" / "unstructured" / "documents" / "doc_type=id_card" / f"run_date={args.run_date}"
    )

    if not input_dir.exists():
        print(f"ERROR: Input directory not found: {input_dir}")
        return 1

    # User input for number of images
    try:
        user_input = input("Enter number of images to process (or press Enter for all): ").strip()
        limit = int(user_input) if user_input else 0
    except ValueError:
        print("Invalid input. Processing all images.")
        limit = 0

    output_xlsx = (
        Path(args.out).with_suffix('.xlsx')
        if args.out
        else PROJECT_ROOT / "data" / "unstructured" / "extracted" / f"id_card_extracted_run_date={args.run_date}.xlsx"
    )

    limit_val = None if limit == 0 else limit

    print("=" * 60)
    print("OCR EXTRACTION PIPELINE - CCCD")
    print("=" * 60)
    print(f"Input dir: {input_dir}")
    print(f"Run date: {args.run_date}")
    print(f"Output: {output_xlsx}")
    print(f"Limit: {limit_val if limit_val is not None else 'all'}")
    print(f"OCR lang: {args.lang}")
    print("=" * 60)

    success = process_id_card_directory(
        input_dir=input_dir,
        run_date=args.run_date,
        output_csv=output_xlsx, # Function still named output_csv but we pass .xlsx path
        lang=args.lang,
        limit=limit_val,
    )

    if success:
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        return 0
    else:
        print("\n" + "=" * 60)
        print("PIPELINE COMPLETED WITH ERRORS")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
