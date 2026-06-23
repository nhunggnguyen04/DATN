"""
Wrapper OCR cho từng ảnh đơn — import hàm từ script gốc, không sửa script gốc.

Thread safety:
  - Preprocessing (deskew, warp, CLAHE) và postprocessing chạy song song.
  - OCR inference serialize qua _ocr_lock — PaddleOCR không thread-safe.
  - Nhiều ảnh có thể được xử lý đồng thời; chỉ bước OCR thực sự phải chờ nhau.
"""
from __future__ import annotations

import base64
import importlib.util
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Optional

import cv2
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

ProgressCb = Optional[Callable[[int, str], None]]

# Lock khởi tạo OCR engine (double-checked locking)
_ocr_init_lock = threading.Lock()
# Lock gọi OCR inference — serialize để tránh crash PaddleOCR
_ocr_lock      = threading.Lock()


def _emit(cb: ProgressCb, pct: int, msg: str) -> None:
    if cb:
        try:
            cb(pct, msg)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Load script gốc như module độc lập
# ---------------------------------------------------------------------------

def _load_script(alias: str, rel_path: str):
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy script: {path}")
    spec = importlib.util.spec_from_file_location(alias, str(path))
    if spec is None:
        raise ImportError(f"Không load được module từ: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[alias] = mod   # bắt buộc: @dataclass tự lookup qua sys.modules
    spec.loader.exec_module(mod)
    return mod


_id_mod = _load_script("_ocr_id_card", "scripts/extract/ocr_extract_id_card.py")
_sb_mod = _load_script("_ocr_savings", "scripts/extract/ocr_extract_savings_book.py")

# ---------------------------------------------------------------------------
# Singleton PaddleOCR — thread-safe init với double-checked locking
# ---------------------------------------------------------------------------

_ocr = None


def _get_ocr():
    global _ocr
    if _ocr is None:
        with _ocr_init_lock:
            if _ocr is None:
                from paddleocr import PaddleOCR
                _ocr = PaddleOCR(lang="en", ocr_version="PP-OCRv3",
                                  use_textline_orientation=False)
    return _ocr


# ---------------------------------------------------------------------------
# Nhãn tiếng Việt
# ---------------------------------------------------------------------------

CCCD_LABELS: dict[str, str] = {
    "full_name":          "Họ và tên",
    "id_number":          "Số CCCD",
    "date_of_birth":      "Ngày sinh",
    "sex":                "Giới tính",
    "nationality":        "Quốc tịch",
    "place_of_origin":    "Quê quán",
    "place_of_residence": "Nơi thường trú",
    "issue_date":         "Ngày cấp",
    "expiry_date":        "Ngày hết hạn",
}

SAVINGS_LABELS: dict[str, str] = {
    "transaction_date":   "Ngày giao dịch",
    "description":        "Nội dung",
    "transaction_code":   "Mã giao dịch",
    "transaction_amount": "Số tiền",
    "balance":            "Số dư",
    "interest_rate":      "Lãi suất",
    "signature":          "Chữ ký",
}

# BGR (OpenCV) cho từng mức confidence
_BOX_COLORS = {
    "high":   (94, 197, 34),
    "medium": (11, 158, 245),
    "low":    (68,  68, 239),
    "none":   (184, 163, 148),
}


def _conf_cls(score) -> str:
    if score is None:       return "none"
    if float(score) >= 0.80: return "high"
    if float(score) >= 0.50: return "medium"
    return "low"


def _annotate_rois(bgr: np.ndarray,
                   roi_boxes: dict[str, tuple],
                   field_conf: dict[str, Any]) -> str:
    """Vẽ bounding box màu cho từng ROI, trả chuỗi base64 JPEG."""
    img = bgr.copy()
    h, w = img.shape[:2]
    font       = cv2.FONT_HERSHEY_SIMPLEX
    font_scale = max(0.28, min(0.42, w / 2400))
    thick      = 1

    for field_name, (x1, y1, x2, y2) in roi_boxes.items():
        color = _BOX_COLORS[_conf_cls(field_conf.get(field_name))]
        cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
        label = field_name.replace("_", " ")
        (tw, th), baseline = cv2.getTextSize(label, font, font_scale, thick)
        lx, ly = x1, max(y1, th + 6)
        cv2.rectangle(img, (lx, ly - th - 5), (lx + tw + 6, ly + baseline), color, -1)
        cv2.putText(img, label, (lx + 3, ly - 2),
                    font, font_scale, (255, 255, 255), thick, cv2.LINE_AA)

    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 88])
    return ("data:image/jpeg;base64," + base64.b64encode(buf.tobytes()).decode()) if ok else ""


# ---------------------------------------------------------------------------
# Xử lý một ảnh CCCD
#
#  Luồng:
#   [pre] đọc + deskew + warp         → song song với ảnh khác
#   [ocr] ocr_predict_boxes + fallback → serialize qua _ocr_lock
#   [post] parse + annotate            → song song với ảnh khác
# ---------------------------------------------------------------------------

def extract_id_card(img_path: Path,
                    progress_cb: ProgressCb = None) -> dict[str, Any]:
    # ── PRE (song song) ────────────────────────────────────────────
    _emit(progress_cb, 5, "Đang khởi tạo OCR engine...")
    ocr = _get_ocr()
    m   = _id_mod

    _emit(progress_cb, 15, "Đang đọc ảnh...")
    bgr = cv2.imread(str(img_path))
    if bgr is None:
        raise ValueError(f"Không đọc được ảnh: {img_path}")

    _emit(progress_cb, 28, "Đang căn chỉnh và chuẩn hóa ảnh...")
    deskewed, _ = m.deskew(bgr)
    quad = m.find_document_quad(deskewed)
    warped = (m.warp_document(deskewed, quad, out_w=1000)
              if quad is not None
              else m.normalize_width(deskewed, out_w=1000))
    warped, _ = m.normalize_front_orientation(warped, out_w=1000)
    if not m.is_plausible_front_card_image(warped):
        warped, _ = m.normalize_front_orientation(bgr, out_w=1000)
    warped    = m.normalize_card_geometry(warped, out_w=1000)
    pp        = m.preprocess_for_ocr(warped)
    roi_boxes = m.scale_rois(m.FRONT_ROIS, warped.shape[1], warped.shape[0])

    # ── OCR (serialize) ────────────────────────────────────────────
    _emit(progress_cb, 46, "Đang chờ OCR engine sẵn sàng...")
    with _ocr_lock:
        _emit(progress_cb, 52, "Đang nhận diện văn bản (OCR)...")
        text_boxes = m.ocr_predict_boxes(ocr, warped)
        anchored_raw, anchored_scores = m.extract_by_labels(text_boxes)

        _emit(progress_cb, 68, "Đang phân tích từng trường dữ liệu...")
        field_raw: dict[str, dict] = {}
        for field_name, box in roi_boxes.items():
            raw   = anchored_raw.get(field_name, "")
            score = anchored_scores.get(field_name)
            src   = "label_anchored"
            if not m.is_plausible(field_name, m.parse_field(field_name, raw)):
                if field_name == "full_name":
                    raw, score, src = m.ocr_name_fallback(ocr, pp, box)
                else:
                    roi_img = pp["clahe"][box[1]:box[3], box[0]:box[2]]
                    raw, score = m.ocr_predict_text(ocr, roi_img)
                    src = "roi_fallback"
            field_raw[field_name] = {"raw_text": raw, "score": score, "src": src}
    # lock giải phóng — các ảnh khác tiếp tục chạy OCR

    # ── POST (song song) ───────────────────────────────────────────
    parsed = {fn: m.parse_field(fn, p["raw_text"]) for fn, p in field_raw.items()}

    _emit(progress_cb, 82, "Đang tính điểm tin cậy...")
    scores_list = [float(p["score"]) for p in field_raw.values() if p.get("score") is not None]
    plaus = sum(1 for fn, pf in parsed.items() if m.is_plausible(fn, pf))
    total = len(field_raw)
    ocr_conf   = float(np.mean(scores_list)) if scores_list else 0.0
    parse_conf = plaus / max(1, total)
    final_conf = round(0.70 * ocr_conf + 0.30 * parse_conf, 4)

    field_conf = {k: v.get("score") for k, v in field_raw.items()}

    _emit(progress_cb, 93, "Đang tạo ảnh minh họa ROI...")
    image_annotated = _annotate_rois(warped, roi_boxes, field_conf)

    fields = []
    for key, label in CCCD_LABELS.items():
        sc = field_raw[key].get("score")
        fields.append({
            "key":        key,
            "label":      label,
            "value":      parsed.get(key) or "",
            "confidence": round(float(sc), 4) if sc is not None else None,
            "source":     field_raw[key].get("src", ""),
        })

    _emit(progress_cb, 100, "Hoàn thành!")
    return {
        "doc_type":         "id_card",
        "fields":           fields,
        "final_confidence": final_conf,
        "ocr_confidence":   round(ocr_conf, 4),
        "parse_confidence": round(parse_conf, 4),
        "plausible_fields": plaus,
        "total_fields":     total,
        "image_annotated":  image_annotated,
    }


# ---------------------------------------------------------------------------
# Xử lý một ảnh Sổ Tiết Kiệm
# ---------------------------------------------------------------------------

def extract_savings_book(img_path: Path,
                         progress_cb: ProgressCb = None) -> dict[str, Any]:
    # ── PRE (song song) ────────────────────────────────────────────
    _emit(progress_cb, 5, "Đang khởi tạo OCR engine...")
    ocr = _get_ocr()
    m   = _sb_mod

    _emit(progress_cb, 15, "Đang đọc ảnh...")
    bgr = cv2.imread(str(img_path))
    if bgr is None:
        raise ValueError(f"Không đọc được ảnh: {img_path}")

    _emit(progress_cb, 28, "Đang căn chỉnh và chuẩn hóa ảnh...")
    deskewed, _ = m.deskew(bgr)
    quad = m.find_document_quad(deskewed)
    warped = (m.warp_document(deskewed, quad, out_w=1200)
              if quad is not None
              else m.normalize_width(deskewed, out_w=1200))
    pp        = m.preprocess_for_ocr(warped)
    roi_boxes = m.scale_rois(m.SAVINGS_BOOK_ROIS, warped.shape[1], warped.shape[0])

    # ── OCR (serialize) ────────────────────────────────────────────
    _emit(progress_cb, 46, "Đang chờ OCR engine sẵn sàng...")
    with _ocr_lock:
        _emit(progress_cb, 52, "Đang nhận diện văn bản từng vùng ROI...")
        field_raw: dict[str, dict] = {}
        for field_name, box in roi_boxes.items():
            candidates = []
            for src, roi in m.ocr_roi_variants(pp, box):
                items_list, full_text, mean_score = m.ocr_predict_items(ocr, roi)
                candidates.append((src, full_text, mean_score, len(items_list)))
            raw, score, src, _ = m.choose_best_ocr(field_name, candidates)
            field_raw[field_name] = {"raw_text": raw, "score": score, "src": src}
    # lock giải phóng

    # ── POST (song song) ───────────────────────────────────────────
    _emit(progress_cb, 75, "Đang phân tích và chuẩn hóa dữ liệu...")
    parsed = {fn: m.parse_field(fn, p["raw_text"]) for fn, p in field_raw.items()}

    _emit(progress_cb, 83, "Đang tính điểm tin cậy...")
    scores = [float(p["score"]) for p in field_raw.values() if p.get("score") is not None]
    plaus  = sum(1 for fn, pf in parsed.items() if m.is_plausible(fn, pf))
    total  = len(field_raw)
    ocr_conf   = float(np.mean(scores)) if scores else 0.0
    parse_conf = plaus / max(1, total)
    final_conf = round(0.70 * ocr_conf + 0.30 * parse_conf, 4)

    field_conf = {k: v.get("score") for k, v in field_raw.items()}

    _emit(progress_cb, 93, "Đang tạo ảnh minh họa ROI...")
    image_annotated = _annotate_rois(warped, roi_boxes, field_conf)

    fields = []
    for key, label in SAVINGS_LABELS.items():
        sc = field_raw[key].get("score")
        fields.append({
            "key":        key,
            "label":      label,
            "value":      parsed.get(key) or "",
            "confidence": round(float(sc), 4) if sc is not None else None,
            "source":     field_raw[key].get("src", ""),
        })

    _emit(progress_cb, 100, "Hoàn thành!")
    return {
        "doc_type":         "savings_book",
        "fields":           fields,
        "final_confidence": final_conf,
        "ocr_confidence":   round(ocr_conf, 4),
        "parse_confidence": round(parse_conf, 4),
        "plausible_fields": plaus,
        "total_fields":     total,
        "image_annotated":  image_annotated,
    }
