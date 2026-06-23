"""
Trích xuất thông tin từ ảnh CCCD / Sổ Tiết Kiệm bằng Google Gemini Flash.
Trả về cùng format với ocr_engine.py.
Không có ROI highlight — image_annotated để trống, app.py sẽ điền bằng ảnh gốc.
"""
from __future__ import annotations

import base64
import json
import re
from pathlib import Path
from typing import Callable, Optional

import numpy as np

ProgressCb = Optional[Callable[[int, str], None]]

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

CCCD_PROMPT = """Đây là ảnh chụp mặt trước của Căn Cước Công Dân (CCCD) hoặc Chứng Minh Nhân Dân (CMND) Việt Nam.
Hãy đọc và trích xuất chính xác các trường thông tin bên dưới.
Trả về DUY NHẤT một JSON object (không giải thích, không markdown):

{
  "full_name":          {"value": "Họ tên đầy đủ", "confidence": 0.0},
  "id_number":          {"value": "12 chữ số", "confidence": 0.0},
  "date_of_birth":      {"value": "DD/MM/YYYY", "confidence": 0.0},
  "sex":                {"value": "Nam hoặc Nữ", "confidence": 0.0},
  "nationality":        {"value": "Quốc tịch", "confidence": 0.0},
  "place_of_origin":    {"value": "Quê quán đầy đủ", "confidence": 0.0},
  "place_of_residence": {"value": "Địa chỉ thường trú đầy đủ", "confidence": 0.0},
  "issue_date":         {"value": "DD/MM/YYYY", "confidence": 0.0},
  "expiry_date":        {"value": "DD/MM/YYYY", "confidence": 0.0}
}

Quy tắc:
- confidence: 0.0 nếu không đọc được, 0.5 nếu đọc được một phần, 0.9-1.0 nếu rõ ràng.
- Nếu không đọc được trường nào thì value = "" và confidence = 0.0.
- Giữ nguyên định dạng ngày DD/MM/YYYY, không chuyển đổi.
- Nếu đây là ảnh demo/mẫu (có watermark, chữ SAMPLE...) vẫn đọc thông tin hiển thị."""

SAVINGS_PROMPT = """Đây là ảnh chụp một trang sổ tiết kiệm ngân hàng Việt Nam.
Hãy đọc và trích xuất chính xác các trường thông tin bên dưới.
Trả về DUY NHẤT một JSON object (không giải thích, không markdown):

{
  "transaction_date":   {"value": "DD/MM/YYYY", "confidence": 0.0},
  "description":        {"value": "Nội dung giao dịch", "confidence": 0.0},
  "transaction_code":   {"value": "Mã giao dịch", "confidence": 0.0},
  "transaction_amount": {"value": "Số tiền (VND)", "confidence": 0.0},
  "balance":            {"value": "Số dư (VND)", "confidence": 0.0},
  "interest_rate":      {"value": "Lãi suất %/năm", "confidence": 0.0},
  "signature":          {"value": "có / không", "confidence": 0.0}
}

Quy tắc:
- confidence: 0.0 nếu không đọc được, 0.5 nếu đọc được một phần, 0.9-1.0 nếu rõ ràng.
- Nếu không đọc được trường nào thì value = "" và confidence = 0.0.
- Nếu có nhiều dòng giao dịch, lấy dòng giao dịch GẦN NHẤT (cuối trang).
- Nếu đây là ảnh demo/mẫu vẫn đọc thông tin hiển thị."""


def _emit(cb: ProgressCb, pct: int, msg: str) -> None:
    if cb:
        try:
            cb(pct, msg)
        except Exception:
            pass


def _img_to_b64(path: Path) -> tuple[str, str]:
    ext = path.suffix.lower()
    mime_map = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png",  ".bmp":  "image/bmp",
        ".tif": "image/tiff", ".tiff": "image/tiff",
    }
    mime = mime_map.get(ext, "image/jpeg")
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode(), mime


def _parse_json(text: str) -> dict:
    """Trích JSON từ response Gemini, loại bỏ markdown fence nếu có."""
    text = text.strip()
    m = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if m:
        text = m.group(1)
    return json.loads(text)


def _build_result(doc_type: str, labels: dict, raw: dict) -> dict:
    fields = []
    plaus = 0
    scores: list[float] = []

    for key, label in labels.items():
        entry = raw.get(key, {})
        value = str(entry.get("value", "")).strip() if isinstance(entry, dict) else ""
        conf  = float(entry.get("confidence", 0.0)) if isinstance(entry, dict) else 0.0
        conf  = max(0.0, min(1.0, conf))
        if value:
            plaus += 1
        scores.append(conf)
        fields.append({
            "key":        key,
            "label":      label,
            "value":      value,
            "confidence": round(conf, 4),
            "source":     "gemini",
        })

    total       = len(labels)
    ocr_conf    = float(np.mean(scores)) if scores else 0.0
    parse_conf  = plaus / max(1, total)
    final_conf  = round(0.70 * ocr_conf + 0.30 * parse_conf, 4)

    return {
        "doc_type":         doc_type,
        "fields":           fields,
        "final_confidence": final_conf,
        "ocr_confidence":   round(ocr_conf, 4),
        "parse_confidence": round(parse_conf, 4),
        "plausible_fields": plaus,
        "total_fields":     total,
        "image_annotated":  "",   # không hỗ trợ ROI, app.py sẽ điền ảnh gốc
        "engine":           "gemini",
    }


# Chỉ giữ các model đã xác nhận hoạt động với vision free tier
_FALLBACK_MODELS = [
    "gemini-2.0-flash-lite",
]
_GEMINI_BASE = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:generateContent?key={key}"
)


def _parse_api_keys(raw: str) -> list[str]:
    keys = [k.strip() for k in raw.replace("\n", ",").split(",")]
    return [k for k in keys if k]


def _call_gemini(api_keys_raw: str, prompt: str, img_b64: str, mime: str,
                 emit_cb: ProgressCb = None) -> tuple[str, str]:
    """
    Xoay vòng theo thứ tự: model1/key1 → model1/key2 → ... → model2/key1 → ...
    Thử tất cả key với một model trước rồi mới đổi model — quota của mỗi model độc lập.
    Trả về (text, "model (key_label)").
    """
    import time
    import requests

    keys = _parse_api_keys(api_keys_raw)
    if not keys:
        raise ValueError("Chưa nhập API key Gemini.")

    payload = {
        "contents": [{"parts": [
            {"text": prompt},
            {"inline_data": {"mime_type": mime, "data": img_b64}},
        ]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1024},
    }

    invalid_keys: set[int] = set()   # key bị 400 → bỏ hẳn
    dead_models:  set[str] = set()   # model bị 404 → bỏ hẳn
    errors: list[str] = []

    for model in _FALLBACK_MODELS:
        if model in dead_models:
            continue

        for ki, key in enumerate(keys):
            if ki in invalid_keys:
                continue

            key_label = f"key#{ki + 1}"
            url = _GEMINI_BASE.format(model=model, key=key)
            _emit(emit_cb, 40, f"Thử {model} / {key_label}...")

            for attempt in range(2):
                try:
                    resp = requests.post(url, json=payload, timeout=60)
                except requests.exceptions.Timeout:
                    errors.append(f"{key_label}/{model}: timeout")
                    break
                except requests.exceptions.ConnectionError:
                    raise ValueError("Không kết nối được tới Gemini API. Kiểm tra mạng.")

                if resp.status_code == 400:
                    invalid_keys.add(ki)
                    errors.append(f"{key_label}: key không hợp lệ")
                    break

                if resp.status_code == 404:
                    dead_models.add(model)
                    errors.append(f"{model}: model không tồn tại, bỏ qua")
                    break

                if resp.status_code == 429:
                    if attempt == 0:
                        _emit(emit_cb, 40,
                              f"{model}/{key_label} rate limit — đợi 12s...")
                        time.sleep(12)
                        continue
                    # Sau retry vẫn 429 → key này hết quota với model này
                    errors.append(f"{key_label}/{model}: quota hết")
                    break

                if not resp.ok:
                    errors.append(f"{key_label}/{model}: HTTP {resp.status_code}")
                    break

                try:
                    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                    return text, f"{model} ({key_label})"
                except (KeyError, IndexError):
                    errors.append(f"{key_label}/{model}: response không hợp lệ")
                    break

    summary = "; ".join(errors[-8:])
    raise ValueError(
        f"Tất cả key/model đều không khả dụng ({summary}). "
        "Quota free tier reset sau 24h — thêm key mới hoặc thử lại ngày mai."
    )


# ---------------------------------------------------------------------------

def extract_id_card(img_path: Path, api_keys_raw: str,
                    progress_cb: ProgressCb = None) -> dict:
    _emit(progress_cb, 5,  "Đang chuẩn bị ảnh...")

    _emit(progress_cb, 20, "Đang đọc ảnh...")
    b64, mime = _img_to_b64(img_path)

    _emit(progress_cb, 38, "Đang gửi ảnh lên Gemini (có thể mất 5-20 giây)...")
    raw_text, model_used = _call_gemini(api_keys_raw, CCCD_PROMPT, b64, mime,
                                        emit_cb=progress_cb)

    _emit(progress_cb, 78, "Đang phân tích kết quả...")
    try:
        raw = _parse_json(raw_text)
    except (json.JSONDecodeError, AttributeError):
        raise ValueError(f"Gemini trả về kết quả không hợp lệ: {raw_text[:300]}")

    _emit(progress_cb, 93, "Đang hoàn thiện dữ liệu...")
    result = _build_result("id_card", CCCD_LABELS, raw)
    result["engine_model"] = model_used

    _emit(progress_cb, 100, f"Hoàn thành! (dùng {model_used})")
    return result


def extract_savings_book(img_path: Path, api_keys_raw: str,
                         progress_cb: ProgressCb = None) -> dict:
    _emit(progress_cb, 5,  "Đang chuẩn bị ảnh...")

    _emit(progress_cb, 20, "Đang đọc ảnh...")
    b64, mime = _img_to_b64(img_path)

    _emit(progress_cb, 38, "Đang gửi ảnh lên Gemini (có thể mất 5-20 giây)...")
    raw_text, model_used = _call_gemini(api_keys_raw, SAVINGS_PROMPT, b64, mime,
                                        emit_cb=progress_cb)

    _emit(progress_cb, 78, "Đang phân tích kết quả...")
    try:
        raw = _parse_json(raw_text)
    except (json.JSONDecodeError, AttributeError):
        raise ValueError(f"Gemini trả về kết quả không hợp lệ: {raw_text[:300]}")

    _emit(progress_cb, 93, "Đang hoàn thiện dữ liệu...")
    result = _build_result("savings_book", SAVINGS_LABELS, raw)
    result["engine_model"] = model_used

    _emit(progress_cb, 100, f"Hoàn thành! (dùng {model_used})")
    return result
