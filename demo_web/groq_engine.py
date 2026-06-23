"""
Trích xuất thông tin bằng Groq API (LLaMA Vision).
Miễn phí, không giới hạn theo ngày — chỉ rate limit 30 RPM.
Dùng chung labels, helpers từ gemini_engine.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Callable, Optional

import base64

import cv2
import numpy as np

from gemini_engine import (
    CCCD_LABELS, SAVINGS_LABELS,
    CCCD_PROMPT as _CCCD_PROMPT,
    SAVINGS_PROMPT as _SAVINGS_PROMPT,
    _parse_api_keys, _parse_json, _build_result, _emit,
)

ProgressCb = Optional[Callable[[int, str], None]]

_GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

_GROQ_MODELS = [
    "meta-llama/llama-4-scout-17b-16e-instruct",
    "meta-llama/llama-4-maverick-17b-128e-instruct",
]

# Groq giới hạn context — resize ảnh xuống tối đa chiều này trước khi gửi
_MAX_SIDE = 768


def _read_and_resize(img_path: "Path") -> tuple[str, str]:
    """Đọc ảnh, resize nếu cần, trả (base64_str, 'image/jpeg')."""
    bgr = cv2.imread(str(img_path))
    if bgr is None:
        raise ValueError(f"Không đọc được ảnh: {img_path}")
    h, w = bgr.shape[:2]
    if max(h, w) > _MAX_SIDE:
        scale = _MAX_SIDE / max(h, w)
        bgr = cv2.resize(bgr, (int(w * scale), int(h * scale)),
                         interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".jpg", bgr, [cv2.IMWRITE_JPEG_QUALITY, 82])
    if not ok:
        raise ValueError("Không encode được ảnh JPEG.")
    return base64.b64encode(buf.tobytes()).decode(), "image/jpeg"


def _call_groq(api_keys_raw: str, prompt: str,
               img_b64: str, mime: str,
               emit_cb: ProgressCb = None) -> tuple[str, str]:
    """
    Xoay vòng model × key cho đến khi có kết quả.
    Trả về (text, "model (key_label)").
    """
    import time
    import requests

    keys = _parse_api_keys(api_keys_raw)
    if not keys:
        raise ValueError("Chưa nhập Groq API key.")

    errors: list[str] = []
    dead_models: set[str] = set()

    for model in _GROQ_MODELS:
        if model in dead_models:
            continue

        for ki, key in enumerate(keys):
            key_label = f"key#{ki + 1}"
            _emit(emit_cb, 40, f"Thử Groq {model} / {key_label}...")

            payload = {
                "model": model,
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url",
                         "image_url": {"url": f"data:{mime};base64,{img_b64}"}},
                    ],
                }],
                "max_tokens": 1024,
            }
            headers = {
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            }

            for attempt in range(2):
                try:
                    resp = requests.post(_GROQ_URL, headers=headers,
                                         json=payload, timeout=60)
                except requests.exceptions.Timeout:
                    errors.append(f"{key_label}/{model}: timeout")
                    break
                except requests.exceptions.ConnectionError:
                    raise ValueError("Không kết nối được tới Groq API. Kiểm tra mạng.")

                if resp.status_code == 401:
                    errors.append(f"{key_label}: key không hợp lệ")
                    break

                if resp.status_code == 400:
                    try:
                        body = resp.json()
                        detail = body.get("error", {}).get("message", resp.text[:200])
                    except Exception:
                        detail = resp.text[:200]
                    # Model bị khai tử → bỏ hẳn model này
                    if "decommissioned" in detail or "no longer supported" in detail:
                        dead_models.add(model)
                        errors.append(f"{model}: đã bị khai tử")
                    else:
                        errors.append(f"{key_label}/{model}: 400 — {detail}")
                    break

                if resp.status_code == 404:
                    dead_models.add(model)
                    errors.append(f"{model}: model không tồn tại")
                    break

                if resp.status_code == 429:
                    if attempt == 0:
                        _emit(emit_cb, 40,
                              f"{model}/{key_label} rate limit — đợi 10s...")
                        time.sleep(10)
                        continue
                    errors.append(f"{key_label}/{model}: rate limit")
                    break

                if not resp.ok:
                    errors.append(f"{key_label}/{model}: HTTP {resp.status_code}")
                    break

                try:
                    text = resp.json()["choices"][0]["message"]["content"]
                    return text, f"{model} ({key_label})"
                except (KeyError, IndexError):
                    errors.append(f"{key_label}/{model}: response không hợp lệ")
                    break

    summary = "; ".join(errors[-6:])
    raise ValueError(
        f"Không thể kết nối Groq ({summary}). "
        "Lấy key miễn phí tại console.groq.com/keys"
    )


def extract_id_card(img_path: Path, api_keys_raw: str,
                    progress_cb: ProgressCb = None) -> dict:
    _emit(progress_cb, 5,  "Đang chuẩn bị ảnh...")
    _emit(progress_cb, 20, "Đang resize và nén ảnh...")
    b64, mime = _read_and_resize(img_path)

    _emit(progress_cb, 38, "Đang gửi lên Groq LLaMA Vision...")
    raw_text, model_used = _call_groq(api_keys_raw, _CCCD_PROMPT,
                                      b64, mime, emit_cb=progress_cb)

    _emit(progress_cb, 78, "Đang phân tích kết quả...")
    try:
        raw = _parse_json(raw_text)
    except (json.JSONDecodeError, AttributeError):
        raise ValueError(f"Groq trả về kết quả không hợp lệ: {raw_text[:300]}")

    _emit(progress_cb, 93, "Đang hoàn thiện dữ liệu...")
    result = _build_result("id_card", CCCD_LABELS, raw)
    result["engine"]       = "groq"
    result["engine_model"] = model_used

    _emit(progress_cb, 100, f"Hoàn thành! ({model_used})")
    return result


def extract_savings_book(img_path: Path, api_keys_raw: str,
                         progress_cb: ProgressCb = None) -> dict:
    _emit(progress_cb, 5,  "Đang chuẩn bị ảnh...")
    _emit(progress_cb, 20, "Đang resize và nén ảnh...")
    b64, mime = _read_and_resize(img_path)

    _emit(progress_cb, 38, "Đang gửi lên Groq LLaMA Vision...")
    raw_text, model_used = _call_groq(api_keys_raw, _SAVINGS_PROMPT,
                                      b64, mime, emit_cb=progress_cb)

    _emit(progress_cb, 78, "Đang phân tích kết quả...")
    try:
        raw = _parse_json(raw_text)
    except (json.JSONDecodeError, AttributeError):
        raise ValueError(f"Groq trả về kết quả không hợp lệ: {raw_text[:300]}")

    _emit(progress_cb, 93, "Đang hoàn thiện dữ liệu...")
    result = _build_result("savings_book", SAVINGS_LABELS, raw)
    result["engine"]       = "groq"
    result["engine_model"] = model_used

    _emit(progress_cb, 100, f"Hoàn thành! ({model_used})")
    return result
