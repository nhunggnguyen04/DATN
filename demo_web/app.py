"""
Web demo OCR cho CCCD và Sổ Tiết Kiệm.
Chạy: python demo_web/app.py  →  http://localhost:5000
"""
from __future__ import annotations

import base64
import json
import queue as _qmod
import sys
import threading
import uuid
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = Flask(__name__, template_folder=str(BASE_DIR / "templates"))

ALLOWED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"}
MIME_MAP = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".bmp": "image/bmp", ".tif": "image/tiff", ".tiff": "image/tiff"}

# job_id → Queue (chứa các SSE event dict)
_jobs: dict[str, _qmod.Queue] = {}
_jobs_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Error handler toàn cục — luôn trả JSON
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def handle_404(exc):
    return jsonify({"error": "Không tìm thấy."}), 404

@app.errorhandler(Exception)
def handle_exc(exc):
    import traceback
    traceback.print_exc()
    return jsonify({"error": str(exc)}), 500

@app.route("/favicon.ico")
def favicon():
    return "", 204


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/extract", methods=["POST"])
def extract_start():
    """Nhận file, tạo background job, trả job_id ngay."""
    doc_type   = request.form.get("doc_type", "").strip()
    engine     = request.form.get("engine", "paddleocr").strip()
    gemini_key = request.form.get("gemini_api_key", "").strip()
    groq_key   = request.form.get("groq_api_key",   "").strip()
    file       = request.files.get("image")

    if not file or not file.filename or not doc_type:
        return jsonify({"error": "Thiếu file hoặc loại tài liệu."}), 400

    if engine == "gemini" and not gemini_key:
        return jsonify({"error": "Cần nhập Gemini API key."}), 400
    if engine == "groq" and not groq_key:
        return jsonify({"error": "Cần nhập Groq API key."}), 400

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTS:
        return jsonify({"error": f"Định dạng không hỗ trợ: {ext or '(không rõ)'}"}), 400

    tmp_path = UPLOAD_DIR / f"{uuid.uuid4()}{ext}"
    file.save(tmp_path)

    job_id = str(uuid.uuid4())
    q: _qmod.Queue = _qmod.Queue()

    with _jobs_lock:
        _jobs[job_id] = q
        if len(_jobs) > 200:           # dọn job cũ
            del _jobs[next(iter(_jobs))]

    threading.Thread(
        target=_run_job,
        args=(job_id, doc_type, engine, gemini_key, groq_key, tmp_path, ext),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id})


def _run_job(job_id: str, doc_type: str, engine: str,
             gemini_key: str, groq_key: str, tmp_path: Path, ext: str) -> None:
    q = _jobs.get(job_id)
    if q is None:
        return

    def progress(pct: int, msg: str) -> None:
        q.put({"type": "progress", "pct": pct, "message": msg})

    try:
        if engine == "gemini":
            from gemini_engine import (extract_id_card as gem_id,
                                       extract_savings_book as gem_sb)
            if doc_type == "id_card":
                result = gem_id(tmp_path, api_keys_raw=gemini_key, progress_cb=progress)
            elif doc_type == "savings_book":
                result = gem_sb(tmp_path, api_keys_raw=gemini_key, progress_cb=progress)
            else:
                q.put({"type": "error", "error": f"Loại không hợp lệ: {doc_type}"}); return

        elif engine == "groq":
            from groq_engine import (extract_id_card as groq_id,
                                     extract_savings_book as groq_sb)
            if doc_type == "id_card":
                result = groq_id(tmp_path, api_keys_raw=groq_key, progress_cb=progress)
            elif doc_type == "savings_book":
                result = groq_sb(tmp_path, api_keys_raw=groq_key, progress_cb=progress)
            else:
                q.put({"type": "error", "error": f"Loại không hợp lệ: {doc_type}"}); return

        else:
            from ocr_engine import extract_id_card, extract_savings_book
            if doc_type == "id_card":
                result = extract_id_card(tmp_path, progress_cb=progress)
            elif doc_type == "savings_book":
                result = extract_savings_book(tmp_path, progress_cb=progress)
            else:
                q.put({"type": "error", "error": f"Loại không hợp lệ: {doc_type}"}); return

        mime = MIME_MAP.get(ext, "image/jpeg")
        with open(tmp_path, "rb") as f:
            result["image_preview"] = (
                f"data:{mime};base64,{base64.b64encode(f.read()).decode()}"
            )

        # Gemini không tạo ROI annotation — dùng ảnh gốc cho cả 2 tab
        if not result.get("image_annotated"):
            result["image_annotated"] = result["image_preview"]

        q.put({"type": "done", "result": result})

    except Exception as exc:
        import traceback
        traceback.print_exc()
        q.put({"type": "error", "error": str(exc)})

    finally:
        tmp_path.unlink(missing_ok=True)
        # Giữ job trong dict thêm 60 s để client kịp nhận done event


@app.route("/chat", methods=["POST"])
def chat_endpoint():
    """Chatbot hỏi đáp về dữ liệu đã trích xuất — dùng Groq LLaMA text model."""
    data     = request.get_json(silent=True) or {}
    message  = data.get("message", "").strip()
    fields   = data.get("fields", [])
    doc_type = data.get("doc_type", "")
    groq_key = data.get("groq_api_key", "").strip()
    history  = data.get("history", [])   # [{role, content}] tối đa 10 cặp

    if not message:
        return jsonify({"error": "Thiếu câu hỏi."}), 400
    if not groq_key:
        return jsonify({"error": "Cần Groq API key để dùng chatbot."}), 400

    doc_labels = {
        "id_card":      "Căn Cước Công Dân (CCCD)",
        "savings_book": "Sổ Tiết Kiệm",
    }
    fields_text = "\n".join(
        f"- {f['label']}: {f['value'] or '(không đọc được)'}"
        for f in fields
    )
    system_prompt = (
        f"Bạn là trợ lý AI phân tích tài liệu ngân hàng Việt Nam.\n"
        f"Dữ liệu trích xuất từ ảnh {doc_labels.get(doc_type, doc_type)}:\n\n"
        f"{fields_text}\n\n"
        "Hãy trả lời câu hỏi của người dùng về dữ liệu này bằng tiếng Việt, "
        "ngắn gọn và chính xác. Nếu thông tin không có trong dữ liệu, nói rõ là không có."
    )

    msgs = [{"role": "system", "content": system_prompt}]
    msgs.extend(history[-10:])           # giữ tối đa 10 tin nhắn gần nhất
    msgs.append({"role": "user", "content": message})

    try:
        import requests as _req
        resp = _req.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}",
                     "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": msgs,
                  "max_tokens": 512},
            timeout=30,
        )
        if resp.status_code == 401:
            return jsonify({"error": "Groq API key không hợp lệ."}), 400
        if not resp.ok:
            return jsonify({"error": f"Lỗi Groq ({resp.status_code})."}), 500
        reply = resp.json()["choices"][0]["message"]["content"]
        return jsonify({"reply": reply})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/progress/<job_id>")
def progress_stream(job_id: str):
    """SSE endpoint — client dùng EventSource GET để nhận tiến trình."""
    with _jobs_lock:
        q = _jobs.get(job_id)

    if q is None:
        return jsonify({"error": "Job không tồn tại hoặc đã hết hạn."}), 404

    def generate():
        while True:
            try:
                event = q.get(timeout=120)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if event.get("type") in ("done", "error"):
                    with _jobs_lock:
                        _jobs.pop(job_id, None)
                    break
            except _qmod.Empty:
                yield 'data: {"type":"keepalive"}\n\n'

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n" + "=" * 50)
    print("  OCR Demo — DATN Banking Platform")
    print("=" * 50)
    print("  Truy cập: http://localhost:5000")
    print("  Nhấn Ctrl+C để dừng\n")
    app.run(host="0.0.0.0", debug=False, port=5000, use_reloader=False, threaded=True)
