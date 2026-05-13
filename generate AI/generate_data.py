import argparse
import csv
import hashlib
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyodbc
from dotenv import load_dotenv
from faker import Faker
from PIL import Image, ImageDraw, ImageFilter, ImageFont

load_dotenv()
fake = Faker('vi_VN')


DEFAULT_UNSTRUCTURED_ROOT = Path(os.getenv("UNSTRUCTURED_ROOT", "output/unstructured"))
DEFAULT_MANIFEST_DIR = Path(os.getenv("UNSTRUCTURED_MANIFEST_DIR", "output/manifests"))

# =====================
# KẾT NỐI 2 SERVER
# =====================
def get_source_conn():
    return pyodbc.connect(
        f"DRIVER={{{os.getenv('ODBC_DRIVER')}}};"
        f"SERVER={os.getenv('SOURCE_SERVER')},{os.getenv('SOURCE_PORT')};"
        f"DATABASE={os.getenv('SOURCE_DATABASE')};"
        f"UID={os.getenv('SOURCE_USERNAME')};"
        f"PWD={os.getenv('SOURCE_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )

def get_target_conn():
    return pyodbc.connect(
        f"DRIVER={{{os.getenv('ODBC_DRIVER')}}};"
        f"SERVER={os.getenv('TARGET_SERVER')},{os.getenv('TARGET_PORT')};"
        f"DATABASE={os.getenv('TARGET_DATABASE')};"
        f"UID={os.getenv('TARGET_USERNAME')};"
        f"PWD={os.getenv('TARGET_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )

# =====================
# SINH DỮ LIỆU
# =====================
def _as_posix_str(path: Path) -> str:
    return path.as_posix()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _scanify_image(
    img: Image.Image,
    *,
    angle_range: float = 1.2,
    blur_min: float = 0.2,
    blur_max: float = 0.8,
    noise_sigma_min: float = 6.0,
    noise_sigma_max: float = 14.0,
) -> Image.Image:
    # Make the page look like a scanned document
    img = img.convert("RGB")

    angle = random.uniform(-angle_range, angle_range)
    img = img.rotate(angle, expand=True, fillcolor=(245, 245, 245))

    # Add slight blur
    img = img.filter(ImageFilter.GaussianBlur(radius=random.uniform(blur_min, blur_max)))

    # Add noise
    arr = np.array(img).astype(np.int16)
    noise = np.random.normal(loc=0.0, scale=random.uniform(noise_sigma_min, noise_sigma_max), size=arr.shape)
    arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
    scanned = Image.fromarray(arr, mode="RGB")

    # Slight contrast boost by stretching histogram-ish
    scanned = scanned.point(lambda p: max(0, min(255, int((p - 10) * 1.03))))
    return scanned


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    # Best-effort font loading on Windows; fallback to default bitmap font.
    candidates = [
        ("arialbd.ttf" if bold else "arial.ttf"),
        ("calibrib.ttf" if bold else "calibri.ttf"),
        ("tahomabd.ttf" if bold else "tahoma.ttf"),
    ]
    for name in candidates:
        try:
            return ImageFont.truetype(name, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _add_diagonal_watermark(page: Image.Image, text: str) -> Image.Image:
    page_rgba = page.convert("RGBA")
    layer = Image.new("RGBA", page_rgba.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)

    font = _load_font(84, bold=True)
    # Create a temp surface for rotation so watermark is clearly visible.
    tmp = Image.new("RGBA", (int(page_rgba.size[0] * 1.2), int(page_rgba.size[1] * 0.35)), (0, 0, 0, 0))
    dtmp = ImageDraw.Draw(tmp)
    dtmp.text((20, 20), text, font=font, fill=(40, 140, 210, 65))
    tmp = tmp.rotate(18, expand=True)

    # Center-ish placement
    x = int((page_rgba.size[0] - tmp.size[0]) / 2)
    y = int((page_rgba.size[1] - tmp.size[1]) / 2)
    layer.alpha_composite(tmp, (x, y))

    return Image.alpha_composite(page_rgba, layer).convert("RGB")


def _render_demo_avatar(size: tuple[int, int], seed: int) -> Image.Image:
    # Non-photorealistic, cartoon-like avatar (safe for demo use).
    rnd = random.Random(seed)
    w, h = size
    img = Image.new("RGB", (w, h), (236, 240, 246))
    d = ImageDraw.Draw(img)

    skin = rnd.choice([(242, 207, 178), (230, 195, 165), (214, 176, 144), (199, 160, 130)])
    hair = rnd.choice([(35, 35, 35), (60, 45, 35), (20, 20, 25), (80, 60, 45)])
    shirt = rnd.choice([(30, 60, 110), (20, 120, 140), (90, 40, 120), (120, 70, 20)])

    # Shoulders/shirt
    d.rounded_rectangle([int(w * 0.12), int(h * 0.62), int(w * 0.88), int(h * 1.05)], radius=28, fill=shirt)

    # Head
    head_w = int(w * 0.58)
    head_h = int(h * 0.62)
    hx0 = int((w - head_w) / 2)
    hy0 = int(h * 0.12)
    hx1 = hx0 + head_w
    hy1 = hy0 + head_h
    d.ellipse([hx0, hy0, hx1, hy1], fill=skin)

    # Hair cap
    hair_h = int(head_h * 0.40)
    d.pieslice([hx0 - 6, hy0 - 8, hx1 + 6, hy0 + hair_h], start=180, end=360, fill=hair)
    d.rounded_rectangle([hx0 - 6, hy0 + int(hair_h * 0.35), hx1 + 6, hy0 + hair_h], radius=16, fill=hair)
    # Random fringe
    for _ in range(rnd.randint(3, 6)):
        fx = rnd.randint(hx0 + int(head_w * 0.2), hx1 - int(head_w * 0.2))
        fw = rnd.randint(18, 34)
        fh = rnd.randint(20, 46)
        d.rounded_rectangle([fx - fw, hy0 + int(hair_h * 0.55), fx + fw, hy0 + int(hair_h * 0.55) + fh], radius=10, fill=hair)

    # Eyes
    eye_y = hy0 + int(head_h * 0.42)
    eye_dx = int(head_w * 0.17)
    eye_r = max(4, int(min(w, h) * 0.02))
    cx = int((hx0 + hx1) / 2)
    left_x = cx - eye_dx
    right_x = cx + eye_dx
    d.ellipse([left_x - eye_r, eye_y - eye_r, left_x + eye_r, eye_y + eye_r], fill=(25, 25, 25))
    d.ellipse([right_x - eye_r, eye_y - eye_r, right_x + eye_r, eye_y + eye_r], fill=(25, 25, 25))

    # Nose
    nose_y0 = eye_y + int(head_h * 0.10)
    d.line([cx, nose_y0, cx - 6, nose_y0 + 22], fill=(120, 90, 70), width=3)

    # Mouth
    mouth_y = hy0 + int(head_h * 0.78)
    d.arc([cx - 34, mouth_y - 8, cx + 34, mouth_y + 18], start=10, end=170, fill=(140, 60, 60), width=4)

    # Light paper/grain
    arr = np.array(img).astype(np.int16)
    noise = np.random.normal(loc=0.0, scale=3.0, size=arr.shape)
    arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
    return Image.fromarray(arr, mode="RGB")


def _stable_doc_uuid(entity_type: str, entity_id: int, doc_type: str) -> str:
    # Stable per (entity_type, entity_id, doc_type) so incremental compares are meaningful
    name = f"{entity_type}:{entity_id}:{doc_type}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, name))


def generate_savings_book_scan(
    user_id: int,
    balance: float,
    run_date: str,
    output_format: str = "pdf",
    txn_date: str | None = None,
    txn_code: str = "OPN",
    txn_amount: float | None = None,
    balance_after: float | None = None,
    interest_rate: float | None = None,
    signature_text: str | None = None,
) -> dict:
    issue_date = fake.date_between('-2y', 'today')
    expiry_date = issue_date + timedelta(days=random.choice([180, 365, 730]))
    fields = {
        "Họ và tên": fake.name(),
        "Số CCCD": fake.numerify("0##########"),
        "Số tài khoản": fake.numerify("################"),
        "Số tiền gửi": f"{balance:,.0f} VND",
        "Lãi suất": f"{random.uniform(4.5, 7.2):.1f}%/năm",
        "Ngày mở": issue_date.strftime("%d/%m/%Y"),
        "Ngày đáo hạn": expiry_date.strftime("%d/%m/%Y"),
        "Chi nhánh": fake.city(),
    }

    customer_name = fields["Họ và tên"]

    base_dir = (
        DEFAULT_UNSTRUCTURED_ROOT
        / "documents"
        / "doc_type=savings_book"
        / f"run_date={run_date}"
        / f"user_id={user_id}"
    )
    base_dir.mkdir(parents=True, exist_ok=True)
    if output_format not in {"pdf", "jpg"}:
        raise ValueError("output_format must be 'pdf' or 'jpg'")

    out_path = base_dir / f"savings_book_scan.{output_format}"

    # Create a raster page in a ledger/table style (DEMO) for testing/OCR.
    page = Image.new("RGB", (1654, 2339), color=(248, 248, 248))  # ~A4 at 200dpi

    # Prominent DEMO watermark + disclaimer (safe sample; not a real bank document)
    page = _add_diagonal_watermark(page, "SAMPLE / DEMO - NOT A REAL BANK DOCUMENT")

    draw = ImageDraw.Draw(page)
    margin = 80
    x0, y0 = margin, margin
    x1, y1 = page.size[0] - margin, page.size[1] - margin

    # Title / disclaimer header
    title_font = _load_font(34, bold=True)
    small_font = _load_font(22, bold=False)
    row_font = _load_font(24, bold=False)
    row_font_bold = _load_font(24, bold=True)
    draw.text((x0 + 10, y0 - 55), "DEMO SAVINGS BOOK TRANSACTION PAGE", font=title_font, fill=(10, 10, 10))
    draw.text((x0 + 10, y0 - 20), "FOR TESTING ONLY - DO NOT USE AS A REAL DOCUMENT", font=small_font, fill=(40, 40, 40))

    # Outer border
    draw.rectangle([x0, y0, x1, y1], outline=(40, 40, 40), width=3)

    # Table layout
    header_h = 150
    row_h = 260
    table_top = y0 + 40
    table_left = x0 + 10
    table_right = x1 - 10
    table_bottom = y1 - 260

    # Column boundaries (safe, readable widths; keep Signature inside table)
    # Total inner width ~1470px at current margins.
    date_w = 240
    code_w = 120
    txn_w = 480
    bal_w = 280
    ir_w = 150
    sig_w = max(180, int((table_right - table_left) - (date_w + code_w + txn_w + bal_w + ir_w)))

    col_x = [
        table_left,
        table_left + date_w,                              # Date
        table_left + date_w + code_w,                     # Code
        table_left + date_w + code_w + txn_w,             # Transaction amount
        table_left + date_w + code_w + txn_w + bal_w,     # Balance
        table_left + date_w + code_w + txn_w + bal_w + ir_w,  # Interest
        table_right,                                      # Signature
    ]

    # Header row
    draw.rectangle([table_left, table_top, table_right, table_top + header_h], outline=(40, 40, 40), width=2)
    for cx in col_x[1:-1]:
        draw.line([cx, table_top, cx, table_top + header_h], fill=(40, 40, 40), width=2)

    headers = [
        ("Ngày", "Date"),
        ("Mã", "Code"),
        ("Số tiền giao dịch", "Transaction amount"),
        ("Số dư", "Balance"),
        ("Lãi suất", "IR"),
        ("Chữ ký", "Signature"),
    ]
    for i, (vn, en) in enumerate(headers):
        hx0, hx1 = col_x[i], col_x[i + 1]
        draw.text((hx0 + 12, table_top + 20), vn, font=small_font, fill=(0, 0, 0))
        draw.text((hx0 + 12, table_top + 85), en, font=small_font, fill=(40, 40, 40))

    # Data row 1 (opening)
    row1_top = table_top + header_h
    row1_bottom = row1_top + row_h
    draw.rectangle([table_left, row1_top, table_right, row1_bottom], outline=(40, 40, 40), width=2)
    for cx in col_x[1:-1]:
        draw.line([cx, row1_top, cx, row1_bottom], fill=(40, 40, 40), width=2)

    date_open = txn_date or issue_date.strftime("%d/%m/%Y")
    code_open = (txn_code or "OPN").strip()[:8]
    amount_open = float(txn_amount) if txn_amount is not None else float(balance)
    bal_after = float(balance_after) if balance_after is not None else float(amount_open)
    ir_val = float(interest_rate) if interest_rate is not None else random.uniform(4.5, 7.2)
    ir = f"{ir_val:.1f}%/Năm"

    draw.text((col_x[0] + 12, row1_top + 18), date_open, font=row_font, fill=(0, 0, 0))
    draw.text((col_x[0] + 12, row1_top + 70), "Mở tài khoản", font=row_font, fill=(0, 0, 0))

    draw.text((col_x[1] + 12, row1_top + 18), code_open, font=row_font_bold, fill=(0, 0, 0))

    draw.text((col_x[2] + 12, row1_top + 18), f"{amount_open:,.0f}", font=row_font_bold, fill=(0, 0, 0))
    draw.text((col_x[3] + 12, row1_top + 18), f"{bal_after:,.0f}", font=row_font_bold, fill=(0, 0, 0))
    draw.text((col_x[4] + 12, row1_top + 18), ir, font=row_font, fill=(0, 0, 0))

    # Signature field (safe placeholder: text only; no handwritten signature generation)
    sig_x0 = col_x[5] + 15
    sig_label = (signature_text or "DEMO").strip()
    sig_label = sig_label[:32] if sig_label else "DEMO"
    draw.text((sig_x0, row1_top + 60), sig_label, font=small_font, fill=(10, 10, 10))
    draw.text((sig_x0, row1_top + 120), customer_name, font=small_font, fill=(10, 10, 10))

    # Mid-page circular stamp (generic)
    stamp_center = (int((col_x[2] + col_x[4]) / 2), int(row1_top + 350))
    r_outer, r_inner = 130, 112
    bbox_outer = [stamp_center[0] - r_outer, stamp_center[1] - r_outer, stamp_center[0] + r_outer, stamp_center[1] + r_outer]
    bbox_inner = [stamp_center[0] - r_inner, stamp_center[1] - r_inner, stamp_center[0] + r_inner, stamp_center[1] + r_inner]
    draw.ellipse(bbox_outer, outline=(140, 0, 0), width=5)
    draw.ellipse(bbox_inner, outline=(140, 0, 0), width=2)
    draw.text((stamp_center[0] - 70, stamp_center[1] - 30), "DEMO BANK", fill=(140, 0, 0))
    draw.text((stamp_center[0] - 105, stamp_center[1] + 10), "FOR TESTING ONLY", fill=(140, 0, 0))

    # Footer notes
    footer_y = table_bottom + 40
    draw.text((table_left + 10, footer_y), "• OPN: Mở tài khoản", fill=(0, 0, 0))
    draw.text((table_left + 10, footer_y + 50), "• IPY: Trả lãi", fill=(0, 0, 0))
    draw.text((table_left + 520, footer_y), "• RNW: Gia hạn", fill=(0, 0, 0))
    draw.text((table_left + 520, footer_y + 50), "• CLS: Đóng tài khoản", fill=(0, 0, 0))
    draw.text((table_left + 1040, footer_y), "• ECL: Đóng sớm", fill=(0, 0, 0))
    draw.text((table_left + 1040, footer_y + 50), "• OCL: Đóng quá hạn", fill=(0, 0, 0))

    page = _scanify_image(
        page,
        angle_range=0.6,
        blur_min=0.0,
        blur_max=0.35,
        noise_sigma_min=4.0,
        noise_sigma_max=10.0,
    )
    # Light sharpening to keep text readable after scan effects (keep it fast for bulk gen)
    page = page.filter(ImageFilter.SHARPEN)
    if output_format == "pdf":
        page.save(out_path, "PDF", resolution=200.0)
    else:
        page.save(out_path, quality=85)

    file_size = out_path.stat().st_size
    sha256 = _sha256_file(out_path)

    return {
        "document_id": _stable_doc_uuid("user", user_id, "savings_book"),
        "entity_type": "user",
        "entity_id": user_id,
        "doc_type": "savings_book",
        "file_path": _as_posix_str(out_path),
        "file_format": output_format,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": "ai_generated",
        "sha256": sha256,
        "file_size_bytes": file_size,
        "ocr_text": json.dumps(fields, ensure_ascii=False),
        "run_date": run_date,
    }

def generate_id_card_scan(user_id: int, run_date: str) -> dict:
    base_dir = (
        DEFAULT_UNSTRUCTURED_ROOT
        / "documents"
        / "doc_type=id_card"
        / f"run_date={run_date}"
        / f"user_id={user_id}"
    )
    base_dir.mkdir(parents=True, exist_ok=True)
    img_path = base_dir / "id_card_scan.jpg"

    # IMPORTANT: This is a DEMO ID card for testing only.
    # It intentionally does NOT match real CCCD layout/graphics.
    full_name = fake.name().upper()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=70).strftime('%d/%m/%Y')
    demo_id_no = fake.numerify('DEMO-########')
    sex = random.choice(["Nam", "Nữ"])
    nationality = "Việt Nam"
    origin = fake.city()
    residence = fake.address().replace("\n", ", ")
    city = fake.city()
    address = residence
    issue_date = fake.date_between('-5y', 'today').strftime('%d/%m/%Y')
    expiry_date = fake.date_between('today', '+5y').strftime('%d/%m/%Y')

    img = Image.new('RGB', (1400, 900), color=(245, 246, 248))
    img = _add_diagonal_watermark(img, "SAMPLE / DEMO – NOT A REAL ID")
    draw = ImageDraw.Draw(img)

    title_font = _load_font(42, bold=True)
    label_font = _load_font(24, bold=True)
    value_font = _load_font(26, bold=False)
    small_font = _load_font(20, bold=False)

    # Border and header band
    draw.rounded_rectangle([35, 35, 1365, 865], radius=22, outline=(45, 45, 45), width=5)
    draw.rectangle([35, 35, 1365, 150], fill=(30, 60, 110))
    draw.text((60, 60), "DEMO CITIZEN ID CARD", font=title_font, fill=(255, 255, 255))
    vn_font = _load_font(22, bold=True)
    vn_small = _load_font(20, bold=False)
    draw.text((60, 108), "CỘNG HÒA XÃ HỘI CHỦ NGHĨA VIỆT NAM", font=vn_font, fill=(220, 230, 245))
    draw.text((60, 132), "Độc lập - Tự do - Hạnh phúc", font=vn_small, fill=(220, 230, 245))

    # Neutral DEMO seal (not an official emblem)
    seal_center = (1310, 95)
    r1, r2 = 44, 36
    seal_left = seal_center[0] - r1
    seal_bbox_outer = [
        seal_center[0] - r1,
        seal_center[1] - r1,
        seal_center[0] + r1,
        seal_center[1] + r1,
    ]
    seal_bbox_inner = [
        seal_center[0] - r2,
        seal_center[1] - r2,
        seal_center[0] + r2,
        seal_center[1] + r2,
    ]

    # Keep explicit disclaimer (fit left of the seal, wrap if needed)
    disc1 = "FOR TESTING ONLY"
    disc2 = "NOT A GOVERNMENT DOCUMENT"
    disc_font = _load_font(18, bold=True)
    disc_color = (220, 230, 245)
    disc_x = 780
    max_right = seal_left - 18
    b1 = draw.textbbox((0, 0), disc1, font=disc_font)
    b2 = draw.textbbox((0, 0), disc2, font=disc_font)
    w = max(b1[2] - b1[0], b2[2] - b2[0])
    if disc_x + w > max_right:
        # If still too wide, shift left as needed but keep some padding.
        disc_x = max(60, max_right - w)
    draw.text((disc_x, 78), disc1, font=disc_font, fill=disc_color)
    draw.text((disc_x, 104), disc2, font=disc_font, fill=disc_color)

    draw.ellipse(seal_bbox_outer, outline=(235, 235, 235), width=3)
    draw.ellipse(seal_bbox_inner, outline=(235, 235, 235), width=2)
    draw.text((seal_center[0] - 26, seal_center[1] - 18), "DEMO", font=_load_font(20, bold=True), fill=(235, 235, 235))
    draw.text((seal_center[0] - 30, seal_center[1] + 6), "SAMPLE", font=_load_font(14, bold=False), fill=(235, 235, 235))

    # Photo area with a synthetic demo avatar (non-photorealistic)
    photo_box = (70, 210, 390, 650)
    draw.rounded_rectangle(list(photo_box), radius=16, outline=(70, 70, 70), width=4, fill=(235, 238, 242))
    pw = photo_box[2] - photo_box[0] - 16
    ph = photo_box[3] - photo_box[1] - 16
    avatar = _render_demo_avatar((pw, ph), seed=user_id)
    img.paste(avatar, (photo_box[0] + 8, photo_box[1] + 8))
    # Overlay label so it is clearly not a real photo
    draw.rectangle([photo_box[0] + 8, photo_box[3] - 44, photo_box[2] - 8, photo_box[3] - 8], fill=(0, 0, 0, 90))
    draw.text((photo_box[0] + 18, photo_box[3] - 40), "DEMO AVATAR", font=_load_font(18, bold=True), fill=(255, 255, 255))

    # Fields block
    x_label = 430
    y = 210
    field_gap = 58
    fields = [
        ("FULL NAME", full_name),
        ("DEMO ID NO", demo_id_no),
        ("DATE OF BIRTH", dob),
        ("SEX", sex),
        ("NATIONALITY", nationality),
        ("PLACE OF ORIGIN", origin),
        ("PLACE OF RESIDENCE", residence[:44] + ("…" if len(residence) > 44 else "")),
        ("ISSUE DATE", issue_date),
        ("EXPIRY DATE", expiry_date),
    ]

    # Compute a safe x for value column so long labels never overlap values
    label_texts = [label + ":" for label, _ in fields]
    max_label_w = 0
    for t in label_texts:
        bbox = draw.textbbox((0, 0), t, font=label_font)
        max_label_w = max(max_label_w, bbox[2] - bbox[0])
    x_value = min(1050, x_label + max_label_w + 26)
    for label, value in fields:
        draw.text((x_label, y), label + ":", font=label_font, fill=(25, 25, 25))
        draw.text((x_value, y), str(value), font=value_font, fill=(10, 10, 10))
        y += field_gap

    # Footer disclaimer (move up + add band so border never overlaps text)
    draw.rectangle([50, 770, 1350, 852], fill=(245, 246, 248))
    draw.text((60, 785), "SAMPLE / DEMO – NOT A REAL ID", font=label_font, fill=(120, 0, 0))
    draw.text((60, 820), f"Generated for user_id={user_id} on run_date={run_date}", font=small_font, fill=(45, 45, 45))

    img = _scanify_image(img, angle_range=0.8, blur_min=0.0, blur_max=0.35, noise_sigma_min=3.5, noise_sigma_max=9.0)
    img = img.filter(ImageFilter.SHARPEN)
    img.save(img_path, quality=85)

    file_size = img_path.stat().st_size
    sha256 = _sha256_file(img_path)

    return {
        "document_id": _stable_doc_uuid("user", user_id, "id_card"),
        "entity_type": "user",
        "entity_id": user_id,
        "doc_type": "id_card",
        "file_path": _as_posix_str(img_path),
        "file_format": "jpg",
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "source": "ai_generated",
        "sha256": sha256,
        "file_size_bytes": file_size,
        "ocr_text": None,
        "run_date": run_date,
    }

# =====================
# INSERT VÀO TARGET DB
# =====================
def insert_document(cursor, doc: dict):
    cursor.execute("""
        INSERT INTO metadata.documents
            (entity_type, entity_id, doc_type, file_path,
             file_format, ocr_text, upload_date, ocr_status, is_verified)
        VALUES (?, ?, ?, ?, ?, ?, GETDATE(), 'pending', 0)
    """,
        doc["entity_type"],
        doc["entity_id"],
        doc["doc_type"],
        doc["file_path"],
        doc["file_format"],
        doc.get("ocr_text", None),
    )


def _write_manifest(manifest_dir: Path, run_date: str, rows: list[dict]) -> Path:
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"documents_{run_date}.csv"
    fieldnames = [
        "document_id",
        "entity_type",
        "entity_id",
        "doc_type",
        "file_path",
        "file_format",
        "created_at",
        "source",
        "sha256",
        "file_size_bytes",
        "ocr_text",
        "run_date",
    ]
    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
    return manifest_path


def _generate_offline_users(num_users: int, start_user_id: int) -> list[tuple[int, float]]:
    users: list[tuple[int, float]] = []
    for i in range(num_users):
        user_id = start_user_id + i
        # Yearly income (VND) — wide distribution so some users qualify for savings book
        income = float(max(0, random.gauss(mu=65000, sigma=25000)))
        users.append((user_id, income))
    return users


def _read_users_csv(csv_path: Path) -> list[tuple[int, float]]:
    users: list[tuple[int, float]] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("users CSV has no header")
        # Accept common header variants
        id_key = "user_id" if "user_id" in reader.fieldnames else ("id" if "id" in reader.fieldnames else None)
        income_key = (
            "yearly_income"
            if "yearly_income" in reader.fieldnames
            else ("income" if "income" in reader.fieldnames else None)
        )
        if id_key is None:
            raise ValueError("users CSV must contain column 'user_id' or 'id'")
        if income_key is None:
            raise ValueError("users CSV must contain column 'yearly_income' or 'income'")

        for row in reader:
            user_id = int(row[id_key])
            income_raw = row.get(income_key)
            income = float(income_raw) if income_raw not in (None, "") else 0.0
            users.append((user_id, income))
    return users

# =====================
# MAIN
# =====================
def main():
    parser = argparse.ArgumentParser(description="Generate unstructured banking documents linked to users")
    parser.add_argument(
        "--run-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="YYYY-MM-DD",
    )
    parser.add_argument("--write-db", action="store_true", help="Insert metadata into target DB (metadata.*)")
    parser.add_argument(
        "--use-source-db",
        action="store_true",
        help="Read user_id/yearly_income from SOURCE SQL Server (opt-in)",
    )
    parser.add_argument(
        "--users-csv",
        default="",
        help="CSV path with columns user_id(or id) and yearly_income(or income); avoids DB",
    )
    parser.add_argument(
        "--num-users",
        type=int,
        default=100,
        help="Offline mode: number of users to generate (default 100)",
    )
    parser.add_argument(
        "--start-user-id",
        type=int,
        default=0,
        help="Offline mode: starting user_id (default 0)",
    )
    parser.add_argument("--manifest-dir", default=str(DEFAULT_MANIFEST_DIR), help="Where to write manifest CSV")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of users processed (0 = all)")
    parser.add_argument(
        "--savings-book-format",
        choices=["pdf", "jpg"],
        default="jpg",
        help="Output format for savings book document (default jpg)",
    )
    parser.add_argument(
        "--force-savings-book",
        action="store_true",
        help="Generate savings book for every processed user (ignores income threshold)",
    )
    parser.add_argument(
        "--only-savings-book",
        action="store_true",
        help="Only generate savings book documents (skip ID card generation)",
    )
    parser.add_argument(
        "--only-id-card",
        action="store_true",
        help="Only generate DEMO ID card documents (skip savings book generation)",
    )
    parser.add_argument("--sb-txn-date", default="", help="Override savings book row date (e.g. 25/11/2023)")
    parser.add_argument("--sb-txn-code", default="", help="Override savings book row code (e.g. OPN)")
    parser.add_argument("--sb-txn-amount", type=float, default=0.0, help="Override transaction amount")
    parser.add_argument("--sb-balance", type=float, default=0.0, help="Override balance after transaction")
    parser.add_argument("--sb-interest", type=float, default=0.0, help="Override interest rate percent (e.g. 6.2)")
    parser.add_argument("--sb-signature-text", default="", help="Override signature field text (text only; no handwritten)")
    args = parser.parse_args()

    run_date = args.run_date
    manifest_dir = Path(args.manifest_dir)

    # Determine user list (offline by default)
    users: list[tuple[int, float]]
    if args.users_csv:
        users = _read_users_csv(Path(args.users_csv))
        print(f"Đang đọc users từ CSV: {args.users_csv}")
    elif args.use_source_db:
        print("Đang kết nối SOURCE server...")
        src_conn = get_source_conn()
        src_cursor = src_conn.cursor()
        print("Đang lấy danh sách users từ source...")
        src_cursor.execute("SELECT id, yearly_income FROM banking.users")
        rows = src_cursor.fetchall()
        src_conn.close()
        users = [(int(r[0]), float(r[1]) if r[1] is not None else 0.0) for r in rows]
    else:
        users = _generate_offline_users(args.num_users, args.start_user_id)
        print(f"Offline mode: sinh {len(users)} users giả lập")

    if args.limit and args.limit > 0:
        users = users[: args.limit]
    print(f"Số users sẽ xử lý: {len(users)}")

    tgt_conn = None
    tgt_cursor = None
    if args.write_db:
        print("Đang kết nối TARGET server...")
        tgt_conn = get_target_conn()
        tgt_cursor = tgt_conn.cursor()

    success, failed = 0, 0
    manifest_rows: list[dict] = []

    for user_id, income in users:
        try:
            if not args.only_savings_book:
                # 1) CCCD scan image
                id_doc = generate_id_card_scan(user_id, run_date=run_date)
                manifest_rows.append(id_doc)
                if tgt_cursor is not None:
                    insert_document(tgt_cursor, id_doc)

            # 2) Savings book scan PDF (only for higher income)
            if (not args.only_id_card) and (args.force_savings_book or (income and income > 50000)):
                # Use income-derived balance unless user overrides txn amount/balance via CLI
                balance = float(income) * random.uniform(0.5, 2.0) if income else random.uniform(50_000, 200_000_000)
                txn_date = args.sb_txn_date.strip() or None
                txn_code = args.sb_txn_code.strip() or "OPN"
                txn_amount = args.sb_txn_amount if args.sb_txn_amount > 0 else None
                balance_after = args.sb_balance if args.sb_balance > 0 else None
                interest_rate = args.sb_interest if args.sb_interest > 0 else None
                signature_text = args.sb_signature_text.strip() or None
                sav_doc = generate_savings_book_scan(
                    user_id,
                    balance,
                    run_date=run_date,
                    output_format=args.savings_book_format,
                    txn_date=txn_date,
                    txn_code=txn_code,
                    txn_amount=txn_amount,
                    balance_after=balance_after,
                    interest_rate=interest_rate,
                    signature_text=signature_text,
                )
                manifest_rows.append(sav_doc)
                if tgt_cursor is not None:
                    insert_document(tgt_cursor, sav_doc)

            if tgt_conn is not None:
                tgt_conn.commit()
            success += 1
            print(f"  ✅ user_id={user_id}")

        except Exception as e:
            if tgt_conn is not None:
                tgt_conn.rollback()
            failed += 1
            print(f"  ❌ user_id={user_id} — Lỗi: {e}")

    manifest_path = _write_manifest(manifest_dir, run_date, manifest_rows)
    print(f"\nĐã ghi manifest: {manifest_path.as_posix()}")
    print(f"Hoàn tất: {success} thành công, {failed} thất bại.")
    if tgt_conn is not None:
        tgt_conn.close()

if __name__ == "__main__":
    main()