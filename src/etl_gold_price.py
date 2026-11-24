import os
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

# โหลดค่าตัวแปรจากไฟล์ .env (สำหรับรันทดสอบในเครื่อง; บน GitHub Actions จะอ่านจาก Secrets อัตโนมัติ)
load_dotenv()

# ตั้งค่า Logging ให้แสดงเวลาและระดับความสำคัญชัดเจนบน Console / GitHub Actions Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Constants & Configuration ---
XAU_API_URL = "https://api.gold-api.com/price/XAU"
FX_API_URL = "https://open.er-api.com/v6/latest/USD"
THAI_GOLD_API_URL = "https://api.chnwt.dev/thai-gold-api/latest"
LINE_PUSH_API_URL = "https://api.line.me/v2/bot/message/push"

TROY_OUNCE_TO_GRAMS = 31.1034768
THAI_BAHT_GOLD_GRAMS = 15.244
THAI_GOLD_PURITY = 0.965

DATASET_ID = "gold_market_dw"
TABLE_ID = "daily_gold_prices"


def send_line_message(message_text: str) -> None:
    """ส่งข้อความแจ้งเตือนผ่าน LINE Messaging API (รองรับผู้รับหลายคนคั่นด้วยเครื่องหมายจุลภาค)"""
    access_token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "").strip()
    raw_user_ids = os.getenv("LINE_USER_ID", "").strip()

    if not access_token or not raw_user_ids:
        logger.warning(
            "ไม่พบค่า LINE_CHANNEL_ACCESS_TOKEN หรือ LINE_USER_ID ข้ามการส่งแจ้งเตือน LINE")
        return

    user_ids = [uid.strip() for uid in raw_user_ids.split(",") if uid.strip()]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    for uid in user_ids:
        payload = {
            "to": uid,
            "messages": [{"type": "text", "text": message_text}],
        }
        try:
            resp = requests.post(
                LINE_PUSH_API_URL, headers=headers, json=payload, timeout=15)
            resp.raise_for_status()
            logger.info(f"ส่งข้อความเข้า LINE สำเร็จ (User ID: {uid[:6]}...)")
        except requests.RequestException as exc:
            logger.error(
                f"ส่งข้อความเข้า LINE ไม่สำเร็จสำหรับ {uid[:6]}...: {exc}")


def fetch_global_market_data() -> tuple[float, float]:
    """1A. ดึงราคาทองคำสากล (XAU/USD) และอัตราแลกเปลี่ยน (USD/THB) — แหล่งข้อมูลหลัก"""
    logger.info("กำลังดึงข้อมูลราคาทองคำสากล XAU/USD...")
    xau_resp = requests.get(XAU_API_URL, timeout=15)
    xau_resp.raise_for_status()
    xau_usd_oz = float(xau_resp.json().get("price"))

    logger.info("กำลังดึงข้อมูลอัตราแลกเปลี่ยน USD/THB...")
    fx_resp = requests.get(FX_API_URL, timeout=15)
    fx_resp.raise_for_status()
    usd_thb_rate = float(fx_resp.json().get("rates", {}).get("THB"))

    return xau_usd_oz, usd_thb_rate


def _parse_thai_price(value_str: str | None) -> float | None:
    """แปลงข้อความตัวเลขที่มีลูกน้ำ (เช่น '43,250.00') เป็น float"""
    if not value_str or str(value_str).strip() in ("-", ""):
        return None
    return float(str(value_str).replace(",", "").strip())


def fetch_thai_gold_data() -> dict:
    """1B. ดึงราคาประกาศสมาคมค้าทองคำไทย พร้อมระบบ Graceful Degradation (ถ้าล่มไม่ให้ Pipeline พัง)"""
    fallback_result = {
        "thai_bar_buy_thb": None,
        "thai_bar_sell_thb": None,
        "thai_ornament_buy_thb": None,
        "thai_ornament_sell_thb": None,
        "thai_api_status": "FALLBACK_NULL",
    }

    try:
        logger.info("กำลังดึงข้อมูลราคาประกาศสมาคมค้าทองคำไทย...")
        resp = requests.get(THAI_GOLD_API_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        price_block = data.get("response", {}).get("price", {})
        gold_bar = price_block.get("gold_bar", {})
        gold_ornament = price_block.get("gold", {})

        bar_buy = _parse_thai_price(gold_bar.get("buy"))
        bar_sell = _parse_thai_price(gold_bar.get("sell"))
        ornament_buy = _parse_thai_price(gold_ornament.get("buy"))
        ornament_sell = _parse_thai_price(gold_ornament.get("sell"))

        if bar_buy is None or bar_sell is None:
            raise ValueError("ข้อมูลราคาทองคำแท่งจาก API ไทยไม่สมบูรณ์")

        logger.info("ดึงข้อมูลราคาสมาคมค้าทองคำไทยสำเร็จ")
        return {
            "thai_bar_buy_thb": bar_buy,
            "thai_bar_sell_thb": bar_sell,
            "thai_ornament_buy_thb": ornament_buy,
            "thai_ornament_sell_thb": ornament_sell,
            "thai_api_status": "SUCCESS",
        }
    except Exception as exc:
        logger.warning(
            f"Thai Gold API ขัดข้อง ({exc}) -> เปิดใช้งานโหมด Graceful Degradation (บันทึกค่า Null)")
        return fallback_result


def transform_and_validate(xau_usd_oz: float, usd_thb_rate: float, thai_data: dict) -> pd.DataFrame:
    """2. แปลงหน่วยคำนวณราคาทองคำ 96.5%, ระบุรอบเช้า-บ่าย และตรวจสอบ Data Quality Gate"""
    # Data Quality Check: ตรวจสอบค่าวิกฤตจากตลาดโลก
    if xau_usd_oz is None or xau_usd_oz <= 0:
        raise ValueError(
            f"Data Quality Failed: ราคา XAU/USD ผิดปกติ ({xau_usd_oz})")
    if usd_thb_rate is None or usd_thb_rate <= 0:
        raise ValueError(
            f"Data Quality Failed: อัตราแลกเปลี่ยน USD/THB ผิดปกติ ({usd_thb_rate})")

    # คำนวณเวลาปัจจุบันตามโซนประเทศไทย (Asia/Bangkok) เพื่อแยก SESSION เช้า-เย็น
    bkk_tz = ZoneInfo("Asia/Bangkok")
    now_bkk = datetime.now(bkk_tz)
    record_date = now_bkk.date()
    session = "MORNING" if now_bkk.hour < 12 else "AFTERNOON"
    ingested_at_utc = datetime.now(timezone.utc)

    # สูตรคำนวณราคาทองคำแท่งไทย 96.5% ต่อ 1 บาททองคำ (15.244 กรัม) และปัดเศษทีละ 50 บาท
    raw_global_965 = (
        (xau_usd_oz * usd_thb_rate)
        * (THAI_BAHT_GOLD_GRAMS / TROY_OUNCE_TO_GRAMS)
        * THAI_GOLD_PURITY
    )
    global_calc_965_thb = float(round(raw_global_965 / 50.0) * 50.0)

    # คำนวณส่วนต่างราคาสมาคมฯ เทียบกับราคาตลาดโลก (Thai Market Premium)
    thai_bar_sell = thai_data["thai_bar_sell_thb"]
    thai_market_premium = (
        float(thai_bar_sell -
              global_calc_965_thb) if thai_bar_sell is not None else None
    )

    row = {
        "record_date": record_date,
        "session": session,
        "xau_usd_oz": round(xau_usd_oz, 2),
        "usd_thb_rate": round(usd_thb_rate, 4),
        "global_calc_965_thb": global_calc_965_thb,
        "thai_bar_buy_thb": thai_data["thai_bar_buy_thb"],
        "thai_bar_sell_thb": thai_bar_sell,
        "thai_ornament_buy_thb": thai_data["thai_ornament_buy_thb"],
        "thai_ornament_sell_thb": thai_data["thai_ornament_sell_thb"],
        "thai_market_premium_thb": thai_market_premium,
        "thai_api_status": thai_data["thai_api_status"],
        "ingested_at": ingested_at_utc,
    }

    df = pd.DataFrame([row])
    logger.info(
        f"Transform สำเร็จ | วันที่: {record_date} | รอบ: {session} | "
        f"คำนวณตลาดโลก: {global_calc_965_thb:,.0f} บ. | สมาคมฯ ขายออก: {thai_bar_sell}"
    )
    return df


def load_to_bigquery_idempotent(df: pd.DataFrame) -> None:
    """3. โหลดข้อมูลเข้า BigQuery แบบ Partition-Level Atomic Overwrite (รองรับ BigQuery Sandbox 100%)"""
    project_id = os.getenv("GCP_PROJECT_ID", "").strip()
    if not project_id:
        raise EnvironmentError(
            "ไม่พบค่า GCP_PROJECT_ID ใน Environment Variables")

    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{DATASET_ID}.{TABLE_ID}"

    target_date = df.loc[0, "record_date"]
    target_session = df.loc[0, "session"]
    partition_suffix = target_date.strftime("%Y%m%d")
    partition_table_id = f"{full_table_id}${partition_suffix}"

    # 1. ดึงข้อมูลที่มีอยู่แล้วเฉพาะของวันที่รัน (SELECT ใช้งานได้ฟรีบน BigQuery Sandbox)
    select_query = f"""
        SELECT *
        FROM `{full_table_id}`
        WHERE record_date = @target_date
    """
    query_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date),
        ]
    )

    logger.info(
        f"กำลังตรวจสอบข้อมูลเดิมของวันที่ {target_date} ใน BigQuery...")
    existing_df = client.query(
        select_query, job_config=query_config).to_dataframe()

    # 2. กรองเอาเฉพาะรอบอื่นในวันเดียวกันเก็บไว้ (เช่น รันรอบเย็น จะเก็บข้อมูลรอบเช้าไว้ไม่ให้หาย)
    if not existing_df.empty:
        existing_df = existing_df[existing_df["session"] != target_session]

    if not existing_df.empty:
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df.copy()

    # ปรับชนิดdataให้ตรงกับ Schema ก่อนโหลดเข้า BigQuery
    combined_df["record_date"] = pd.to_datetime(
        combined_df["record_date"]).dt.date
    combined_df["ingested_at"] = pd.to_datetime(
        combined_df["ingested_at"], utc=True)

    float_cols = [
        "xau_usd_oz",
        "usd_thb_rate",
        "global_calc_965_thb",
        "thai_bar_buy_thb",
        "thai_bar_sell_thb",
        "thai_ornament_buy_thb",
        "thai_ornament_sell_thb",
        "thai_market_premium_thb",
    ]
    for col in float_cols:
        combined_df[col] = pd.to_numeric(combined_df[col], errors="coerce")

    schema = [
        bigquery.SchemaField("record_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("session", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("xau_usd_oz", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("usd_thb_rate", "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("global_calc_965_thb",
                             "FLOAT64", mode="REQUIRED"),
        bigquery.SchemaField("thai_bar_buy_thb", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("thai_bar_sell_thb", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("thai_ornament_buy_thb",
                             "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("thai_ornament_sell_thb",
                             "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("thai_market_premium_thb",
                             "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("thai_api_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    # 3. เขียนทับเฉพาะ Partition ของวันนั้น ($YYYYMMDD) แบบ Atomic (ไม่ต้องใช้คำสั่ง DELETE)
    load_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info(
        f"กำลังโหลดข้อมูลเข้าสู่ Partition {partition_table_id} (รอบ {target_session})...")
    load_job = client.load_table_from_dataframe(
        combined_df, partition_table_id, job_config=load_config
    )
    load_job.result()
    logger.info("โหลดข้อมูลเข้า BigQuery สำเร็จสมบูรณ์!")


def format_mom_line_report(df: pd.DataFrame) -> str:
    """4. จัดรูปแบบข้อความรายงานราคาทองคำให้อ่านง่าย สบายตา สำหรับส่งให้คุณแม่ใน LINE"""
    row = df.iloc[0]
    date_str = row["record_date"].strftime("%d/%m/%Y")
    session_thai = "รอบเช้า ☀️" if row["session"] == "MORNING" else "รอบเย็น 🌤️"

    lines = [
        f"✨ รายงานราคาทองคำ ({session_thai})",
        f"📅 ประจำวันที่ {date_str}",
        "━━━━━━━━━━━━━━━━━━",
    ]

    if row["thai_api_status"] == "SUCCESS":
        lines.extend([
            "🏆 ราคาสมาคมค้าทองคำ (บาทละ)",
            f"• ทองคำแท่ง ขายออก : {row['thai_bar_sell_thb']:,.0f} บาท",
            f"• ทองคำแท่ง รับซื้อ  : {row['thai_bar_buy_thb']:,.0f} บาท",
            f"• ทองรูปพรรณ ขายออก: {row['thai_ornament_sell_thb']:,.0f} บาท",
            "",
        ])
    else:
        lines.extend([
            "⚠️ ระบบประกาศสมาคมฯ ขัดข้องชั่วคราว",
            "(แสดงราคาคำนวณจากตลาดโลกแทนครับ)",
            "",
        ])

    lines.extend([
        "🌍 เทียบราคาตลาดโลก (96.5%)",
        f"• ราคาคำนวณจากตลาดโลก: {row['global_calc_965_thb']:,.0f} บาท",
        f"• ทองโลก: ${row['xau_usd_oz']:,.2f} | ค่าเงิน: {row['usd_thb_rate']:.2f} บ.",
    ])

    if pd.notna(row["thai_market_premium_thb"]):
        diff = row["thai_market_premium_thb"]
        sign = "+" if diff > 0 else ""
        lines.append(f"• ส่วนต่างสมาคมฯ vs โลก: {sign}{diff:,.0f} บาท")

    dashboard_url = os.getenv("LOOKER_DASHBOARD_URL", "").strip()
    if dashboard_url and not dashboard_url.endswith("your-dashboard-id"):
        lines.extend([
            "━━━━━━━━━━━━━━━━━━",
            f"📊 กดดูกราฟย้อนหลังได้ที่นี่:\n{dashboard_url}",
        ])

    return "\n".join(lines)


def main() -> None:
    logger.info("=== เริ่มการทำงาน Automated Gold Price ETL Pipeline ===")
    try:
        xau_usd_oz, usd_thb_rate = fetch_global_market_data()
        thai_data = fetch_thai_gold_data()
        df = transform_and_validate(xau_usd_oz, usd_thb_rate, thai_data)
        load_to_bigquery_idempotent(df)

        # ส่งเข้า LINE เฉพาะรอบ 10:00 น. และ 16:00 น. เวลาไทย (หรือตอนกดปุ่ม Run workflow ทดสอบเองบน GitHub)
        now_bkk = datetime.now(ZoneInfo("Asia/Bangkok"))
        github_event = os.getenv("GITHUB_EVENT_NAME", "").strip()

        if now_bkk.hour in (10, 16) or github_event == "workflow_dispatch":
            report_msg = format_mom_line_report(df)
            send_line_message(report_msg)
        else:
            logger.info(
                f"อัปโหลดข้อมูลเข้า BigQuery สำเร็จ (รอบ {now_bkk.strftime('%H:%M')} น. — ข้ามการส่ง LINE นอกรอบ 10:00 / 16:00 น.)"
            )

        logger.info("=== Pipeline ทำงานเสร็จสิ้นสมบูรณ์ ===")

    except Exception as exc:
        error_msg = f"🚨 แจ้งเตือนระบบขัดข้อง (Gold ETL Pipeline):\n{exc}"
        logger.error(error_msg)
        send_line_message(error_msg)
        raise  # โยน Error ต่อเพื่อให้ GitHub Actions แสดงสถานะ Failed (สีแดง)


if __name__ == "__main__":
    main()
