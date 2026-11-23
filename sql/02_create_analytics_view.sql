CREATE OR REPLACE VIEW `gold_market_dw.vw_gold_price_analytics` AS
WITH ordered_prices AS (
  SELECT
    record_date,
    session,
    CASE
      WHEN session = 'MORNING' THEN 1
      ELSE 2
    END AS session_order,
    CASE
      WHEN session = 'MORNING' THEN 'รอบเช้า'
      ELSE 'รอบเย็น'
    END AS session_th,
    -- สร้าง Timestamp เชิงตรรกะ (10:00 และ 16:00) เพื่อให้ Looker Studio พล็อตกราฟเรียงตามเวลาได้แม่นยำ
    DATETIME(
      record_date,
      IF(session = 'MORNING', TIME(10, 0, 0), TIME(16, 0, 0))
    ) AS session_datetime,
    xau_usd_oz,
    usd_thb_rate,
    global_calc_965_thb,
    thai_bar_buy_thb,
    thai_bar_sell_thb,
    thai_ornament_buy_thb,
    thai_ornament_sell_thb,
    thai_market_premium_thb,
    thai_api_status,
    DATETIME(ingested_at, "Asia/Bangkok") AS ingested_at_bkk
  FROM `gold_market_dw.daily_gold_prices`
)
SELECT
  record_date,
  session,
  session_th,
  session_datetime,
  xau_usd_oz,
  usd_thb_rate,
  global_calc_965_thb,
  thai_bar_buy_thb,
  thai_bar_sell_thb,
  thai_ornament_buy_thb,
  thai_ornament_sell_thb,
  thai_market_premium_thb,
  thai_api_status,
  ingested_at_bkk,

  -- 1. ใช้ LAG() คำนวณส่วนต่างราคาเทียบกับรอบก่อนหน้า (บ่ายเทียบเช้า / เช้าเทียบเมื่อวานบ่าย)
  thai_bar_sell_thb - LAG(thai_bar_sell_thb) OVER (
    ORDER BY record_date ASC, session_order ASC
  ) AS thai_bar_sell_diff,

  global_calc_965_thb - LAG(global_calc_965_thb) OVER (
    ORDER BY record_date ASC, session_order ASC
  ) AS global_calc_diff,

  -- 2. Moving Average 7 วัน (คำนวณจาก 14 รอบล่าสุด เพราะเก็บข้อมูลวันละ 2 รอบ)
  ROUND(
    AVG(thai_bar_sell_thb) OVER (
      ORDER BY record_date ASC, session_order ASC
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ), 2
  ) AS thai_bar_sell_ma7d,

  ROUND(
    AVG(global_calc_965_thb) OVER (
      ORDER BY record_date ASC, session_order ASC
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ), 2
  ) AS global_calc_ma7d,

  -- 3. ระบุแถวข้อมูลล่าสุดสำหรับทำ Filter บน Scorecard ใน Looker Studio
  IF(
    ROW_NUMBER() OVER (ORDER BY record_date DESC, session_order DESC) = 1,
    'LATEST',
    'HISTORY'
  ) AS row_status
FROM ordered_prices;