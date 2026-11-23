CREATE TABLE IF NOT EXISTS `gold_market_dw.daily_gold_prices` (
  record_date DATE NOT NULL OPTIONS(description="วันที่ของข้อมูลตามเวลาประเทศไทย (YYYY-MM-DD)"),
  session STRING NOT NULL OPTIONS(description="รอบการดึงข้อมูล: MORNING หรือ AFTERNOON"),
  xau_usd_oz FLOAT64 NOT NULL OPTIONS(description="ราคาทองคำสากล XAU/USD ต่อ 1 Troy Ounce"),
  usd_thb_rate FLOAT64 NOT NULL OPTIONS(description="อัตราแลกเปลี่ยน USD/THB"),
  global_calc_965_thb FLOAT64 NOT NULL OPTIONS(description="ราคาประเมินทองคำไทย 96.5% จากตลาดโลก (ปัดเศษ 50 บาท)"),
  thai_bar_buy_thb FLOAT64 OPTIONS(description="ราคารับซื้อทองคำแท่งสมาคมฯ (Nullable หาก API ไทยล่ม)"),
  thai_bar_sell_thb FLOAT64 OPTIONS(description="ราคาขายออกทองคำแท่งสมาคมฯ (Nullable หาก API ไทยล่ม)"),
  thai_ornament_buy_thb FLOAT64 OPTIONS(description="ราคารับซื้อทองรูปพรรณสมาคมฯ (Nullable)"),
  thai_ornament_sell_thb FLOAT64 OPTIONS(description="ราคาขายออกทองรูปพรรณสมาคมฯ (Nullable)"),
  thai_market_premium_thb FLOAT64 OPTIONS(description="ส่วนต่างราคาขายออกสมาคมฯ เทียบกับราคาคำนวณตลาดโลก"),
  thai_api_status STRING NOT NULL OPTIONS(description="สถานะ API สมาคมฯ: SUCCESS หรือ FALLBACK_NULL"),
  ingested_at TIMESTAMP NOT NULL OPTIONS(description="เวลาที่ Pipeline โหลดข้อมูลเข้าสู่ระบบ (UTC)")
)
PARTITION BY record_date
CLUSTER BY session
OPTIONS(
  description="ตารางเก็บข้อมูลราคาทองคำไทยและตลาดโลกรายรอบ (เช้า-เย็น) รองรับ Composite-Key Idempotency"
);