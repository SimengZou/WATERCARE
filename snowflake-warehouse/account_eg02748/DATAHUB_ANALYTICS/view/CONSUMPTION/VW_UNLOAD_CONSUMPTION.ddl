CREATE OR REPLACE VIEW datahub_analytics.vw_unload_consumption AS
SELECT distinct
  dt.cal_year AS cal_year,
  dt.cal_month_no AS cal_month_no,
  ad.suburb AS suburb,
  w.comp_key AS comp_key,
  trim(w.unit_id) AS unit_id,
  a.Account_Number,
  CASE WHEN a.account_class is null then null
    when a.account_class in ('Domestic','Dialysis') THEN 'Domestic'
    Else 'Non-domestic' END as account_type,
  a.customer_description,
  CASE WHEN a.account_class='Accmmdtn' then 'ACCOMMODATION'
	WHEN a.account_class='Agricltre' then 'AGRICULTURE & HORTICULTURE'
	WHEN a.account_class='BevrgeMan' then 'BEVERAGE MANUFACTURER'
	WHEN a.account_class='Commercial' then 'COMMERCIAL'
	WHEN a.account_class='ChrchComy' then 'COMMUNITY FACILITY/CHURCH'
	WHEN a.account_class='CncreteMan' then 'CONCRETE MANUFACTURING'
	WHEN a.account_class='Dialysis' then 'DIALYSIS PATIENTS'
	WHEN a.account_class='Domestic' then 'DOMESTIC'
	WHEN a.account_class='TrainFac' then 'EDUCATION TRAINING NO FIELD'
	WHEN a.account_class='FoodMan' then 'FOOD MANUFACTURING'
	WHEN a.account_class='Medical' then 'HOSPITAL / MEDICAL CLINIC'
	WHEN a.account_class='Industry' then 'INDUSTRY'
	WHEN a.account_class='Laundry' then 'LAUNDRY'
	WHEN a.account_class='MatrlMan' then 'MATERIAL MANUFACTURER'
	WHEN a.account_class='Abattoir' then 'ABATTOIR'
	WHEN a.account_class='Pool' then 'POOL'
	WHEN a.account_class='Retail' then 'RETAIL SHOP/CAFE/RESTAURANT'
	WHEN a.account_class='School' then 'SCHOOL WITH PLAYING FIELDS'
	WHEN a.account_class='SportsRec' then 'SPORTS/RECREATION FACILITY'
	WHEN a.account_class='TradeWaste' then 'TRADE WASTE'
	WHEN a.account_class='Warehouse' then 'WAREHOUSE/POSTAL/TRANSPORT'
	else a.account_class END AS account_class_desc,
  CASE WHEN a.account_class is null then null
  	   when a.account_class = 'Domestic' then 'Residential'
  	   else 'Non Residential' end account_class_code,
  CASE WHEN ad.subdivision_code = 'FRANK' THEN 'FRANKLIN'
      WHEN ad.subdivision_code = 'METRO' THEN 'METRO WATER'
      WHEN ad.subdivision_code = 'MWL' THEN 'MANUKAU WATER'
      WHEN ad.subdivision_code = 'NSHORE' THEN 'NORTH SHORE'
      WHEN ad.subdivision_code = 'WAITAK' THEN 'WAITAKERE'
      WHEN ad.subdivision_code = 'PAPKRA' THEN 'PAPAKURA'
      ELSE ad.subdivision_code
      END lno,
  w.address_key,
  
  LEFT(CASE Rtrim(Ltrim(ad.flat)) WHEN '' THEN '' ELSE Rtrim(Ltrim(ad.flat))||'/' END
       || Rtrim(Ltrim(ad.house_number || ', ' || ad.street_name || ', ' || Ltrim(ad.street_type || ', ') || suburb))
       || ', ' || ad.post_code, 100) as full_address,
  act_daily_avg,
  est_daily_avg,
  actual_days,
  actual_volume as act_monthly_avg,
  est_Volume as est_monthly_avg,
  coalesce(Estimate_Flag,'N') as estimate_flag,
  full_month_flag,
  'Datahub Snowflake' as etl_source,
   getdate() as etl_load_datetime
FROM
  datahub_analytics.dw_ips_fact_consumption t
  INNER JOIN datahub_analytics.dw_ips_dim_account a ON a.Account_key=t.Account_key
  INNER JOIN datahub_analytics.dw_ips_dim_compwmtr w ON w.comp_key=t.comp_key
  INNER JOIN datahub_analytics.dw_ips_dim_address ad ON ad.address_key=t.address_key
  INNER JOIN datahub_analytics.dw_ips_dim_date_calendar dt ON dt.cal_month=t.month
WHERE dt.calendar_date BETWEEN  LAST_day(add_months(sysdate(), -7),'month')+1 
   AND LAST_day(add_months(sysdate(), -1),'month');
  