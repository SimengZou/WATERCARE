-- datahub_analytics.dw_ips_date_calendar definition

-- Drop table


DROP TABLE IF EXISTS datahub_analytics.dw_ips_dim_date_calendar;

CREATE TABLE IF NOT EXISTS datahub_analytics.dw_ips_dim_date_calendar
(
	dim_ips_date_calendar_key INTEGER IDENTITY NOT NULL
	,dim_date_key INTEGER NOT NULL  
	,calendar_date TIMESTAMP
	,cal_day_in_week VARCHAR(3)   
	,cal_day_in_week_no INTEGER   
	,cal_day_in_month INTEGER   
	,cal_day_in_year INTEGER   
	,cal_week_in_year INTEGER   
	,cal_month_no INTEGER   
	,cal_month INTEGER   
	,cal_month_name VARCHAR(7)   
	,cal_quarter_no INTEGER   
	,cal_quarter INTEGER   
	,cal_year INTEGER   
	,fin_day_in_year INTEGER   
	,fin_week_in_year INTEGER   
	,fin_month_no INTEGER   
	,fin_month INTEGER   
	,fin_period VARCHAR(20)   
	,fin_quarter_no INTEGER   
	,fin_quarter INTEGER   
	,fin_year INTEGER   
	,current_cal_day VARCHAR(8)   
	,current_cal_week VARCHAR(8)   
	,current_cal_month VARCHAR(8)   
	,current_cal_year VARCHAR(8)   
	,current_cal_mtd VARCHAR(8)   
	,current_cal_ytd VARCHAR(8)   
	,moving_cal_year VARCHAR(8)   
	,current_fin_day VARCHAR(8)   
	,current_fin_week VARCHAR(8)   
	,current_fin_month VARCHAR(8)   
	,current_fin_quarter VARCHAR(8)   
	,current_fin_year VARCHAR(8)   
	,current_fin_mtd VARCHAR(8)   
	,current_fin_ytd VARCHAR(8)   
	,moving_fin_quarter VARCHAR(8)   
	,moving_fin_year VARCHAR(8)   
	,week_day_flag VARCHAR(1)   
	,week_end_flag VARCHAR(1)   
	,holiday_flag VARCHAR(1)   
	,holiday_desc VARCHAR(64)   
	,trading_day_flag VARCHAR(1)   
	,trading_days_in_mth INTEGER   
	,trading_days_so_far INTEGER   
	,dim_month_key INTEGER   
	,cal_month_day1 TIMESTAMP  
	,cal_month_year VARCHAR(8)   
	,summer_winter VARCHAR(8)   
	,cal_year_char VARCHAR(4)   
	,season VARCHAR(8)   
	,rolling_month_no INTEGER   
	,last_day_of_month_flag VARCHAR(1)   
	,rolling_cal_month VARCHAR(8)   
	,rolling_cal_quarter VARCHAR(8)   
	,rolling_cal_year VARCHAR(8)   
	,days_in_month INTEGER   
	,days_ago INTEGER   
	,etl_load_datetime TIMESTAMP
)
;
-- ALTER TABLE datahub_analytics.dw_ips_date_calendar owner to wslmaster;