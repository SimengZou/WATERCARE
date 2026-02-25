CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_LINEITEMRATE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSERVICEKEY::integer AS ADDRESSSERVICEKEY, 
                        src:BILLRUNKEY::integer AS BILLRUNKEY, 
                        src:CALCULATIONDETAILKEY::integer AS CALCULATIONDETAILKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPOSITCHARGEKEY::integer AS DEPOSITCHARGEKEY, 
                        src:FIXEDCHGCHARGEDAYS::integer AS FIXEDCHGCHARGEDAYS, 
                        src:FIXEDCHGFROMDATE::datetime AS FIXEDCHGFROMDATE, 
                        src:FIXEDCHGNUMBERPERIODS::integer AS FIXEDCHGNUMBERPERIODS, 
                        src:FIXEDCHGPERIODDAYS::integer AS FIXEDCHGPERIODDAYS, 
                        src:FIXEDCHGPERIODSINYEAR::integer AS FIXEDCHGPERIODSINYEAR, 
                        src:FIXEDCHGRATEPERPERIOD::numeric(38, 10) AS FIXEDCHGRATEPERPERIOD, 
                        src:FIXEDCHGTODATE::datetime AS FIXEDCHGTODATE, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:LINEITEMRATEKEY::integer AS LINEITEMRATEKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONEOFFCHARGEKEY::integer AS ONEOFFCHARGEKEY, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:LINEITEMRATEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LINEITEMRATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_LINEITEMRATE as strm))