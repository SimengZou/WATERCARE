CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ADJUSTMENT AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJTYPESUKEY::integer AS ADJTYPESUKEY, 
                        src:ADJUSTEDBY::varchar AS ADJUSTEDBY, 
                        src:ADJUSTMENTDTTM::datetime AS ADJUSTMENTDTTM, 
                        src:ADJUSTMENTKEY::integer AS ADJUSTMENTKEY, 
                        src:ADJUSTMENTREASON::varchar AS ADJUSTMENTREASON, 
                        src:ADJUSTMENTSCOUNT::integer AS ADJUSTMENTSCOUNT, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:APPROVALAMOUNT::numeric(38, 10) AS APPROVALAMOUNT, 
                        src:APPROVALLEVEL::integer AS APPROVALLEVEL, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:FULLYPAIDSTAT::varchar AS FULLYPAIDSTAT, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PARENTADJUSTMENTKEY::integer AS PARENTADJUSTMENTKEY, 
                        src:PENALTYKEY::integer AS PENALTYKEY, 
                        src:PRIORITY::varchar AS PRIORITY, 
                        src:PROPERTYBASEDFLAG::varchar AS PROPERTYBASEDFLAG, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSBY::varchar AS STATUSBY, 
                        src:STATUSDATE::datetime AS STATUSDATE, 
                        src:TRANSACTIONDESIGNATOR::integer AS TRANSACTIONDESIGNATOR, 
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
    
                        
                src:ADJUSTMENTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ADJUSTMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ADJUSTMENT as strm))