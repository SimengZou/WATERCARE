CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_PAYSPECIFICBILL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJUSTMENTKEY::integer AS ADJUSTMENTKEY, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:CDRAPPLICATIONFEEKEY::integer AS CDRAPPLICATIONFEEKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEPOSITADJUSTMENTKEY::integer AS DEPOSITADJUSTMENTKEY, 
                        src:DEPOSITCHARGEKEY::integer AS DEPOSITCHARGEKEY, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONEOFFCHARGEKEY::integer AS ONEOFFCHARGEKEY, 
                        src:ORIGPAYALLOCAMT::numeric(38, 10) AS ORIGPAYALLOCAMT, 
                        src:PAYMENTKEY::integer AS PAYMENTKEY, 
                        src:PAYORDER::integer AS PAYORDER, 
                        src:PAYSPECIFICBILLKEY::integer AS PAYSPECIFICBILLKEY, 
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
    
                        
                src:PAYSPECIFICBILLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PAYSPECIFICBILLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_PAYSPECIFICBILL as strm))