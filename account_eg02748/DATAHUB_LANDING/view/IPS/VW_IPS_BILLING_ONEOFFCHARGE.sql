CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ONEOFFCHARGE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:CHARGEDBY::varchar AS CHARGEDBY, 
                        src:CHARGEDDTTM::datetime AS CHARGEDDTTM, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:FULLYPAIDSTAT::varchar AS FULLYPAIDSTAT, 
                        src:LINEITEMSETUPKEY::integer AS LINEITEMSETUPKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONEOFFCHARGEKEY::integer AS ONEOFFCHARGEKEY, 
                        src:PAYMENTKEY::integer AS PAYMENTKEY, 
                        src:PRINTTEXT::varchar AS PRINTTEXT, 
                        src:QUANTITY::numeric(38, 10) AS QUANTITY, 
                        src:SERVICEREQUESTNUMBER::integer AS SERVICEREQUESTNUMBER, 
                        src:SUBTOTAL::numeric(38, 10) AS SUBTOTAL, 
                        src:UNITOFMEASURE::varchar AS UNITOFMEASURE, 
                        src:VALUE::numeric(38, 10) AS VALUE, 
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
    
                        
                src:ONEOFFCHARGEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ONEOFFCHARGEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ONEOFFCHARGE as strm))