CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCTCNTC AS SELECT
                        src:ACCOUNTCONTACTKEY::integer AS ACCOUNTCONTACTKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACTIVEFLAG::varchar AS ACTIVEFLAG, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:CAPFRDTTM::datetime AS CAPFRDTTM, 
                        src:CAPTODTTM::datetime AS CAPTODTTM, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENTEFFDATE::datetime AS ENTEFFDATE, 
                        src:ENTEXPDATE::datetime AS ENTEXPDATE, 
                        src:ENTORD::integer AS ENTORD, 
                        src:ENTPCT::integer AS ENTPCT, 
                        src:ENTVAL1::numeric(38, 10) AS ENTVAL1, 
                        src:ENTVAL2::numeric(38, 10) AS ENTVAL2, 
                        src:ENTVAL3::numeric(38, 10) AS ENTVAL3, 
                        src:ENTVAL4::numeric(38, 10) AS ENTVAL4, 
                        src:ENTVAL5::numeric(38, 10) AS ENTVAL5, 
                        src:ENTVAL6::numeric(38, 10) AS ENTVAL6, 
                        src:ENTVAL7::numeric(38, 10) AS ENTVAL7, 
                        src:ENTVAL8::numeric(38, 10) AS ENTVAL8, 
                        src:IDKEY::integer AS IDKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
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
    
                        
                src:ACCOUNTCONTACTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTCONTACTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCTCNTC as strm))