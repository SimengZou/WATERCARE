CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLBILLING_LOADPAYMENT AS SELECT
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:BATCHKEY::integer AS BATCHKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:FILEEXTENSION::varchar AS FILEEXTENSION, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:LOADPAYMENTKEY::integer AS LOADPAYMENTKEY, 
                        src:METHOD::varchar AS METHOD, 
                        src:MISCREFNO::varchar AS MISCREFNO, 
                        src:MISCTYPE::varchar AS MISCTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OTHER1::varchar AS OTHER1, 
                        src:OTHER2::varchar AS OTHER2, 
                        src:OTHER3::varchar AS OTHER3, 
                        src:PAYMENTDATE::varchar AS PAYMENTDATE, 
                        src:RECORDCOUNT::integer AS RECORDCOUNT, 
                        src:RECORDTYPE::varchar AS RECORDTYPE, 
                        src:SOURCETYPE::varchar AS SOURCETYPE, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSMESG::varchar AS STATUSMESG, 
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
    
                        
                src:LOADPAYMENTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LOADPAYMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLBILLING_LOADPAYMENT as strm))