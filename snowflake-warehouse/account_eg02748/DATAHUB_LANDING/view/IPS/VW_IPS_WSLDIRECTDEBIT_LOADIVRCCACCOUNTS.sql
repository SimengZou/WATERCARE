CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:AUTHCODE::varchar AS AUTHCODE, 
                        src:AUTHORISED::varchar AS AUTHORISED, 
                        src:BILLINGID::varchar AS BILLINGID, 
                        src:CARDHOLDERNAME::varchar AS CARDHOLDERNAME, 
                        src:CARDNO::varchar AS CARDNO, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:CUR::varchar AS CUR, 
                        src:DELETED::boolean AS DELETED, 
                        src:DPSBILLINGID::varchar AS DPSBILLINGID, 
                        src:DPSTXNREF::varchar AS DPSTXNREF, 
                        src:FILEEXTENSION::varchar AS FILEEXTENSION, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:GROUPACCT::varchar AS GROUPACCT, 
                        src:LOADIVRCCACCOUNTSKEY::integer AS LOADIVRCCACCOUNTSKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:REFERENCE::varchar AS REFERENCE, 
                        src:RESPCODE::varchar AS RESPCODE, 
                        src:RESPTEXT::varchar AS RESPTEXT, 
                        src:SETTLEMENTDATE::datetime AS SETTLEMENTDATE, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSMESG::varchar AS STATUSMESG, 
                        src:TIME::datetime AS TIME, 
                        src:TXNDATA1::varchar AS TXNDATA1, 
                        src:TXNDATA2::varchar AS TXNDATA2, 
                        src:TXNDATA3::varchar AS TXNDATA3, 
                        src:TXNREF::varchar AS TXNREF, 
                        src:TYPE::varchar AS TYPE, 
                        src:USERNAME::varchar AS USERNAME, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOIDED::varchar AS VOIDED, 
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
    
                        
                src:LOADIVRCCACCOUNTSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LOADIVRCCACCOUNTSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLDIRECTDEBIT_LOADIVRCCACCOUNTS as strm))