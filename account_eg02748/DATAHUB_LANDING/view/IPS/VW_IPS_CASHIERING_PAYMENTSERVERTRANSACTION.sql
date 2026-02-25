CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_PAYMENTSERVERTRANSACTION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTHCODE::varchar AS AUTHCODE, 
                        src:CARDSWIPEFLAG::varchar AS CARDSWIPEFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:MERCHANTACCOUNTKEY::integer AS MERCHANTACCOUNTKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYSERVERTRANKEY::integer AS PAYSERVERTRANKEY, 
                        src:RESPONSECODE::integer AS RESPONSECODE, 
                        src:RESPONSEMESSAGE::varchar AS RESPONSEMESSAGE, 
                        src:TRANMODE::varchar AS TRANMODE, 
                        src:TRANSACTIONAMOUNT::numeric(38, 10) AS TRANSACTIONAMOUNT, 
                        src:TRANSACTIONDATETIME::datetime AS TRANSACTIONDATETIME, 
                        src:TRANSACTIONREFERENCENUMBER::varchar AS TRANSACTIONREFERENCENUMBER, 
                        src:TRANSACTIONSTATUS::varchar AS TRANSACTIONSTATUS, 
                        src:TRANSACTIONTYPE::varchar AS TRANSACTIONTYPE, 
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
    
                        
                src:PAYSERVERTRANKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PAYSERVERTRANKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_PAYMENTSERVERTRANSACTION as strm))