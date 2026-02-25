CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_PAYMENTSERVERTRANSACTION AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PAYSERVERTRANKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_PAYMENTSERVERTRANSACTION as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MERCHANTACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYSERVERTRANKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESPONSECODE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TRANSACTIONAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRANSACTIONDATETIME), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null