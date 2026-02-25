CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS AS SELECT
                        src:ACCOUNTIDENTIFIER::varchar AS ACCOUNTIDENTIFIER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMOUNT::numeric(38, 10) AS AMOUNT, 
                        src:AUTOFLAG::varchar AS AUTOFLAG, 
                        src:CASHERINGBASEDFLAG::varchar AS CASHERINGBASEDFLAG, 
                        src:CHECKNUMBER::varchar AS CHECKNUMBER, 
                        src:DATAROW::varchar AS DATAROW, 
                        src:DELETED::boolean AS DELETED, 
                        src:EMPLOYEE::varchar AS EMPLOYEE, 
                        src:EXCEPTIONDESC::varchar AS EXCEPTIONDESC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTES::varchar AS NOTES, 
                        src:NSFCREATEDFLAG::varchar AS NSFCREATEDFLAG, 
                        src:NSFIMPORTACTIVITYDTLKEY::integer AS NSFIMPORTACTIVITYDTLKEY, 
                        src:NSFIMPORTACTIVITYKEY::integer AS NSFIMPORTACTIVITYKEY, 
                        src:NSFPROCESSEXCEPTION::integer AS NSFPROCESSEXCEPTION, 
                        src:PAYMENTBATCHKEY::integer AS PAYMENTBATCHKEY, 
                        src:PAYMENTIDENTIFIER::integer AS PAYMENTIDENTIFIER, 
                        src:RERUNFLAG::varchar AS RERUNFLAG, 
                        src:RESOLVEDFLAG::varchar AS RESOLVEDFLAG, 
                        src:RETURNREASONCODE::varchar AS RETURNREASONCODE, 
                        src:REVERSEDFLAG::varchar AS REVERSEDFLAG, 
                        src:REVIEWDATE::datetime AS REVIEWDATE, 
                        src:REVIEWEDFLAG::varchar AS REVIEWEDFLAG, 
                        src:TRANSACTIONDATE::datetime AS TRANSACTIONDATE, 
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
                                        
                src:NSFIMPORTACTIVITYDTLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_NSFIMPORTACTIVITYDETAILS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NSFIMPORTACTIVITYDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NSFIMPORTACTIVITYKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NSFPROCESSEXCEPTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTBATCHKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTIDENTIFIER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REVIEWDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRANSACTIONDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null