CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_PAYBATCH AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTOPOST::varchar AS AUTOPOST, 
                        src:BACKFLAG::varchar AS BACKFLAG, 
                        src:BATCFRDTTM::datetime AS BATCFRDTTM, 
                        src:BATCHAMT::numeric(38, 10) AS BATCHAMT, 
                        src:BATCHCT::integer AS BATCHCT, 
                        src:BATCHDESC::varchar AS BATCHDESC, 
                        src:BATCHKEY::integer AS BATCHKEY, 
                        src:BATCHNO::varchar AS BATCHNO, 
                        src:BATCHSTAT::varchar AS BATCHSTAT, 
                        src:BATCTODTTM::datetime AS BATCTODTTM, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:CRPENALTYFLAG::varchar AS CRPENALTYFLAG, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIRECTDEBITRUNKEY::integer AS DIRECTDEBITRUNKEY, 
                        src:DPFLAG::varchar AS DPFLAG, 
                        src:DRAWERBATCHGROUP::varchar AS DRAWERBATCHGROUP, 
                        src:FORTRANSFERPAYMENTFLAG::varchar AS FORTRANSFERPAYMENTFLAG, 
                        src:LASTINVOCATIONSTATUS::integer AS LASTINVOCATIONSTATUS, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYMENTIMPORTRUNFLAG::varchar AS PAYMENTIMPORTRUNFLAG, 
                        src:POSTBY::varchar AS POSTBY, 
                        src:POSTDTTM::datetime AS POSTDTTM, 
                        src:POSTFLAG::varchar AS POSTFLAG, 
                        src:RDYPSTFLAG::varchar AS RDYPSTFLAG, 
                        src:REVERSALREASONCODE::varchar AS REVERSALREASONCODE, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:SRC::varchar AS SRC, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOIDEDBY::varchar AS VOIDEDBY, 
                        src:VOIDEDDATE::datetime AS VOIDEDDATE, 
                        src:WAIVEFLAG::varchar AS WAIVEFLAG, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:BATCHKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_PAYBATCH as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BATCFRDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHCT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BATCTODTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIRECTDEBITRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LASTINVOCATIONSTATUS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:POSTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCHEDULEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VOIDEDDATE), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null