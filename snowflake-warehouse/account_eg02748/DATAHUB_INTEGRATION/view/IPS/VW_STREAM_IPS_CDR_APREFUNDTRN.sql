CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_APREFUNDTRN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AMTCONF::numeric(38, 10) AS AMTCONF, 
                        src:APKEY::integer AS APKEY, 
                        src:APRFNDTRNNO::integer AS APRFNDTRNNO, 
                        src:CARDAUTH::varchar AS CARDAUTH, 
                        src:CARDBANK::varchar AS CARDBANK, 
                        src:CARDEXPDT::datetime AS CARDEXPDT, 
                        src:CARDNAME::varchar AS CARDNAME, 
                        src:CARDNO::varchar AS CARDNO, 
                        src:CARDTYPE::varchar AS CARDTYPE, 
                        src:CHECKBANK::varchar AS CHECKBANK, 
                        src:CHECKNO::varchar AS CHECKNO, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:CONFDTTM::datetime AS CONFDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESCROWACCTKEY::integer AS ESCROWACCTKEY, 
                        src:ESCROWNO::varchar AS ESCROWNO, 
                        src:ESCROWTRNNO::integer AS ESCROWTRNNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYMENTMETHOD::integer AS PAYMENTMETHOD, 
                        src:PAYMETHOD::varchar AS PAYMETHOD, 
                        src:REFUNDAMT::numeric(38, 10) AS REFUNDAMT, 
                        src:REGTRANNO::integer AS REGTRANNO, 
                        src:SRCBGTNOKEY::integer AS SRCBGTNOKEY, 
                        src:STAT::varchar AS STAT, 
                        src:TRNDTTM::datetime AS TRNDTTM, 
                        src:TRNEMP::varchar AS TRNEMP, 
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
                                        
                src:APRFNDTRNNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APREFUNDTRN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AMTCONF), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APRFNDTRNNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CARDEXPDT), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CONFDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWACCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWTRNNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTMETHOD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REFUNDAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REGTRANNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SRCBGTNOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TRNDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null