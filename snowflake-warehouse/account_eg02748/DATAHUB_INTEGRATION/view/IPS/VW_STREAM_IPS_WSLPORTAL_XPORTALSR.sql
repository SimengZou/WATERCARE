CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLPORTAL_XPORTALSR AS SELECT
                        src:ACCTNO::varchar AS ACCTNO, 
                        src:ACCTNOTADD::varchar AS ACCTNOTADD, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDR1::varchar AS ADDR1, 
                        src:ADDR2::varchar AS ADDR2, 
                        src:CITY::varchar AS CITY, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:DACCTNAME::varchar AS DACCTNAME, 
                        src:DATEFIXD::datetime AS DATEFIXD, 
                        src:DAYPHN::varchar AS DAYPHN, 
                        src:DDACCTNO::varchar AS DDACCTNO, 
                        src:DDACTYPE::varchar AS DDACTYPE, 
                        src:DDBANK::varchar AS DDBANK, 
                        src:DDBRANCH::varchar AS DDBRANCH, 
                        src:DDMASKNO::varchar AS DDMASKNO, 
                        src:DDMAXAMT::numeric(38, 10) AS DDMAXAMT, 
                        src:DELETED::boolean AS DELETED, 
                        src:EMAIL::varchar AS EMAIL, 
                        src:EVEPHN::varchar AS EVEPHN, 
                        src:FEDBKTYE::varchar AS FEDBKTYE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOOCC::integer AS NOOCC, 
                        src:POSTCODE::varchar AS POSTCODE, 
                        src:QUESTTOPC::varchar AS QUESTTOPC, 
                        src:SDLDATE::datetime AS SDLDATE, 
                        src:SERVNO::integer AS SERVNO, 
                        src:STATE::varchar AS STATE, 
                        src:USERID::varchar AS USERID, 
                        src:VALDATE::datetime AS VALDATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VERIFIED::varchar AS VERIFIED, 
                        src:XPORTALSRKEY::integer AS XPORTALSRKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XPORTALSRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLPORTAL_XPORTALSR as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DATEFIXD), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DDMAXAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOOCC), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SDLDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VALDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XPORTALSRKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null