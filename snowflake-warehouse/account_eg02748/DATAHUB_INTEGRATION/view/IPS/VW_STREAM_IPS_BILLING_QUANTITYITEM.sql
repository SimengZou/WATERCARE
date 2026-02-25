CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_QUANTITYITEM AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:DEFAULTFLAG::varchar AS DEFAULTFLAG, 
                        src:DEFAULTQTY::integer AS DEFAULTQTY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:DISPLAYORDER::integer AS DISPLAYORDER, 
                        src:INCRLINKTEXT::varchar AS INCRLINKTEXT, 
                        src:MAXQTY::integer AS MAXQTY, 
                        src:MINQTY::integer AS MINQTY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMERICFIELDTEXT::varchar AS NUMERICFIELDTEXT, 
                        src:POSTINCRLINKTEXT::varchar AS POSTINCRLINKTEXT, 
                        src:QUANTITYFIELDKEY::integer AS QUANTITYFIELDKEY, 
                        src:QUANTITYITEMKEY::integer AS QUANTITYITEMKEY, 
                        src:RADIOBUTTONTEXT::varchar AS RADIOBUTTONTEXT, 
                        src:TITLE::varchar AS TITLE, 
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
                                        
                src:QUANTITYITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_QUANTITYITEM as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEFAULTQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINQTY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUANTITYFIELDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:QUANTITYITEMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null