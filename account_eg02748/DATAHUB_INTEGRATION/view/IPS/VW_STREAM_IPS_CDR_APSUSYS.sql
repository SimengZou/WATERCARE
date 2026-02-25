CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_APSUSYS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTOCON::varchar AS AUTOCON, 
                        src:BLDGIRAUTOASSIGNFAILEDLOGTYPE::varchar AS BLDGIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:CASEIRAUTOASSIGNFAILEDLOGTYPE::varchar AS CASEIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:CASHBGTNOKEY::integer AS CASHBGTNOKEY, 
                        src:CDRPRODFAMILY::integer AS CDRPRODFAMILY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:LICIRAUTOASSIGNFAILEDLOGTYPE::varchar AS LICIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OVERBGTNOKEY::integer AS OVERBGTNOKEY, 
                        src:PLANIRAUTOASSIGNFAILEDLOGTYPE::varchar AS PLANIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:PROJIRAUTOASSIGNFAILEDLOGTYPE::varchar AS PROJIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:PROPERTYOPTION::integer AS PROPERTYOPTION, 
                        src:REVBGTNOKEY::integer AS REVBGTNOKEY, 
                        src:RNDBGTNOKEY::integer AS RNDBGTNOKEY, 
                        src:SHOWASSET::varchar AS SHOWASSET, 
                        src:STARTTAB::integer AS STARTTAB, 
                        src:TRADEIRAUTOASSIGNFAILEDLOGTYPE::varchar AS TRADEIRAUTOASSIGNFAILEDLOGTYPE, 
                        src:USEIRAUTOASSIGNFAILEDLOGTYPE::varchar AS USEIRAUTOASSIGNFAILEDLOGTYPE, 
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
                                        
                src:CDRPRODFAMILY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_APSUSYS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CASHBGTNOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CDRPRODFAMILY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OVERBGTNOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROPERTYOPTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVBGTNOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RNDBGTNOKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STARTTAB), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null