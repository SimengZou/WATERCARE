CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEAPPLTYPE AS SELECT
                        src:ACCID::integer AS ACCID, 
                        src:ACCOUNTTYPECONFIGURATION::integer AS ACCOUNTTYPECONFIGURATION, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APAPPLTYPECATKEY::integer AS APAPPLTYPECATKEY, 
                        src:APDESC::varchar AS APDESC, 
                        src:APNOPREFIX::varchar AS APNOPREFIX, 
                        src:APTYPE::varchar AS APTYPE, 
                        src:APUSEDEFNKEY::integer AS APUSEDEFNKEY, 
                        src:AUTOFILL::varchar AS AUTOFILL, 
                        src:BILLINGACCOUNTTYPE::integer AS BILLINGACCOUNTTYPE, 
                        src:BILLTYPEKEY::integer AS BILLTYPEKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:DPLEVEL::integer AS DPLEVEL, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:EXPDATEFRMLA::integer AS EXPDATEFRMLA, 
                        src:GISICON::varchar AS GISICON, 
                        src:HASSUPROVR::varchar AS HASSUPROVR, 
                        src:INTERNALONLYFLAG::varchar AS INTERNALONLYFLAG, 
                        src:ISPUBLIC::varchar AS ISPUBLIC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:PRIMARYSITETYPE::integer AS PRIMARYSITETYPE, 
                        src:RESPACCOUNTHOLDER::integer AS RESPACCOUNTHOLDER, 
                        src:SHOWINPORTAL::varchar AS SHOWINPORTAL, 
                        src:SUBMITINPORTAL::varchar AS SUBMITINPORTAL, 
                        src:USEEST::varchar AS USEEST, 
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
                                        
                src:APUSEDEFNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_USE_USEAPPLTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTTYPECONFIGURATION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APAPPLTYPECATKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEDEFNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLINGACCOUNTTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DPLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPDATEFRMLA), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRIMARYSITETYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESPACCOUNTHOLDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null