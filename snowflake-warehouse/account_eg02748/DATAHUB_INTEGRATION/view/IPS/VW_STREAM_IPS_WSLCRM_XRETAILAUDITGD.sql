CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCRM_XRETAILAUDITGD AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSETKEY::integer AS ASSETKEY, 
                        src:BLUSAGEDIFF::varchar AS BLUSAGEDIFF, 
                        src:DELETED::boolean AS DELETED, 
                        src:FIELDNAME::varchar AS FIELDNAME, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODIFIEDBYEMPLOYEE::varchar AS MODIFIEDBYEMPLOYEE, 
                        src:MODIFIEDDATE::datetime AS MODIFIEDDATE, 
                        src:NEWBLUSAGE::varchar AS NEWBLUSAGE, 
                        src:NEWVALUE::varchar AS NEWVALUE, 
                        src:PREVIOUSBLUSAGE::varchar AS PREVIOUSBLUSAGE, 
                        src:PREVIOUSVALUE::varchar AS PREVIOUSVALUE, 
                        src:READINGDATE::datetime AS READINGDATE, 
                        src:READINGKEY::integer AS READINGKEY, 
                        src:SOURCEID::varchar AS SOURCEID, 
                        src:VALDIFF::varchar AS VALDIFF, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XRETAILAUDITDPKEY::integer AS XRETAILAUDITDPKEY, 
                        src:XRETAILAUDITGDKEY::integer AS XRETAILAUDITGDKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XRETAILAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XRETAILAUDITGD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODIFIEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:READINGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XRETAILAUDITDPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XRETAILAUDITGDKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null