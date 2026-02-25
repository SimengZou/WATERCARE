CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_ADDRESSSERVICE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSERVICEKEY::integer AS ADDRESSSERVICEKEY, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:BUILDERFLAG::varchar AS BUILDERFLAG, 
                        src:CONSUMPTIONPERCENTAGE::numeric(38, 10) AS CONSUMPTIONPERCENTAGE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPLAYORDER::integer AS DISPLAYORDER, 
                        src:FIXEDCHGFROMDATE::datetime AS FIXEDCHGFROMDATE, 
                        src:FIXEDCHGTODATE::datetime AS FIXEDCHGTODATE, 
                        src:IMPERVIOUSPERCENTAGE::numeric(38, 10) AS IMPERVIOUSPERCENTAGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MOVEININSTRUCTIONS::varchar AS MOVEININSTRUCTIONS, 
                        src:OWNERFLAG::varchar AS OWNERFLAG, 
                        src:REMOVEDDTTM::datetime AS REMOVEDDTTM, 
                        src:SERVICECLASS1::varchar AS SERVICECLASS1, 
                        src:SERVICECLASS2::varchar AS SERVICECLASS2, 
                        src:SERVICEOPTIONSKEY::integer AS SERVICEOPTIONSKEY, 
                        src:SRVFLD1VALUE::numeric(38, 10) AS SRVFLD1VALUE, 
                        src:SRVFLD2VALUE::numeric(38, 10) AS SRVFLD2VALUE, 
                        src:SRVFLD3VALUE::numeric(38, 10) AS SRVFLD3VALUE, 
                        src:TENANTFLAG::varchar AS TENANTFLAG, 
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
                                        
                src:ADDRESSSERVICEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_ADDRESSSERVICE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONSUMPTIONPERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FIXEDCHGFROMDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FIXEDCHGTODATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPERVIOUSPERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REMOVEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SRVFLD1VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SRVFLD2VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SRVFLD3VALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null