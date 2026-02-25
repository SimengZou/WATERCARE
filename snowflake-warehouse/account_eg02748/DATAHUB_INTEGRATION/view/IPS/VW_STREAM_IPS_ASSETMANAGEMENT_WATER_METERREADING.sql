CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUDITFLAG::varchar AS AUDITFLAG, 
                        src:BLUSAGE::numeric(38, 10) AS BLUSAGE, 
                        src:BLUSAGECUBICFT::numeric(38, 10) AS BLUSAGECUBICFT, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CORRFLAG::varchar AS CORRFLAG, 
                        src:CUSTPROB::integer AS CUSTPROB, 
                        src:CYCLE::varchar AS CYCLE, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESTMFLAG::varchar AS ESTMFLAG, 
                        src:FIELDNOTES::varchar AS FIELDNOTES, 
                        src:FINALFLAG::varchar AS FINALFLAG, 
                        src:GRPPROJ::integer AS GRPPROJ, 
                        src:HIGHBLUSAGE::numeric(38, 10) AS HIGHBLUSAGE, 
                        src:HIGHBLUSAGEINCUBICFEET::numeric(38, 10) AS HIGHBLUSAGEINCUBICFEET, 
                        src:HIGHREADING::numeric(38, 10) AS HIGHREADING, 
                        src:HIGHUSAGE::numeric(38, 10) AS HIGHUSAGE, 
                        src:HISTKEY::integer AS HISTKEY, 
                        src:INITFLAG::varchar AS INITFLAG, 
                        src:INSPKEY::integer AS INSPKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOREADFLAG::varchar AS NOREADFLAG, 
                        src:PROBBCODEWO::integer AS PROBBCODEWO, 
                        src:PROBCODESRVREQNO::integer AS PROBCODESRVREQNO, 
                        src:PROBLEMCODE::varchar AS PROBLEMCODE, 
                        src:RDRCODESRVREQNO::integer AS RDRCODESRVREQNO, 
                        src:READBY::varchar AS READBY, 
                        src:READDTTM::datetime AS READDTTM, 
                        src:READERCODE::varchar AS READERCODE, 
                        src:READERCODEWO::integer AS READERCODEWO, 
                        src:READING::numeric(38, 10) AS READING, 
                        src:READINGKEY::integer AS READINGKEY, 
                        src:READREAS::varchar AS READREAS, 
                        src:READSRC::varchar AS READSRC, 
                        src:READVAL1::varchar AS READVAL1, 
                        src:READVAL2::varchar AS READVAL2, 
                        src:READVAL3::varchar AS READVAL3, 
                        src:READVAL4::varchar AS READVAL4, 
                        src:READYFLAG::varchar AS READYFLAG, 
                        src:USAGE::numeric(38, 10) AS USAGE, 
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
                                        
                src:READINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLUSAGECUBICFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CUSTPROB), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRPPROJ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HIGHBLUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HIGHBLUSAGEINCUBICFEET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HIGHREADING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HIGHUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROBBCODEWO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROBCODESRVREQNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RDRCODESRVREQNO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:READDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READERCODEWO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null