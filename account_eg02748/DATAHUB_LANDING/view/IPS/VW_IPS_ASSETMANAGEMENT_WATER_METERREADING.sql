CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_WATER_METERREADING AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:READINGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:READINGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_WATER_METERREADING as strm))