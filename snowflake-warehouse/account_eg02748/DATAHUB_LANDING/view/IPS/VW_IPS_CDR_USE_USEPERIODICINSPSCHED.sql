CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_USE_USEPERIODICINSPSCHED AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APUSEINSPKEY::integer AS APUSEINSPKEY, 
                        src:APUSEINSPTYPEKEY::integer AS APUSEINSPTYPEKEY, 
                        src:APUSEKEY::integer AS APUSEKEY, 
                        src:APUSEPERDINSPKEY::integer AS APUSEPERDINSPKEY, 
                        src:APUSEPERDINSPSUKEY::integer AS APUSEPERDINSPSUKEY, 
                        src:ASSIGNTO::varchar AS ASSIGNTO, 
                        src:AUTOASSIGNED::varchar AS AUTOASSIGNED, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:HOLD::varchar AS HOLD, 
                        src:LASTSCHEDULED::datetime AS LASTSCHEDULED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTSCHEDULED::datetime AS NEXTSCHEDULED, 
                        src:TIMESCHEDKEY::integer AS TIMESCHEDKEY, 
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
    
                        
                src:APUSEPERDINSPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APUSEPERDINSPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_USE_USEPERIODICINSPSCHED as strm))