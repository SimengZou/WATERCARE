CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PROJECT_PROJLOG AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDBYPORTALUSR::varchar AS ADDBYPORTALUSR, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJINSPKEY::integer AS APPROJINSPKEY, 
                        src:APPROJKEY::integer AS APPROJKEY, 
                        src:APPROJLOGKEY::integer AS APPROJLOGKEY, 
                        src:APPROJREVIEWKEY::integer AS APPROJREVIEWKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:LOGTYPE::varchar AS LOGTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODBYPORTALUSR::varchar AS MODBYPORTALUSR, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:STARTBY::varchar AS STARTBY, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STOPBY::varchar AS STOPBY, 
                        src:STOPDTTM::datetime AS STOPDTTM, 
                        src:TOTALTIME::numeric(38, 10) AS TOTALTIME, 
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
    
                        
                src:APPROJLOGKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APPROJLOGKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PROJECT_PROJLOG as strm))