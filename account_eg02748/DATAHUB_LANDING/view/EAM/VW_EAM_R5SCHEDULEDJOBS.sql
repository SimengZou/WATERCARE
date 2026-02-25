CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5SCHEDULEDJOBS AS SELECT
                        src:SCJ_ACTIVE::varchar AS SCJ_ACTIVE, 
                        src:SCJ_BROKEN::varchar AS SCJ_BROKEN, 
                        src:SCJ_CODE::varchar AS SCJ_CODE, 
                        src:SCJ_JOBNAME::varchar AS SCJ_JOBNAME, 
                        src:SCJ_LASTRUN::datetime AS SCJ_LASTRUN, 
                        src:SCJ_LASTSAVED::datetime AS SCJ_LASTSAVED, 
                        src:SCJ_NEXTRUN::datetime AS SCJ_NEXTRUN, 
                        src:SCJ_SCHCODE::varchar AS SCJ_SCHCODE, 
                        src:SCJ_SERVERID::varchar AS SCJ_SERVERID, 
                        src:SCJ_TYPE::varchar AS SCJ_TYPE, 
                        src:SCJ_UPDATECOUNT::numeric(38, 10) AS SCJ_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:SCJ_CODE,
            src:SCJ_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:SCJ_CODE  ORDER BY 
            src:SCJ_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5SCHEDULEDJOBS as strm))