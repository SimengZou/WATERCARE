CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5ALERTWO AS SELECT
                        src:ALW_ALERT::varchar AS ALW_ALERT, 
                        src:ALW_DELAY::numeric(38, 10) AS ALW_DELAY, 
                        src:ALW_DELAYUOM::varchar AS ALW_DELAYUOM, 
                        src:ALW_DUENONCONFORMITIESONLY::varchar AS ALW_DUENONCONFORMITIESONLY, 
                        src:ALW_DURATIONFIELDID::numeric(38, 10) AS ALW_DURATIONFIELDID, 
                        src:ALW_INCLUDENONCONFORMITIES::varchar AS ALW_INCLUDENONCONFORMITIES, 
                        src:ALW_JOBTYPEFIELDID::numeric(38, 10) AS ALW_JOBTYPEFIELDID, 
                        src:ALW_LASTSAVED::datetime AS ALW_LASTSAVED, 
                        src:ALW_OBJECTFIELDID::numeric(38, 10) AS ALW_OBJECTFIELDID, 
                        src:ALW_OBJECTORGFIELDID::numeric(38, 10) AS ALW_OBJECTORGFIELDID, 
                        src:ALW_PRIORITYFIELDID::numeric(38, 10) AS ALW_PRIORITYFIELDID, 
                        src:ALW_PROBLEMCODEFIELDID::numeric(38, 10) AS ALW_PROBLEMCODEFIELDID, 
                        src:ALW_REQUESTENDFIELDID::numeric(38, 10) AS ALW_REQUESTENDFIELDID, 
                        src:ALW_REQUESTSTARTFIELDID::numeric(38, 10) AS ALW_REQUESTSTARTFIELDID, 
                        src:ALW_SCHEDSTARTFIELDID::numeric(38, 10) AS ALW_SCHEDSTARTFIELDID, 
                        src:ALW_STANDWORK::varchar AS ALW_STANDWORK, 
                        src:ALW_UPDATECOUNT::numeric(38, 10) AS ALW_UPDATECOUNT, 
                        src:ALW_WOCOMMENT::varchar AS ALW_WOCOMMENT, 
                        src:ALW_WODESC::varchar AS ALW_WODESC, 
                        src:ALW_WORKORDERORG::varchar AS ALW_WORKORDERORG, 
                        src:ALW_WORKORDERORGFIELDID::numeric(38, 10) AS ALW_WORKORDERORGFIELDID, 
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
    
                        
                src:ALW_ALERT,
            src:ALW_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ALW_ALERT  ORDER BY 
            src:ALW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5ALERTWO as strm))