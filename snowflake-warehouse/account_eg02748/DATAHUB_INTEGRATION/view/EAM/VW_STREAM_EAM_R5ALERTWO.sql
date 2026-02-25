CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTWO AS SELECT
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
                        src:ALW_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
                                        
                src:ALW_ALERT  ORDER BY 
            src:ALW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTWO as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_DELAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_DURATIONFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_JOBTYPEFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALW_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_OBJECTFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_OBJECTORGFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_PRIORITYFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_PROBLEMCODEFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_REQUESTENDFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_REQUESTSTARTFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_SCHEDSTARTFIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ALW_WORKORDERORGFIELDID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ALW_LASTSAVED), '1900-01-01')) is not null