CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ACTSCHEDULES AS SELECT
                        src:ACS_ACTIVITY::numeric(38, 10) AS ACS_ACTIVITY, 
                        src:ACS_CODE::varchar AS ACS_CODE, 
                        src:ACS_COMMENT::varchar AS ACS_COMMENT, 
                        src:ACS_ENDTIME::numeric(38, 10) AS ACS_ENDTIME, 
                        src:ACS_EVENT::varchar AS ACS_EVENT, 
                        src:ACS_FROZEN::varchar AS ACS_FROZEN, 
                        src:ACS_HOURS::numeric(38, 10) AS ACS_HOURS, 
                        src:ACS_LASTSAVED::datetime AS ACS_LASTSAVED, 
                        src:ACS_MPPROJ::varchar AS ACS_MPPROJ, 
                        src:ACS_MRC::varchar AS ACS_MRC, 
                        src:ACS_OBJECT::varchar AS ACS_OBJECT, 
                        src:ACS_OBJECT_ORG::varchar AS ACS_OBJECT_ORG, 
                        src:ACS_RESPONSIBLE::varchar AS ACS_RESPONSIBLE, 
                        src:ACS_SCHED::datetime AS ACS_SCHED, 
                        src:ACS_SCHEDULER::varchar AS ACS_SCHEDULER, 
                        src:ACS_SHIFT::varchar AS ACS_SHIFT, 
                        src:ACS_SHIFTSCHEDULESESSION::numeric(38, 10) AS ACS_SHIFTSCHEDULESESSION, 
                        src:ACS_STARTTIME::numeric(38, 10) AS ACS_STARTTIME, 
                        src:ACS_TRADE::varchar AS ACS_TRADE, 
                        src:ACS_UPDATECOUNT::numeric(38, 10) AS ACS_UPDATECOUNT, 
                        src:ACS_UPDATED::datetime AS ACS_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ACS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ACS_CODE  ORDER BY 
            src:ACS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ACTSCHEDULES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACS_ACTIVITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACS_ENDTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACS_HOURS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACS_SCHED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACS_SHIFTSCHEDULESESSION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACS_STARTTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACS_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACS_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACS_LASTSAVED), '1900-01-01')) is not null