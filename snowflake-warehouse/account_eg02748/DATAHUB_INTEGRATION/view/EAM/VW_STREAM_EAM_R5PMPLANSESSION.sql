CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PMPLANSESSION AS SELECT
                        src:PPS_DUEDATE::datetime AS PPS_DUEDATE, 
                        src:PPS_DUEDAYOFWEEK::numeric(38, 10) AS PPS_DUEDAYOFWEEK, 
                        src:PPS_DUEWEEKOFMONTH::varchar AS PPS_DUEWEEKOFMONTH, 
                        src:PPS_IGNOREFREQWARNING::varchar AS PPS_IGNOREFREQWARNING, 
                        src:PPS_IGNORERANGEWARNING::varchar AS PPS_IGNORERANGEWARNING, 
                        src:PPS_LASTSAVED::datetime AS PPS_LASTSAVED, 
                        src:PPS_LOCKED::varchar AS PPS_LOCKED, 
                        src:PPS_NESTINGREF::varchar AS PPS_NESTINGREF, 
                        src:PPS_PK::numeric(38, 10) AS PPS_PK, 
                        src:PPS_PMREVISION::numeric(38, 10) AS PPS_PMREVISION, 
                        src:PPS_PPOPK::numeric(38, 10) AS PPS_PPOPK, 
                        src:PPS_SESSIONID::numeric(38, 10) AS PPS_SESSIONID, 
                        src:PPS_UPDATECOUNT::numeric(38, 10) AS PPS_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PPS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PPS_PK  ORDER BY 
            src:PPS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PMPLANSESSION as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPS_DUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPS_DUEDAYOFWEEK), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPS_PK), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPS_PMREVISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPS_PPOPK), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPS_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PPS_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PPS_LASTSAVED), '1900-01-01')) is not null