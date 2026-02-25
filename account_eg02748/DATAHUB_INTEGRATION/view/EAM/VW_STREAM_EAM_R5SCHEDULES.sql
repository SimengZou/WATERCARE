CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5SCHEDULES AS SELECT
                        src:SCH_CODE::varchar AS SCH_CODE, 
                        src:SCH_DAYOFMONTH::varchar AS SCH_DAYOFMONTH, 
                        src:SCH_DAYOFWEEK::varchar AS SCH_DAYOFWEEK, 
                        src:SCH_DESCRIPTION::varchar AS SCH_DESCRIPTION, 
                        src:SCH_HOUR::varchar AS SCH_HOUR, 
                        src:SCH_LASTSAVED::datetime AS SCH_LASTSAVED, 
                        src:SCH_MINUTE::varchar AS SCH_MINUTE, 
                        src:SCH_MONTH::varchar AS SCH_MONTH, 
                        src:SCH_NAME::varchar AS SCH_NAME, 
                        src:SCH_TYPE::varchar AS SCH_TYPE, 
                        src:SCH_UPDATECOUNT::numeric(38, 10) AS SCH_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:SCH_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:SCH_CODE  ORDER BY 
            src:SCH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5SCHEDULES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCH_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SCH_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCH_LASTSAVED), '1900-01-01')) is not null