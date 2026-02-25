CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_P100 AS SELECT
                        src:PM100::varchar AS PM100, 
                        src:Period::varchar AS Period, 
                        src:Project::varchar AS Project, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Value::numeric(38, 10) AS Value, 
                        src:Version::varchar AS Version, 
                        src:WBS::varchar AS WBS, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:Project,
                src:Version,
                src:PM100,
                src:Value,
                src:Period,
                src:WBS  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_P100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:Value), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null