CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_C100 AS SELECT
                        src:Asset::varchar AS Asset, 
                        src:AssetBook::varchar AS AssetBook, 
                        src:CM100::varchar AS CM100, 
                        src:Project::varchar AS Project, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Value::varchar AS Value, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:Asset,
                src:AssetBook,
                src:CM100,
                src:Project  ORDER BY 
            src:Timestamp desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_C100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null