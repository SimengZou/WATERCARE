CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERVIEWS AS SELECT
                        src:UVW_ACTIVE::varchar AS UVW_ACTIVE, 
                        src:UVW_CODE::varchar AS UVW_CODE, 
                        src:UVW_DESC::varchar AS UVW_DESC, 
                        src:UVW_LASTSAVED::datetime AS UVW_LASTSAVED, 
                        src:UVW_NAME::varchar AS UVW_NAME, 
                        src:UVW_NOTE::varchar AS UVW_NOTE, 
                        src:UVW_STMT::varchar AS UVW_STMT, 
                        src:UVW_UPDATECOUNT::numeric(38, 10) AS UVW_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UVW_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:UVW_CODE  ORDER BY 
            src:UVW_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERVIEWS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UVW_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UVW_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UVW_LASTSAVED), '1900-01-01')) is not null