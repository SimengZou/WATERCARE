CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAPSCONSENT AS SELECT
                        src:MCT_APPID::varchar AS MCT_APPID, 
                        src:MCT_DEVICEID::varchar AS MCT_DEVICEID, 
                        src:MCT_LASTSAVED::datetime AS MCT_LASTSAVED, 
                        src:MCT_LASTUPDATED::datetime AS MCT_LASTUPDATED, 
                        src:MCT_PRODUCT::varchar AS MCT_PRODUCT, 
                        src:MCT_UPDATECOUNT::numeric(38, 10) AS MCT_UPDATECOUNT, 
                        src:MCT_USERID::varchar AS MCT_USERID, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MCT_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MCT_APPID,
                src:MCT_DEVICEID,
                src:MCT_PRODUCT,
                src:MCT_USERID  ORDER BY 
            src:MCT_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MAPSCONSENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCT_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCT_LASTUPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MCT_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MCT_LASTSAVED), '1900-01-01')) is not null