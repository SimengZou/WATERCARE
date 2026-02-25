CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEPNREGISTRY AS SELECT
                        src:MPN_APPID::varchar AS MPN_APPID, 
                        src:MPN_CREATED::datetime AS MPN_CREATED, 
                        src:MPN_CREATEDBY::varchar AS MPN_CREATEDBY, 
                        src:MPN_DEVICEID::varchar AS MPN_DEVICEID, 
                        src:MPN_LASTSAVED::datetime AS MPN_LASTSAVED, 
                        src:MPN_PLATFORM::varchar AS MPN_PLATFORM, 
                        src:MPN_TOKEN::varchar AS MPN_TOKEN, 
                        src:MPN_UPDATECOUNT::numeric(38, 10) AS MPN_UPDATECOUNT, 
                        src:MPN_UPDATED::datetime AS MPN_UPDATED, 
                        src:MPN_UPDATEDBY::varchar AS MPN_UPDATEDBY, 
                        src:MPN_USER::varchar AS MPN_USER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MPN_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MPN_APPID,
                src:MPN_DEVICEID  ORDER BY 
            src:MPN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILEPNREGISTRY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MPN_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MPN_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MPN_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MPN_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MPN_LASTSAVED), '1900-01-01')) is not null