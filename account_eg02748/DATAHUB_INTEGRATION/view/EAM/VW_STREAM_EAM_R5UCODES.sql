CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UCODES AS SELECT
                        src:UCO_CATEGORY::varchar AS UCO_CATEGORY, 
                        src:UCO_CODE::varchar AS UCO_CODE, 
                        src:UCO_COLOR::varchar AS UCO_COLOR, 
                        src:UCO_CREATED::datetime AS UCO_CREATED, 
                        src:UCO_DESC::varchar AS UCO_DESC, 
                        src:UCO_ENTITY::varchar AS UCO_ENTITY, 
                        src:UCO_GISDISPATCHRANKING::numeric(38, 10) AS UCO_GISDISPATCHRANKING, 
                        src:UCO_ICON::varchar AS UCO_ICON, 
                        src:UCO_ICONPATH::varchar AS UCO_ICONPATH, 
                        src:UCO_LASTSAVED::datetime AS UCO_LASTSAVED, 
                        src:UCO_NOTUSED::varchar AS UCO_NOTUSED, 
                        src:UCO_RCODE::varchar AS UCO_RCODE, 
                        src:UCO_RENTITY::varchar AS UCO_RENTITY, 
                        src:UCO_SYSTEM::varchar AS UCO_SYSTEM, 
                        src:UCO_TEXTCODE::varchar AS UCO_TEXTCODE, 
                        src:UCO_UPDATECOUNT::numeric(38, 10) AS UCO_UPDATECOUNT, 
                        src:UCO_UPDATED::datetime AS UCO_UPDATED, 
                        src:UCO_WEIGHT::numeric(38, 10) AS UCO_WEIGHT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UCO_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:UCO_CODE,
                src:UCO_ENTITY  ORDER BY 
            src:UCO_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UCODES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UCO_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UCO_GISDISPATCHRANKING), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UCO_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UCO_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UCO_UPDATED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UCO_WEIGHT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UCO_LASTSAVED), '1900-01-01')) is not null