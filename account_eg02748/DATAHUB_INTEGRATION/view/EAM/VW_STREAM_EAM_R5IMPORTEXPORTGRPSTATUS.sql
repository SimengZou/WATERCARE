CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IMPORTEXPORTGRPSTATUS AS SELECT
                        src:IMG_END::datetime AS IMG_END, 
                        src:IMG_GROUP::varchar AS IMG_GROUP, 
                        src:IMG_LASTSAVED::datetime AS IMG_LASTSAVED, 
                        src:IMG_PROCESSORDER::numeric(38, 10) AS IMG_PROCESSORDER, 
                        src:IMG_SESSIONID::numeric(38, 10) AS IMG_SESSIONID, 
                        src:IMG_START::datetime AS IMG_START, 
                        src:IMG_STATUS::varchar AS IMG_STATUS, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:IMG_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:IMG_GROUP,
                src:IMG_SESSIONID  ORDER BY 
            src:IMG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IMPORTEXPORTGRPSTATUS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IMG_END), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IMG_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMG_PROCESSORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMG_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IMG_START), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:IMG_LASTSAVED), '1900-01-01')) is not null