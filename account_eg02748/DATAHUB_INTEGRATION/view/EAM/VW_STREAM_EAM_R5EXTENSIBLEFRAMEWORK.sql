CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EXTENSIBLEFRAMEWORK AS SELECT
                        src:EFR_ACTIVE::varchar AS EFR_ACTIVE, 
                        src:EFR_LASTSAVED::datetime AS EFR_LASTSAVED, 
                        src:EFR_NAME::varchar AS EFR_NAME, 
                        src:EFR_SOURCECODE::varchar AS EFR_SOURCECODE, 
                        src:EFR_UPDATECOUNT::numeric(38, 10) AS EFR_UPDATECOUNT, 
                        src:EFR_USERFUNCTION::varchar AS EFR_USERFUNCTION, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:EFR_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:EFR_NAME  ORDER BY 
            src:EFR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EXTENSIBLEFRAMEWORK as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EFR_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFR_LASTSAVED), '1900-01-01')) is not null