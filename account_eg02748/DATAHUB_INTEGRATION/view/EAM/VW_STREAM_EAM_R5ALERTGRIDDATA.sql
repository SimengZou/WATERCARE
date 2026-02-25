CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTGRIDDATA AS SELECT
                        src:AGD_ALERT::varchar AS AGD_ALERT, 
                        src:AGD_DATA::varchar AS AGD_DATA, 
                        src:AGD_DATASPYID::numeric(38, 10) AS AGD_DATASPYID, 
                        src:AGD_DATE::datetime AS AGD_DATE, 
                        src:AGD_DESCRIPTION::varchar AS AGD_DESCRIPTION, 
                        src:AGD_GRIDID::numeric(38, 10) AS AGD_GRIDID, 
                        src:AGD_LASTSAVED::datetime AS AGD_LASTSAVED, 
                        src:AGD_PK::varchar AS AGD_PK, 
                        src:AGD_RECIPIENT::varchar AS AGD_RECIPIENT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:AGD_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AGD_PK  ORDER BY 
            src:AGD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTGRIDDATA as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGD_DATASPYID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGD_DATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGD_GRIDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGD_LASTSAVED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGD_LASTSAVED), '1900-01-01')) is not null