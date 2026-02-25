CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DRIVERCTRL AS SELECT
                        src:DRV_ACTION::varchar AS DRV_ACTION, 
                        src:DRV_CODE::varchar AS DRV_CODE, 
                        src:DRV_FREQUENCY::numeric(38, 10) AS DRV_FREQUENCY, 
                        src:DRV_LASTRAN::datetime AS DRV_LASTRAN, 
                        src:DRV_LASTSAVED::datetime AS DRV_LASTSAVED, 
                        src:DRV_NEXTRUN::datetime AS DRV_NEXTRUN, 
                        src:DRV_QUEUE::varchar AS DRV_QUEUE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DRV_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DRV_CODE,
                src:DRV_QUEUE  ORDER BY 
            src:DRV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DRIVERCTRL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DRV_FREQUENCY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DRV_LASTRAN), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DRV_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DRV_NEXTRUN), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DRV_LASTSAVED), '1900-01-01')) is not null