CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REMEMBER AS SELECT
                        src:RMB_FUNCTION::varchar AS RMB_FUNCTION, 
                        src:RMB_ITEM::varchar AS RMB_ITEM, 
                        src:RMB_ITEM2::varchar AS RMB_ITEM2, 
                        src:RMB_LASTSAVED::datetime AS RMB_LASTSAVED, 
                        src:RMB_RENTITY::varchar AS RMB_RENTITY, 
                        src:RMB_UPDATECOUNT::numeric(38, 10) AS RMB_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RMB_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RMB_FUNCTION,
                src:RMB_ITEM,
                src:RMB_RENTITY  ORDER BY 
            src:RMB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REMEMBER as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RMB_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RMB_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RMB_LASTSAVED), '1900-01-01')) is not null