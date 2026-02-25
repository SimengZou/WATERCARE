CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PRODUCTVERSIONHISTORY AS SELECT
                        src:PVH_BUILD::varchar AS PVH_BUILD, 
                        src:PVH_BUILDDATE::varchar AS PVH_BUILDDATE, 
                        src:PVH_CHANGED::datetime AS PVH_CHANGED, 
                        src:PVH_DESC::varchar AS PVH_DESC, 
                        src:PVH_LASTSAVED::datetime AS PVH_LASTSAVED, 
                        src:PVH_PATCH::varchar AS PVH_PATCH, 
                        src:PVH_PRODUCTCODE::varchar AS PVH_PRODUCTCODE, 
                        src:PVH_UPDATECOUNT::numeric(38, 10) AS PVH_UPDATECOUNT, 
                        src:PVH_VERSION::varchar AS PVH_VERSION, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PVH_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PVH_CHANGED,
                src:PVH_PRODUCTCODE  ORDER BY 
            src:PVH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PRODUCTVERSIONHISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PVH_CHANGED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PVH_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PVH_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PVH_LASTSAVED), '1900-01-01')) is not null