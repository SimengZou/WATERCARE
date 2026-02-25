CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PERMISSIONS AS SELECT
                        src:PRM_CREATED::datetime AS PRM_CREATED, 
                        src:PRM_DEFQUERY::varchar AS PRM_DEFQUERY, 
                        src:PRM_DELETE::varchar AS PRM_DELETE, 
                        src:PRM_FUNCTION::varchar AS PRM_FUNCTION, 
                        src:PRM_GROUP::varchar AS PRM_GROUP, 
                        src:PRM_INPUT::varchar AS PRM_INPUT, 
                        src:PRM_INSERT::varchar AS PRM_INSERT, 
                        src:PRM_LASTSAVED::datetime AS PRM_LASTSAVED, 
                        src:PRM_LOCAL::varchar AS PRM_LOCAL, 
                        src:PRM_OVERRULE::varchar AS PRM_OVERRULE, 
                        src:PRM_PRFILE::varchar AS PRM_PRFILE, 
                        src:PRM_PRINTER::varchar AS PRM_PRINTER, 
                        src:PRM_SCREEN::varchar AS PRM_SCREEN, 
                        src:PRM_SECURITYDDSPYID::numeric(38, 10) AS PRM_SECURITYDDSPYID, 
                        src:PRM_SELECT::varchar AS PRM_SELECT, 
                        src:PRM_UPDATE::varchar AS PRM_UPDATE, 
                        src:PRM_UPDATECOUNT::numeric(38, 10) AS PRM_UPDATECOUNT, 
                        src:PRM_UPDATED::datetime AS PRM_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PRM_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PRM_FUNCTION,
                src:PRM_GROUP  ORDER BY 
            src:PRM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PERMISSIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRM_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRM_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRM_SECURITYDDSPYID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRM_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRM_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRM_LASTSAVED), '1900-01-01')) is not null