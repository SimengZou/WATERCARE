CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UOMS AS SELECT
                        src:UOM_CLASS::varchar AS UOM_CLASS, 
                        src:UOM_CLASS_ORG::varchar AS UOM_CLASS_ORG, 
                        src:UOM_CODE::varchar AS UOM_CODE, 
                        src:UOM_CREATED::datetime AS UOM_CREATED, 
                        src:UOM_DESC::varchar AS UOM_DESC, 
                        src:UOM_LASTSAVED::datetime AS UOM_LASTSAVED, 
                        src:UOM_NOTUSED::varchar AS UOM_NOTUSED, 
                        src:UOM_SOAUOM::varchar AS UOM_SOAUOM, 
                        src:UOM_UPDATECOUNT::numeric(38, 10) AS UOM_UPDATECOUNT, 
                        src:UOM_UPDATED::datetime AS UOM_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UOM_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:UOM_CODE  ORDER BY 
            src:UOM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5UOMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UOM_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UOM_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UOM_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UOM_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UOM_LASTSAVED), '1900-01-01')) is not null