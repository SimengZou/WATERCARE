CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MOBILEUSERCONFIGS AS SELECT
                        src:MUC_CODE::varchar AS MUC_CODE, 
                        src:MUC_COMPTYPE::varchar AS MUC_COMPTYPE, 
                        src:MUC_CREATED::datetime AS MUC_CREATED, 
                        src:MUC_DESK::varchar AS MUC_DESK, 
                        src:MUC_GROUP::varchar AS MUC_GROUP, 
                        src:MUC_LASTSAVED::datetime AS MUC_LASTSAVED, 
                        src:MUC_RCODE::varchar AS MUC_RCODE, 
                        src:MUC_UPDATECOUNT::numeric(38, 10) AS MUC_UPDATECOUNT, 
                        src:MUC_UPDATED::datetime AS MUC_UPDATED, 
                        src:MUC_USER::varchar AS MUC_USER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MUC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MUC_CODE,
                src:MUC_DESK,
                src:MUC_GROUP,
                src:MUC_USER  ORDER BY 
            src:MUC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MOBILEUSERCONFIGS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MUC_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MUC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MUC_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MUC_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MUC_LASTSAVED), '1900-01-01')) is not null