CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REQUIRCODES AS SELECT
                        src:RQM_CLASS::varchar AS RQM_CLASS, 
                        src:RQM_CLASS_ORG::varchar AS RQM_CLASS_ORG, 
                        src:RQM_CODE::varchar AS RQM_CODE, 
                        src:RQM_CREATED::datetime AS RQM_CREATED, 
                        src:RQM_DESC::varchar AS RQM_DESC, 
                        src:RQM_ENABLEWORKORDERS::varchar AS RQM_ENABLEWORKORDERS, 
                        src:RQM_GEN::varchar AS RQM_GEN, 
                        src:RQM_GROUP::varchar AS RQM_GROUP, 
                        src:RQM_LASTSAVED::datetime AS RQM_LASTSAVED, 
                        src:RQM_NOTUSED::varchar AS RQM_NOTUSED, 
                        src:RQM_PARTFAILURE::varchar AS RQM_PARTFAILURE, 
                        src:RQM_UPDATECOUNT::numeric(38, 10) AS RQM_UPDATECOUNT, 
                        src:RQM_UPDATED::datetime AS RQM_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RQM_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RQM_CODE  ORDER BY 
            src:RQM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REQUIRCODES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RQM_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RQM_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RQM_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RQM_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RQM_LASTSAVED), '1900-01-01')) is not null