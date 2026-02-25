CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBS AS SELECT
                        src:JOB_CLASS::varchar AS JOB_CLASS, 
                        src:JOB_DESCRIPTION::varchar AS JOB_DESCRIPTION, 
                        src:JOB_LASTSAVED::datetime AS JOB_LASTSAVED, 
                        src:JOB_NAME::varchar AS JOB_NAME, 
                        src:JOB_PARTNERID::varchar AS JOB_PARTNERID, 
                        src:JOB_TYPE::varchar AS JOB_TYPE, 
                        src:JOB_UPDATECOUNT::numeric(38, 10) AS JOB_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:JOB_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:JOB_NAME  ORDER BY 
            src:JOB_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5JOBS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:JOB_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:JOB_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:JOB_LASTSAVED), '1900-01-01')) is not null