CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBTYPEAUTH AS SELECT
                        src:JTA_DELETE::varchar AS JTA_DELETE, 
                        src:JTA_GROUP::varchar AS JTA_GROUP, 
                        src:JTA_INSERT::varchar AS JTA_INSERT, 
                        src:JTA_JOBTYPE::varchar AS JTA_JOBTYPE, 
                        src:JTA_LASTSAVED::datetime AS JTA_LASTSAVED, 
                        src:JTA_STATUS::varchar AS JTA_STATUS, 
                        src:JTA_UPDATE::varchar AS JTA_UPDATE, 
                        src:JTA_UPDATECOUNT::numeric(38, 10) AS JTA_UPDATECOUNT, 
                        src:JTA_UPDATED::datetime AS JTA_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:JTA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:JTA_GROUP,
                src:JTA_JOBTYPE,
                src:JTA_STATUS  ORDER BY 
            src:JTA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5JOBTYPEAUTH as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:JTA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:JTA_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:JTA_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:JTA_LASTSAVED), '1900-01-01')) is not null