CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONCLAUSES AS SELECT
                        src:CCL_CLASS::varchar AS CCL_CLASS, 
                        src:CCL_CLASS_ORG::varchar AS CCL_CLASS_ORG, 
                        src:CCL_CODE::varchar AS CCL_CODE, 
                        src:CCL_DESC::varchar AS CCL_DESC, 
                        src:CCL_LASTSAVED::datetime AS CCL_LASTSAVED, 
                        src:CCL_LINE::numeric(38, 10) AS CCL_LINE, 
                        src:CCL_NOTUSED::varchar AS CCL_NOTUSED, 
                        src:CCL_ORG::varchar AS CCL_ORG, 
                        src:CCL_PARENT::varchar AS CCL_PARENT, 
                        src:CCL_SYSTEM::varchar AS CCL_SYSTEM, 
                        src:CCL_UPDATECOUNT::numeric(38, 10) AS CCL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CCL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CCL_CODE  ORDER BY 
            src:CCL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CONCLAUSES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CCL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CCL_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CCL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CCL_LASTSAVED), '1900-01-01')) is not null