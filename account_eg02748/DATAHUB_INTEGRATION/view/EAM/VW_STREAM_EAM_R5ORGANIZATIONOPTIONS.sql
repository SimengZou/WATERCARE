CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ORGANIZATIONOPTIONS AS SELECT
                        src:OPA_CODE::varchar AS OPA_CODE, 
                        src:OPA_COMMENT::varchar AS OPA_COMMENT, 
                        src:OPA_DESC::varchar AS OPA_DESC, 
                        src:OPA_FIXED::varchar AS OPA_FIXED, 
                        src:OPA_LASTSAVED::datetime AS OPA_LASTSAVED, 
                        src:OPA_MODULE::varchar AS OPA_MODULE, 
                        src:OPA_ORG::varchar AS OPA_ORG, 
                        src:OPA_SYSTEM::varchar AS OPA_SYSTEM, 
                        src:OPA_TYPE::varchar AS OPA_TYPE, 
                        src:OPA_UPDATECOUNT::numeric(38, 10) AS OPA_UPDATECOUNT, 
                        src:OPA_VALIDVALUES::varchar AS OPA_VALIDVALUES, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:OPA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:OPA_CODE,
                src:OPA_ORG,
                src:OPA_SYSTEM  ORDER BY 
            src:OPA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORGANIZATIONOPTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OPA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPA_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OPA_LASTSAVED), '1900-01-01')) is not null