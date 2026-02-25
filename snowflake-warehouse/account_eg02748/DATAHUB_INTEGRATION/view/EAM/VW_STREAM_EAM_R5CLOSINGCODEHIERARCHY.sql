CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CLOSINGCODEHIERARCHY AS SELECT
                        src:CCH_CHILDCLOSINGCODE::varchar AS CCH_CHILDCLOSINGCODE, 
                        src:CCH_CHILDCLOSINGCODETYPE::varchar AS CCH_CHILDCLOSINGCODETYPE, 
                        src:CCH_LASTSAVED::datetime AS CCH_LASTSAVED, 
                        src:CCH_OBJECT::varchar AS CCH_OBJECT, 
                        src:CCH_OBJECT_ORG::varchar AS CCH_OBJECT_ORG, 
                        src:CCH_PARENTCLOSINGCODE::varchar AS CCH_PARENTCLOSINGCODE, 
                        src:CCH_PARENTCLOSINGCODETYPE::varchar AS CCH_PARENTCLOSINGCODETYPE, 
                        src:CCH_REPLDEPT::varchar AS CCH_REPLDEPT, 
                        src:CCH_UPDATECOUNT::numeric(38, 10) AS CCH_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:CCH_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CCH_CHILDCLOSINGCODE,
                src:CCH_CHILDCLOSINGCODETYPE,
                src:CCH_PARENTCLOSINGCODE,
                src:CCH_PARENTCLOSINGCODETYPE  ORDER BY 
            src:CCH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5CLOSINGCODEHIERARCHY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CCH_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CCH_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CCH_LASTSAVED), '1900-01-01')) is not null