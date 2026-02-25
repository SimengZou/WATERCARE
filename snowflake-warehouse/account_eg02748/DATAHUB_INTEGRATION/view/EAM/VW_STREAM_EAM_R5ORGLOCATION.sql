CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ORGLOCATION AS SELECT
                        src:OGL_BODGROUP::varchar AS OGL_BODGROUP, 
                        src:OGL_ENTERPRISELOCATION::varchar AS OGL_ENTERPRISELOCATION, 
                        src:OGL_LASTSAVED::datetime AS OGL_LASTSAVED, 
                        src:OGL_ORG::varchar AS OGL_ORG, 
                        src:OGL_UPDATECOUNT::numeric(38, 10) AS OGL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:OGL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:OGL_BODGROUP,
                src:OGL_ORG  ORDER BY 
            src:OGL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ORGLOCATION as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OGL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OGL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OGL_LASTSAVED), '1900-01-01')) is not null