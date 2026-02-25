CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LABELFORMATS AS SELECT
                        src:LBL_CLASS::varchar AS LBL_CLASS, 
                        src:LBL_CLASS_ORG::varchar AS LBL_CLASS_ORG, 
                        src:LBL_CODE::varchar AS LBL_CODE, 
                        src:LBL_DESC::varchar AS LBL_DESC, 
                        src:LBL_FIELDS::varchar AS LBL_FIELDS, 
                        src:LBL_FILENAME::varchar AS LBL_FILENAME, 
                        src:LBL_LASTSAVED::datetime AS LBL_LASTSAVED, 
                        src:LBL_PRINTERTYPE::varchar AS LBL_PRINTERTYPE, 
                        src:LBL_UPDATECOUNT::numeric(38, 10) AS LBL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LBL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:LBL_CLASS,
                src:LBL_CLASS_ORG,
                src:LBL_CODE  ORDER BY 
            src:LBL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5LABELFORMATS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LBL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LBL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LBL_LASTSAVED), '1900-01-01')) is not null