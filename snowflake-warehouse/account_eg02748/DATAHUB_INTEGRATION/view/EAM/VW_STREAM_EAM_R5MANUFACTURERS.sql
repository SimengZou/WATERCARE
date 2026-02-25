CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MANUFACTURERS AS SELECT
                        src:MFG_CLASS::varchar AS MFG_CLASS, 
                        src:MFG_CLASS_ORG::varchar AS MFG_CLASS_ORG, 
                        src:MFG_CODE::varchar AS MFG_CODE, 
                        src:MFG_DESC::varchar AS MFG_DESC, 
                        src:MFG_LASTSAVED::datetime AS MFG_LASTSAVED, 
                        src:MFG_NOTUSED::varchar AS MFG_NOTUSED, 
                        src:MFG_ORG::varchar AS MFG_ORG, 
                        src:MFG_SOURCECODE::varchar AS MFG_SOURCECODE, 
                        src:MFG_SOURCESYSTEM::varchar AS MFG_SOURCESYSTEM, 
                        src:MFG_SUPPLIER::varchar AS MFG_SUPPLIER, 
                        src:MFG_SUPPLIER_ORG::varchar AS MFG_SUPPLIER_ORG, 
                        src:MFG_UPDATECOUNT::numeric(38, 10) AS MFG_UPDATECOUNT, 
                        src:MFG_UPDATED::datetime AS MFG_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:MFG_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:MFG_CODE  ORDER BY 
            src:MFG_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5MANUFACTURERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MFG_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MFG_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MFG_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MFG_LASTSAVED), '1900-01-01')) is not null