CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDPROPERTIES AS SELECT
                        src:APR_CLASS::varchar AS APR_CLASS, 
                        src:APR_CLASS_ORG::varchar AS APR_CLASS_ORG, 
                        src:APR_CREATED::datetime AS APR_CREATED, 
                        src:APR_GROUPLABEL::varchar AS APR_GROUPLABEL, 
                        src:APR_LASTSAVED::datetime AS APR_LASTSAVED, 
                        src:APR_LINE::numeric(38, 10) AS APR_LINE, 
                        src:APR_LIST::varchar AS APR_LIST, 
                        src:APR_LISTVALID::varchar AS APR_LISTVALID, 
                        src:APR_NONUPDCAT::varchar AS APR_NONUPDCAT, 
                        src:APR_PROPERTY::varchar AS APR_PROPERTY, 
                        src:APR_RENTITY::varchar AS APR_RENTITY, 
                        src:APR_REQUIRED::varchar AS APR_REQUIRED, 
                        src:APR_STATUS::varchar AS APR_STATUS, 
                        src:APR_UOM::varchar AS APR_UOM, 
                        src:APR_UPDATECOUNT::numeric(38, 10) AS APR_UPDATECOUNT, 
                        src:APR_UPDATED::datetime AS APR_UPDATED, 
                        src:APR_WODISP::varchar AS APR_WODISP, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:APR_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:APR_CLASS,
                src:APR_CLASS_ORG,
                src:APR_PROPERTY,
                src:APR_RENTITY  ORDER BY 
            src:APR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ADDPROPERTIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APR_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APR_LINE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APR_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APR_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APR_LASTSAVED), '1900-01-01')) is not null