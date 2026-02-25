CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5PUBLICHOLIDAYS AS SELECT
                        src:AUTO::numeric(38, 10) AS AUTO, 
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:HLY_CONCODE::varchar AS HLY_CONCODE, 
                        src:HLY_DATE::datetime AS HLY_DATE, 
                        src:HLY_ID::varchar AS HLY_ID, 
                        src:HLY_NAME::varchar AS HLY_NAME, 
                        src:HLY_ORG::varchar AS HLY_ORG, 
                        src:HLY_YEAR::varchar AS HLY_YEAR, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:HLY_ID  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5PUBLICHOLIDAYS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTO), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:HLY_DATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null