CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5GRIDPARAM AS SELECT
                        src:GDP_DATATYPE::varchar AS GDP_DATATYPE, 
                        src:GDP_GRIDID::numeric(38, 10) AS GDP_GRIDID, 
                        src:GDP_LASTSAVED::datetime AS GDP_LASTSAVED, 
                        src:GDP_PARAM::varchar AS GDP_PARAM, 
                        src:GDP_TAGNAME::varchar AS GDP_TAGNAME, 
                        src:GDP_UPDATECOUNT::numeric(38, 10) AS GDP_UPDATECOUNT, 
                        src:GDP_UPDATED::datetime AS GDP_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:GDP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:GDP_GRIDID,
                src:GDP_PARAM  ORDER BY 
            src:GDP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5GRIDPARAM as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GDP_GRIDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:GDP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GDP_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:GDP_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:GDP_LASTSAVED), '1900-01-01')) is not null