CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5QUERYFIELD AS SELECT
                        src:DQF_COLUMNORDER::numeric(38, 10) AS DQF_COLUMNORDER, 
                        src:DQF_COLUMNWIDTH::varchar AS DQF_COLUMNWIDTH, 
                        src:DQF_DDSPYID::numeric(38, 10) AS DQF_DDSPYID, 
                        src:DQF_FIELDID::numeric(38, 10) AS DQF_FIELDID, 
                        src:DQF_LASTSAVED::datetime AS DQF_LASTSAVED, 
                        src:DQF_UPDATECOUNT::numeric(38, 10) AS DQF_UPDATECOUNT, 
                        src:DQF_UPDATED::datetime AS DQF_UPDATED, 
                        src:DQF_VIEWTYPE::varchar AS DQF_VIEWTYPE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DQF_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DQF_DDSPYID,
                src:DQF_FIELDID,
                src:DQF_VIEWTYPE  ORDER BY 
            src:DQF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5QUERYFIELD as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DQF_COLUMNORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DQF_DDSPYID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DQF_FIELDID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DQF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DQF_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DQF_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DQF_LASTSAVED), '1900-01-01')) is not null