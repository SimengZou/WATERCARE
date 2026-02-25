CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FUNCUSERS AS SELECT
                        src:FUS_BUTFUN1::varchar AS FUS_BUTFUN1, 
                        src:FUS_BUTFUN2::varchar AS FUS_BUTFUN2, 
                        src:FUS_BUTFUN3::varchar AS FUS_BUTFUN3, 
                        src:FUS_BUTFUN4::varchar AS FUS_BUTFUN4, 
                        src:FUS_BUTFUN5::varchar AS FUS_BUTFUN5, 
                        src:FUS_BUTFUN6::varchar AS FUS_BUTFUN6, 
                        src:FUS_BUTICON1::varchar AS FUS_BUTICON1, 
                        src:FUS_BUTICON2::varchar AS FUS_BUTICON2, 
                        src:FUS_BUTICON3::varchar AS FUS_BUTICON3, 
                        src:FUS_BUTICON4::varchar AS FUS_BUTICON4, 
                        src:FUS_BUTICON5::varchar AS FUS_BUTICON5, 
                        src:FUS_BUTICON6::varchar AS FUS_BUTICON6, 
                        src:FUS_BUTNAME1::varchar AS FUS_BUTNAME1, 
                        src:FUS_BUTNAME2::varchar AS FUS_BUTNAME2, 
                        src:FUS_BUTNAME3::varchar AS FUS_BUTNAME3, 
                        src:FUS_BUTNAME4::varchar AS FUS_BUTNAME4, 
                        src:FUS_BUTNAME5::varchar AS FUS_BUTNAME5, 
                        src:FUS_BUTNAME6::varchar AS FUS_BUTNAME6, 
                        src:FUS_FILTER::varchar AS FUS_FILTER, 
                        src:FUS_FUNCTION::varchar AS FUS_FUNCTION, 
                        src:FUS_LASTSAVED::datetime AS FUS_LASTSAVED, 
                        src:FUS_NOTES::varchar AS FUS_NOTES, 
                        src:FUS_PRINTER::varchar AS FUS_PRINTER, 
                        src:FUS_SHORTN::varchar AS FUS_SHORTN, 
                        src:FUS_UPDATECOUNT::numeric(38, 10) AS FUS_UPDATECOUNT, 
                        src:FUS_USER::varchar AS FUS_USER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:FUS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:FUS_FUNCTION,
                src:FUS_USER  ORDER BY 
            src:FUS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5FUNCUSERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FUS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FUS_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:FUS_LASTSAVED), '1900-01-01')) is not null