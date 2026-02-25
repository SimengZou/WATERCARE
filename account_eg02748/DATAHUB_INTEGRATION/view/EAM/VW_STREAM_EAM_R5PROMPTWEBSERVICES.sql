CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROMPTWEBSERVICES AS SELECT
                        src:PWS_ACTIONCODE::varchar AS PWS_ACTIONCODE, 
                        src:PWS_BOTTOMBLOCKTITLE::varchar AS PWS_BOTTOMBLOCKTITLE, 
                        src:PWS_CFBLOCKTITLE::varchar AS PWS_CFBLOCKTITLE, 
                        src:PWS_CFKEYCODE::varchar AS PWS_CFKEYCODE, 
                        src:PWS_FUNCTION::varchar AS PWS_FUNCTION, 
                        src:PWS_LASTSAVED::datetime AS PWS_LASTSAVED, 
                        src:PWS_ORGXPATH::varchar AS PWS_ORGXPATH, 
                        src:PWS_PROCESSGROUP::numeric(38, 10) AS PWS_PROCESSGROUP, 
                        src:PWS_TAB::varchar AS PWS_TAB, 
                        src:PWS_TOPBLOCKTITLE::varchar AS PWS_TOPBLOCKTITLE, 
                        src:PWS_UPDATECOUNT::numeric(38, 10) AS PWS_UPDATECOUNT, 
                        src:PWS_UPDATED::datetime AS PWS_UPDATED, 
                        src:PWS_WEBSERVICE::varchar AS PWS_WEBSERVICE, 
                        src:PWS_WSPRMCODE::varchar AS PWS_WSPRMCODE, 
                        src:PWS_WSTITLE::varchar AS PWS_WSTITLE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PWS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PWS_PROCESSGROUP,
                src:PWS_WSPRMCODE  ORDER BY 
            src:PWS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROMPTWEBSERVICES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PWS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PWS_PROCESSGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PWS_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PWS_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PWS_LASTSAVED), '1900-01-01')) is not null