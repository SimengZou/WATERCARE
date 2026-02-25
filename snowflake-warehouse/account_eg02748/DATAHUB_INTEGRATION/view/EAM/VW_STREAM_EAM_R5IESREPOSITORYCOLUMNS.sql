CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IESREPOSITORYCOLUMNS AS SELECT
                        src:ENC_ALIAS::varchar AS ENC_ALIAS, 
                        src:ENC_COLUMNNAME::varchar AS ENC_COLUMNNAME, 
                        src:ENC_DATATYPE::varchar AS ENC_DATATYPE, 
                        src:ENC_DISPLAYORDER::numeric(38, 10) AS ENC_DISPLAYORDER, 
                        src:ENC_FACET::varchar AS ENC_FACET, 
                        src:ENC_FIELDNAME::varchar AS ENC_FIELDNAME, 
                        src:ENC_HIDDENFROMSEARCHRESULTS::varchar AS ENC_HIDDENFROMSEARCHRESULTS, 
                        src:ENC_ISJSPKEYFIELD::varchar AS ENC_ISJSPKEYFIELD, 
                        src:ENC_LASTSAVED::datetime AS ENC_LASTSAVED, 
                        src:ENC_PKORDER::numeric(38, 10) AS ENC_PKORDER, 
                        src:ENC_REPOSITORYID::varchar AS ENC_REPOSITORYID, 
                        src:ENC_UPDATECOUNT::numeric(38, 10) AS ENC_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ENC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ENC_COLUMNNAME,
                src:ENC_REPOSITORYID  ORDER BY 
            src:ENC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5IESREPOSITORYCOLUMNS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENC_DISPLAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENC_PKORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENC_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ENC_LASTSAVED), '1900-01-01')) is not null