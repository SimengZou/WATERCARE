CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTYVALUES AS SELECT
                        src:PRV_CLASS::varchar AS PRV_CLASS, 
                        src:PRV_CLASS_ORG::varchar AS PRV_CLASS_ORG, 
                        src:PRV_CODE::varchar AS PRV_CODE, 
                        src:PRV_CREATED::datetime AS PRV_CREATED, 
                        src:PRV_DVALUE::datetime AS PRV_DVALUE, 
                        src:PRV_LASTSAVED::datetime AS PRV_LASTSAVED, 
                        src:PRV_NOTUSED::varchar AS PRV_NOTUSED, 
                        src:PRV_NVALUE::numeric(38, 10) AS PRV_NVALUE, 
                        src:PRV_PROPERTY::varchar AS PRV_PROPERTY, 
                        src:PRV_RENTITY::varchar AS PRV_RENTITY, 
                        src:PRV_SEQNO::numeric(38, 10) AS PRV_SEQNO, 
                        src:PRV_UPDATECOUNT::numeric(38, 10) AS PRV_UPDATECOUNT, 
                        src:PRV_UPDATED::datetime AS PRV_UPDATED, 
                        src:PRV_VALUE::varchar AS PRV_VALUE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PRV_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PRV_CLASS,
                src:PRV_CLASS_ORG,
                src:PRV_CODE,
                src:PRV_PROPERTY,
                src:PRV_RENTITY,
                src:PRV_SEQNO  ORDER BY 
            src:PRV_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PROPERTYVALUES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRV_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRV_DVALUE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRV_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRV_NVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRV_SEQNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRV_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRV_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PRV_LASTSAVED), '1900-01-01')) is not null