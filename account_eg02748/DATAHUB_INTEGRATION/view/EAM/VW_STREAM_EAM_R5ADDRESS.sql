CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDRESS AS SELECT
                        src:ADR_ADDRESS1::varchar AS ADR_ADDRESS1, 
                        src:ADR_ADDRESS2::varchar AS ADR_ADDRESS2, 
                        src:ADR_ADDRESS3::varchar AS ADR_ADDRESS3, 
                        src:ADR_CITY::varchar AS ADR_CITY, 
                        src:ADR_CODE::varchar AS ADR_CODE, 
                        src:ADR_COUNTRY::varchar AS ADR_COUNTRY, 
                        src:ADR_EMAIL::varchar AS ADR_EMAIL, 
                        src:ADR_FAX::varchar AS ADR_FAX, 
                        src:ADR_LASTSAVED::datetime AS ADR_LASTSAVED, 
                        src:ADR_PHONE::varchar AS ADR_PHONE, 
                        src:ADR_PHONEEXTN::varchar AS ADR_PHONEEXTN, 
                        src:ADR_RENTITY::varchar AS ADR_RENTITY, 
                        src:ADR_RTYPE::varchar AS ADR_RTYPE, 
                        src:ADR_STATE::varchar AS ADR_STATE, 
                        src:ADR_TEXT::varchar AS ADR_TEXT, 
                        src:ADR_UPDATECOUNT::numeric(38, 10) AS ADR_UPDATECOUNT, 
                        src:ADR_ZIP::varchar AS ADR_ZIP, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:ADR_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:ADR_CODE,
                src:ADR_RENTITY,
                src:ADR_RTYPE  ORDER BY 
            src:ADR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ADDRESS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADR_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADR_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADR_LASTSAVED), '1900-01-01')) is not null