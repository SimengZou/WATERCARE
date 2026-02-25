CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DESCRIPTIONS AS SELECT
                        src:DES_CODE::varchar AS DES_CODE, 
                        src:DES_ENTITY::varchar AS DES_ENTITY, 
                        src:DES_LANG::varchar AS DES_LANG, 
                        src:DES_LASTSAVED::datetime AS DES_LASTSAVED, 
                        src:DES_ORG::varchar AS DES_ORG, 
                        src:DES_RENTITY::varchar AS DES_RENTITY, 
                        src:DES_RTYPE::varchar AS DES_RTYPE, 
                        src:DES_TEXT::varchar AS DES_TEXT, 
                        src:DES_TRANS::varchar AS DES_TRANS, 
                        src:DES_TYPE::varchar AS DES_TYPE, 
                        src:DES_UPDATECOUNT::numeric(38, 10) AS DES_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DES_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DES_CODE,
                src:DES_LANG,
                src:DES_ORG,
                src:DES_RENTITY,
                src:DES_RTYPE  ORDER BY 
            src:DES_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DESCRIPTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DES_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DES_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DES_LASTSAVED), '1900-01-01')) is not null