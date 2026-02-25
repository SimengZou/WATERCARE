CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5DOCENTITIES AS SELECT
                        src:DAE_CODE::varchar AS DAE_CODE, 
                        src:DAE_COPYTOPO::varchar AS DAE_COPYTOPO, 
                        src:DAE_COPYTOWO::varchar AS DAE_COPYTOWO, 
                        src:DAE_CREATECOPYTOWO::varchar AS DAE_CREATECOPYTOWO, 
                        src:DAE_DOCUMENT::varchar AS DAE_DOCUMENT, 
                        src:DAE_ENTITY::varchar AS DAE_ENTITY, 
                        src:DAE_IDMCOPY::varchar AS DAE_IDMCOPY, 
                        src:DAE_LASTSAVED::datetime AS DAE_LASTSAVED, 
                        src:DAE_PRINTONPO::varchar AS DAE_PRINTONPO, 
                        src:DAE_PRINTONWO::varchar AS DAE_PRINTONWO, 
                        src:DAE_RENTITY::varchar AS DAE_RENTITY, 
                        src:DAE_RTYPE::varchar AS DAE_RTYPE, 
                        src:DAE_TYPE::varchar AS DAE_TYPE, 
                        src:DAE_UPDATECOUNT::numeric(38, 10) AS DAE_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:DAE_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:DAE_CODE,
                src:DAE_DOCUMENT,
                src:DAE_RENTITY  ORDER BY 
            src:DAE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5DOCENTITIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DAE_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DAE_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DAE_LASTSAVED), '1900-01-01')) is not null