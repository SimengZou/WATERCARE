CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTITYPARTS AS SELECT
                        src:EPA_ASMLEVEL::varchar AS EPA_ASMLEVEL, 
                        src:EPA_CODE::varchar AS EPA_CODE, 
                        src:EPA_COMMENT::varchar AS EPA_COMMENT, 
                        src:EPA_COMPLEVEL::varchar AS EPA_COMPLEVEL, 
                        src:EPA_ENTITY::varchar AS EPA_ENTITY, 
                        src:EPA_LASTSAVED::datetime AS EPA_LASTSAVED, 
                        src:EPA_PART::varchar AS EPA_PART, 
                        src:EPA_PARTLOCATION::varchar AS EPA_PARTLOCATION, 
                        src:EPA_PART_ORG::varchar AS EPA_PART_ORG, 
                        src:EPA_PK::varchar AS EPA_PK, 
                        src:EPA_QTY::numeric(38, 10) AS EPA_QTY, 
                        src:EPA_RENTITY::varchar AS EPA_RENTITY, 
                        src:EPA_RTYPE::varchar AS EPA_RTYPE, 
                        src:EPA_SYSLEVEL::varchar AS EPA_SYSLEVEL, 
                        src:EPA_TYPE::varchar AS EPA_TYPE, 
                        src:EPA_UOM::varchar AS EPA_UOM, 
                        src:EPA_UPDATECOUNT::numeric(38, 10) AS EPA_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:EPA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:EPA_PK  ORDER BY 
            src:EPA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ENTITYPARTS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EPA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EPA_QTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EPA_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EPA_LASTSAVED), '1900-01-01')) is not null