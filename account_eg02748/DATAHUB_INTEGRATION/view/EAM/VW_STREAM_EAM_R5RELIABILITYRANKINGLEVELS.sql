CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGLEVELS AS SELECT
                        src:RRL_ALLOWOPERATORCHECKLIST::varchar AS RRL_ALLOWOPERATORCHECKLIST, 
                        src:RRL_ASPECT::varchar AS RRL_ASPECT, 
                        src:RRL_CALCULATED::varchar AS RRL_CALCULATED, 
                        src:RRL_CHECKLISTTYPE::varchar AS RRL_CHECKLISTTYPE, 
                        src:RRL_CODE::varchar AS RRL_CODE, 
                        src:RRL_DESC::varchar AS RRL_DESC, 
                        src:RRL_FORMULA::varchar AS RRL_FORMULA, 
                        src:RRL_INTEGER::varchar AS RRL_INTEGER, 
                        src:RRL_LASTSAVED::datetime AS RRL_LASTSAVED, 
                        src:RRL_MAX::numeric(38, 10) AS RRL_MAX, 
                        src:RRL_MIN::numeric(38, 10) AS RRL_MIN, 
                        src:RRL_NUMERIC::varchar AS RRL_NUMERIC, 
                        src:RRL_ORG::varchar AS RRL_ORG, 
                        src:RRL_PARENT::varchar AS RRL_PARENT, 
                        src:RRL_PK::varchar AS RRL_PK, 
                        src:RRL_QUERYCODE::varchar AS RRL_QUERYCODE, 
                        src:RRL_QUESTION::varchar AS RRL_QUESTION, 
                        src:RRL_QUESTIONLEVEL::varchar AS RRL_QUESTIONLEVEL, 
                        src:RRL_RELIABILITYRANKING::varchar AS RRL_RELIABILITYRANKING, 
                        src:RRL_REVISION::numeric(38, 10) AS RRL_REVISION, 
                        src:RRL_SEQ::numeric(38, 10) AS RRL_SEQ, 
                        src:RRL_UPDATECOUNT::numeric(38, 10) AS RRL_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RRL_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RRL_PK  ORDER BY 
            src:RRL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5RELIABILITYRANKINGLEVELS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRL_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRL_MAX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRL_MIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRL_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRL_SEQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RRL_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RRL_LASTSAVED), '1900-01-01')) is not null