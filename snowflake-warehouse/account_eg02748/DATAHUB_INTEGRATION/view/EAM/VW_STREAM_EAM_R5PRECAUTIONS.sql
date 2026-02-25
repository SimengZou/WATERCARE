CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PRECAUTIONS AS SELECT
                        src:PCA_APPROVED::datetime AS PCA_APPROVED, 
                        src:PCA_APPROVEDBY::varchar AS PCA_APPROVEDBY, 
                        src:PCA_CLASS::varchar AS PCA_CLASS, 
                        src:PCA_CLASS_ORG::varchar AS PCA_CLASS_ORG, 
                        src:PCA_CODE::varchar AS PCA_CODE, 
                        src:PCA_CREATED::datetime AS PCA_CREATED, 
                        src:PCA_CREATEDBY::varchar AS PCA_CREATEDBY, 
                        src:PCA_DESC::varchar AS PCA_DESC, 
                        src:PCA_LASTSAVED::datetime AS PCA_LASTSAVED, 
                        src:PCA_NOTUSED::varchar AS PCA_NOTUSED, 
                        src:PCA_ORG::varchar AS PCA_ORG, 
                        src:PCA_REQUESTED::datetime AS PCA_REQUESTED, 
                        src:PCA_REQUESTEDBY::varchar AS PCA_REQUESTEDBY, 
                        src:PCA_RESPONSIBILITY::varchar AS PCA_RESPONSIBILITY, 
                        src:PCA_REVIEWREQUIRED::datetime AS PCA_REVIEWREQUIRED, 
                        src:PCA_REVISION::numeric(38, 10) AS PCA_REVISION, 
                        src:PCA_REVISIONREASON::varchar AS PCA_REVISIONREASON, 
                        src:PCA_RSTATUS::varchar AS PCA_RSTATUS, 
                        src:PCA_STATUS::varchar AS PCA_STATUS, 
                        src:PCA_TIMING::varchar AS PCA_TIMING, 
                        src:PCA_UDFCHAR01::varchar AS PCA_UDFCHAR01, 
                        src:PCA_UDFCHAR02::varchar AS PCA_UDFCHAR02, 
                        src:PCA_UDFCHAR03::varchar AS PCA_UDFCHAR03, 
                        src:PCA_UDFCHAR04::varchar AS PCA_UDFCHAR04, 
                        src:PCA_UDFCHAR05::varchar AS PCA_UDFCHAR05, 
                        src:PCA_UDFCHAR06::varchar AS PCA_UDFCHAR06, 
                        src:PCA_UDFCHAR07::varchar AS PCA_UDFCHAR07, 
                        src:PCA_UDFCHAR08::varchar AS PCA_UDFCHAR08, 
                        src:PCA_UDFCHAR09::varchar AS PCA_UDFCHAR09, 
                        src:PCA_UDFCHAR10::varchar AS PCA_UDFCHAR10, 
                        src:PCA_UDFCHAR11::varchar AS PCA_UDFCHAR11, 
                        src:PCA_UDFCHAR12::varchar AS PCA_UDFCHAR12, 
                        src:PCA_UDFCHAR13::varchar AS PCA_UDFCHAR13, 
                        src:PCA_UDFCHAR14::varchar AS PCA_UDFCHAR14, 
                        src:PCA_UDFCHAR15::varchar AS PCA_UDFCHAR15, 
                        src:PCA_UDFCHAR16::varchar AS PCA_UDFCHAR16, 
                        src:PCA_UDFCHAR17::varchar AS PCA_UDFCHAR17, 
                        src:PCA_UDFCHAR18::varchar AS PCA_UDFCHAR18, 
                        src:PCA_UDFCHAR19::varchar AS PCA_UDFCHAR19, 
                        src:PCA_UDFCHAR20::varchar AS PCA_UDFCHAR20, 
                        src:PCA_UDFCHAR21::varchar AS PCA_UDFCHAR21, 
                        src:PCA_UDFCHAR22::varchar AS PCA_UDFCHAR22, 
                        src:PCA_UDFCHAR23::varchar AS PCA_UDFCHAR23, 
                        src:PCA_UDFCHAR24::varchar AS PCA_UDFCHAR24, 
                        src:PCA_UDFCHAR25::varchar AS PCA_UDFCHAR25, 
                        src:PCA_UDFCHAR26::varchar AS PCA_UDFCHAR26, 
                        src:PCA_UDFCHAR27::varchar AS PCA_UDFCHAR27, 
                        src:PCA_UDFCHAR28::varchar AS PCA_UDFCHAR28, 
                        src:PCA_UDFCHAR29::varchar AS PCA_UDFCHAR29, 
                        src:PCA_UDFCHAR30::varchar AS PCA_UDFCHAR30, 
                        src:PCA_UDFCHKBOX01::varchar AS PCA_UDFCHKBOX01, 
                        src:PCA_UDFCHKBOX02::varchar AS PCA_UDFCHKBOX02, 
                        src:PCA_UDFCHKBOX03::varchar AS PCA_UDFCHKBOX03, 
                        src:PCA_UDFCHKBOX04::varchar AS PCA_UDFCHKBOX04, 
                        src:PCA_UDFCHKBOX05::varchar AS PCA_UDFCHKBOX05, 
                        src:PCA_UDFDATE01::datetime AS PCA_UDFDATE01, 
                        src:PCA_UDFDATE02::datetime AS PCA_UDFDATE02, 
                        src:PCA_UDFDATE03::datetime AS PCA_UDFDATE03, 
                        src:PCA_UDFDATE04::datetime AS PCA_UDFDATE04, 
                        src:PCA_UDFDATE05::datetime AS PCA_UDFDATE05, 
                        src:PCA_UDFNUM01::numeric(38, 10) AS PCA_UDFNUM01, 
                        src:PCA_UDFNUM02::numeric(38, 10) AS PCA_UDFNUM02, 
                        src:PCA_UDFNUM03::numeric(38, 10) AS PCA_UDFNUM03, 
                        src:PCA_UDFNUM04::numeric(38, 10) AS PCA_UDFNUM04, 
                        src:PCA_UDFNUM05::numeric(38, 10) AS PCA_UDFNUM05, 
                        src:PCA_UPDATECOUNT::numeric(38, 10) AS PCA_UPDATECOUNT, 
                        src:PCA_UPDATED::datetime AS PCA_UPDATED, 
                        src:PCA_UPDATEDBY::varchar AS PCA_UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PCA_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PCA_CODE,
                src:PCA_ORG,
                src:PCA_REVISION  ORDER BY 
            src:PCA_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PRECAUTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_APPROVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_REQUESTED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_REVIEWREQUIRED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_REVISION), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_UDFDATE01), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_UDFDATE02), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_UDFDATE03), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_UDFDATE04), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_UDFDATE05), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_UDFNUM01), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_UDFNUM02), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_UDFNUM03), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_UDFNUM04), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_UDFNUM05), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PCA_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PCA_LASTSAVED), '1900-01-01')) is not null