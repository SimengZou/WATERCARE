CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5WORKPACKAGES AS SELECT
                        src:WPK_CHANGED::varchar AS WPK_CHANGED, 
                        src:WPK_CLASS::varchar AS WPK_CLASS, 
                        src:WPK_CLASS_ORG::varchar AS WPK_CLASS_ORG, 
                        src:WPK_CODE::varchar AS WPK_CODE, 
                        src:WPK_DESC::varchar AS WPK_DESC, 
                        src:WPK_DUEDATE::datetime AS WPK_DUEDATE, 
                        src:WPK_DURATION::numeric(38, 10) AS WPK_DURATION, 
                        src:WPK_ESTWORKLOAD::numeric(38, 10) AS WPK_ESTWORKLOAD, 
                        src:WPK_FREQ::numeric(38, 10) AS WPK_FREQ, 
                        src:WPK_JOBCLASS::varchar AS WPK_JOBCLASS, 
                        src:WPK_JOBCLASS_ORG::varchar AS WPK_JOBCLASS_ORG, 
                        src:WPK_JOBTYPE::varchar AS WPK_JOBTYPE, 
                        src:WPK_LASTEVENT::varchar AS WPK_LASTEVENT, 
                        src:WPK_LASTSAVED::datetime AS WPK_LASTSAVED, 
                        src:WPK_MRC::varchar AS WPK_MRC, 
                        src:WPK_OBJECT::varchar AS WPK_OBJECT, 
                        src:WPK_OBJECT_ORG::varchar AS WPK_OBJECT_ORG, 
                        src:WPK_ORG::varchar AS WPK_ORG, 
                        src:WPK_PERFORMONDAY::numeric(38, 10) AS WPK_PERFORMONDAY, 
                        src:WPK_PERFORMONWEEK::varchar AS WPK_PERFORMONWEEK, 
                        src:WPK_PERIODUOM::varchar AS WPK_PERIODUOM, 
                        src:WPK_PERSONS::numeric(38, 10) AS WPK_PERSONS, 
                        src:WPK_PPMTYPE::varchar AS WPK_PPMTYPE, 
                        src:WPK_STATUS::varchar AS WPK_STATUS, 
                        src:WPK_TRADE::varchar AS WPK_TRADE, 
                        src:WPK_UPDATECOUNT::numeric(38, 10) AS WPK_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:WPK_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:WPK_CODE  ORDER BY 
            src:WPK_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5WORKPACKAGES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WPK_DUEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WPK_DURATION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WPK_ESTWORKLOAD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WPK_FREQ), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WPK_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WPK_PERFORMONDAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WPK_PERSONS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WPK_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WPK_LASTSAVED), '1900-01-01')) is not null