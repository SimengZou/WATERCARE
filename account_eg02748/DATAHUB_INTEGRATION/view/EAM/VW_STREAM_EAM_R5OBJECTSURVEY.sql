CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5OBJECTSURVEY AS SELECT
                        src:OBS_ANSWERPK::varchar AS OBS_ANSWERPK, 
                        src:OBS_CALCULATEDANSWER::varchar AS OBS_CALCULATEDANSWER, 
                        src:OBS_CALCULATEDVALUE::numeric(38, 10) AS OBS_CALCULATEDVALUE, 
                        src:OBS_DATEEFFECTIVE::datetime AS OBS_DATEEFFECTIVE, 
                        src:OBS_DATELASTCALCULATED::datetime AS OBS_DATELASTCALCULATED, 
                        src:OBS_LASTSAVED::datetime AS OBS_LASTSAVED, 
                        src:OBS_LEVELPK::varchar AS OBS_LEVELPK, 
                        src:OBS_OBJECT::varchar AS OBS_OBJECT, 
                        src:OBS_OBJECT_ORG::varchar AS OBS_OBJECT_ORG, 
                        src:OBS_OPERATORCHECKLIST::varchar AS OBS_OPERATORCHECKLIST, 
                        src:OBS_TYPE::varchar AS OBS_TYPE, 
                        src:OBS_UPDATECOUNT::numeric(38, 10) AS OBS_UPDATECOUNT, 
                        src:OBS_VALUE::numeric(38, 10) AS OBS_VALUE, 
                        src:OBS_WORKORDER::varchar AS OBS_WORKORDER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:OBS_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:OBS_LEVELPK,
                src:OBS_OBJECT,
                src:OBS_OBJECT_ORG,
                src:OBS_TYPE  ORDER BY 
            src:OBS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5OBJECTSURVEY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBS_CALCULATEDVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBS_DATEEFFECTIVE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBS_DATELASTCALCULATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBS_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBS_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OBS_VALUE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:OBS_LASTSAVED), '1900-01-01')) is not null