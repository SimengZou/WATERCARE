CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MAILEVENTS AS SELECT
                        src:MAE_ATTRIBPK::varchar AS MAE_ATTRIBPK, 
                        src:MAE_CODE::varchar AS MAE_CODE, 
                        src:MAE_DATE::datetime AS MAE_DATE, 
                        src:MAE_DOCPK::varchar AS MAE_DOCPK, 
                        src:MAE_DOCRENTITY::varchar AS MAE_DOCRENTITY, 
                        src:MAE_EMAILERRCOUNT::numeric(38, 10) AS MAE_EMAILERRCOUNT, 
                        src:MAE_EMAILRECIPIENT::varchar AS MAE_EMAILRECIPIENT, 
                        src:MAE_ERROR::varchar AS MAE_ERROR, 
                        src:MAE_EWSURL::varchar AS MAE_EWSURL, 
                        src:MAE_LASTSAVED::datetime AS MAE_LASTSAVED, 
                        src:MAE_PARAM1::varchar AS MAE_PARAM1, 
                        src:MAE_PARAM10::varchar AS MAE_PARAM10, 
                        src:MAE_PARAM11::varchar AS MAE_PARAM11, 
                        src:MAE_PARAM12::varchar AS MAE_PARAM12, 
                        src:MAE_PARAM13::varchar AS MAE_PARAM13, 
                        src:MAE_PARAM14::varchar AS MAE_PARAM14, 
                        src:MAE_PARAM15::varchar AS MAE_PARAM15, 
                        src:MAE_PARAM2::varchar AS MAE_PARAM2, 
                        src:MAE_PARAM3::varchar AS MAE_PARAM3, 
                        src:MAE_PARAM4::varchar AS MAE_PARAM4, 
                        src:MAE_PARAM5::varchar AS MAE_PARAM5, 
                        src:MAE_PARAM6::varchar AS MAE_PARAM6, 
                        src:MAE_PARAM7::varchar AS MAE_PARAM7, 
                        src:MAE_PARAM8::varchar AS MAE_PARAM8, 
                        src:MAE_PARAM9::varchar AS MAE_PARAM9, 
                        src:MAE_PTFERRCOUNT::numeric(38, 10) AS MAE_PTFERRCOUNT, 
                        src:MAE_PTFERROR::varchar AS MAE_PTFERROR, 
                        src:MAE_PTFSEND::varchar AS MAE_PTFSEND, 
                        src:MAE_RSTATUS::varchar AS MAE_RSTATUS, 
                        src:MAE_SEND::varchar AS MAE_SEND, 
                        src:MAE_TEMPLATE::varchar AS MAE_TEMPLATE, 
                        src:MAE_UPDATECOUNT::numeric(38, 10) AS MAE_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:MAE_CODE,
            src:MAE_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MAE_CODE  ORDER BY 
            src:MAE_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MAILEVENTS as strm))