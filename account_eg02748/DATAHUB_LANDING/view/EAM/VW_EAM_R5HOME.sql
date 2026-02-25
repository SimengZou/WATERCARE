CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5HOME AS SELECT
                        src:HOM_CODE::varchar AS HOM_CODE, 
                        src:HOM_CURVALUE::numeric(38, 10) AS HOM_CURVALUE, 
                        src:HOM_DESC::varchar AS HOM_DESC, 
                        src:HOM_ERROR::varchar AS HOM_ERROR, 
                        src:HOM_EWSDATASPYID::numeric(38, 10) AS HOM_EWSDATASPYID, 
                        src:HOM_EWSFUNCTION::varchar AS HOM_EWSFUNCTION, 
                        src:HOM_FUNCTION::varchar AS HOM_FUNCTION, 
                        src:HOM_GEN::varchar AS HOM_GEN, 
                        src:HOM_GROUPBYTEXT::varchar AS HOM_GROUPBYTEXT, 
                        src:HOM_HISTORY::varchar AS HOM_HISTORY, 
                        src:HOM_KPITYPE::varchar AS HOM_KPITYPE, 
                        src:HOM_LASTSAVED::datetime AS HOM_LASTSAVED, 
                        src:HOM_MAX::numeric(38, 10) AS HOM_MAX, 
                        src:HOM_MIN::numeric(38, 10) AS HOM_MIN, 
                        src:HOM_NORMSCORE::numeric(38, 10) AS HOM_NORMSCORE, 
                        src:HOM_NOTUSED::varchar AS HOM_NOTUSED, 
                        src:HOM_NXTUPDATE::datetime AS HOM_NXTUPDATE, 
                        src:HOM_OPERATOR::varchar AS HOM_OPERATOR, 
                        src:HOM_PARENT::varchar AS HOM_PARENT, 
                        src:HOM_RADIUSPERCENTAGE::numeric(38, 10) AS HOM_RADIUSPERCENTAGE, 
                        src:HOM_ROLLUPTYPE::varchar AS HOM_ROLLUPTYPE, 
                        src:HOM_SQLCODE::varchar AS HOM_SQLCODE, 
                        src:HOM_TYPE::varchar AS HOM_TYPE, 
                        src:HOM_UDFCHAR01::varchar AS HOM_UDFCHAR01, 
                        src:HOM_UDFCHAR02::varchar AS HOM_UDFCHAR02, 
                        src:HOM_UDFCHAR03::varchar AS HOM_UDFCHAR03, 
                        src:HOM_UDFCHAR04::varchar AS HOM_UDFCHAR04, 
                        src:HOM_UDFCHAR05::varchar AS HOM_UDFCHAR05, 
                        src:HOM_UDFCHAR06::varchar AS HOM_UDFCHAR06, 
                        src:HOM_UDFCHAR07::varchar AS HOM_UDFCHAR07, 
                        src:HOM_UDFCHAR08::varchar AS HOM_UDFCHAR08, 
                        src:HOM_UDFCHAR09::varchar AS HOM_UDFCHAR09, 
                        src:HOM_UDFCHAR10::varchar AS HOM_UDFCHAR10, 
                        src:HOM_UDFCHKBOX01::varchar AS HOM_UDFCHKBOX01, 
                        src:HOM_UDFCHKBOX02::varchar AS HOM_UDFCHKBOX02, 
                        src:HOM_UDFCHKBOX03::varchar AS HOM_UDFCHKBOX03, 
                        src:HOM_UDFCHKBOX04::varchar AS HOM_UDFCHKBOX04, 
                        src:HOM_UDFCHKBOX05::varchar AS HOM_UDFCHKBOX05, 
                        src:HOM_UDFDATE01::datetime AS HOM_UDFDATE01, 
                        src:HOM_UDFDATE02::datetime AS HOM_UDFDATE02, 
                        src:HOM_UDFDATE03::datetime AS HOM_UDFDATE03, 
                        src:HOM_UDFDATE04::datetime AS HOM_UDFDATE04, 
                        src:HOM_UDFDATE05::datetime AS HOM_UDFDATE05, 
                        src:HOM_UDFNUM01::numeric(38, 10) AS HOM_UDFNUM01, 
                        src:HOM_UDFNUM02::numeric(38, 10) AS HOM_UDFNUM02, 
                        src:HOM_UDFNUM03::numeric(38, 10) AS HOM_UDFNUM03, 
                        src:HOM_UDFNUM04::numeric(38, 10) AS HOM_UDFNUM04, 
                        src:HOM_UDFNUM05::numeric(38, 10) AS HOM_UDFNUM05, 
                        src:HOM_UDFNUM06::numeric(38, 10) AS HOM_UDFNUM06, 
                        src:HOM_UDFNUM07::numeric(38, 10) AS HOM_UDFNUM07, 
                        src:HOM_UDFNUM08::numeric(38, 10) AS HOM_UDFNUM08, 
                        src:HOM_UDFNUM09::numeric(38, 10) AS HOM_UDFNUM09, 
                        src:HOM_UDFNUM10::numeric(38, 10) AS HOM_UDFNUM10, 
                        src:HOM_UOM::varchar AS HOM_UOM, 
                        src:HOM_UPDATE::datetime AS HOM_UPDATE, 
                        src:HOM_UPDATECOUNT::numeric(38, 10) AS HOM_UPDATECOUNT, 
                        src:HOM_UPDFREQ::numeric(38, 10) AS HOM_UPDFREQ, 
                        src:HOM_VALUE::numeric(38, 10) AS HOM_VALUE, 
                        src:HOM_WHERECLAUSE::varchar AS HOM_WHERECLAUSE, 
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
    
                        
                src:HOM_CODE,
                src:HOM_TYPE,
            src:HOM_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:HOM_CODE,
                src:HOM_TYPE  ORDER BY 
            src:HOM_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5HOME as strm))