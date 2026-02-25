CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_EAM_R5EQUIPMENTRANKINGHISTORY_ERROR AS SELECT src, 'EAM_R5EQUIPMENTRANKINGHISTORY' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_AVAILABLECAPACITY), '0'), 38, 10) is null and 
                    src:ERH_AVAILABLECAPACITY is not null) THEN
                    'ERH_AVAILABLECAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_AVAILABLECAPACITY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CAPACITYRATING), '0'), 38, 10) is null and 
                    src:ERH_CAPACITYRATING is not null) THEN
                    'ERH_CAPACITYRATING contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CAPACITYRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDITIONRATING), '0'), 38, 10) is null and 
                    src:ERH_CONDITIONRATING is not null) THEN
                    'ERH_CONDITIONRATING contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CONDITIONRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDSCOREEND), '0'), 38, 10) is null and 
                    src:ERH_CONDSCOREEND is not null) THEN
                    'ERH_CONDSCOREEND contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CONDSCOREEND) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDSCORESTART), '0'), 38, 10) is null and 
                    src:ERH_CONDSCORESTART is not null) THEN
                    'ERH_CONDSCORESTART contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CONDSCORESTART) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDSCORETHRESHOLD), '0'), 38, 10) is null and 
                    src:ERH_CONDSCORETHRESHOLD is not null) THEN
                    'ERH_CONDSCORETHRESHOLD contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CONDSCORETHRESHOLD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_CORRCONDITIONDATE), '1900-01-01')) is null and 
                    src:ERH_CORRCONDITIONDATE is not null) THEN
                    'ERH_CORRCONDITIONDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_CORRCONDITIONDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CORRCONDITIONSCORE), '0'), 38, 10) is null and 
                    src:ERH_CORRCONDITIONSCORE is not null) THEN
                    'ERH_CORRCONDITIONSCORE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CORRCONDITIONSCORE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CORRCONDITIONUSAGE), '0'), 38, 10) is null and 
                    src:ERH_CORRCONDITIONUSAGE is not null) THEN
                    'ERH_CORRCONDITIONUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_CORRCONDITIONUSAGE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_CREATED), '1900-01-01')) is null and 
                    src:ERH_CREATED is not null) THEN
                    'ERH_CREATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_CREATED) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_DATE), '1900-01-01')) is null and 
                    src:ERH_DATE is not null) THEN
                    'ERH_DATE contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_DATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_DESIREDCAPACITY), '0'), 38, 10) is null and 
                    src:ERH_DESIREDCAPACITY is not null) THEN
                    'ERH_DESIREDCAPACITY contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_DESIREDCAPACITY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_ENDOFUSEFULLIFE), '1900-01-01')) is null and 
                    src:ERH_ENDOFUSEFULLIFE is not null) THEN
                    'ERH_ENDOFUSEFULLIFE contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_ENDOFUSEFULLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_FROMPOINT), '0'), 38, 10) is null and 
                    src:ERH_FROMPOINT is not null) THEN
                    'ERH_FROMPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_FROMPOINT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_LASTSAVED), '1900-01-01')) is null and 
                    src:ERH_LASTSAVED is not null) THEN
                    'ERH_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_LASTSAVED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTBFDAYS), '0'), 38, 10) is null and 
                    src:ERH_MTBFDAYS is not null) THEN
                    'ERH_MTBFDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_MTBFDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTBFRATING), '0'), 38, 10) is null and 
                    src:ERH_MTBFRATING is not null) THEN
                    'ERH_MTBFRATING contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_MTBFRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTTRHRS), '0'), 38, 10) is null and 
                    src:ERH_MTTRHRS is not null) THEN
                    'ERH_MTTRHRS contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_MTTRHRS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTTRRATING), '0'), 38, 10) is null and 
                    src:ERH_MTTRRATING is not null) THEN
                    'ERH_MTTRRATING contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_MTTRRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MUBF), '0'), 38, 10) is null and 
                    src:ERH_MUBF is not null) THEN
                    'ERH_MUBF contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_MUBF) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MUBFRATING), '0'), 38, 10) is null and 
                    src:ERH_MUBFRATING is not null) THEN
                    'ERH_MUBFRATING contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_MUBFRATING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_NUMBEROFFAILURES), '0'), 38, 10) is null and 
                    src:ERH_NUMBEROFFAILURES is not null) THEN
                    'ERH_NUMBEROFFAILURES contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_NUMBEROFFAILURES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_PERFORMANCE), '0'), 38, 10) is null and 
                    src:ERH_PERFORMANCE is not null) THEN
                    'ERH_PERFORMANCE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_PERFORMANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_RANKINGREVISION), '0'), 38, 10) is null and 
                    src:ERH_RANKINGREVISION is not null) THEN
                    'ERH_RANKINGREVISION contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_RANKINGREVISION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_SCORE), '0'), 38, 10) is null and 
                    src:ERH_SCORE is not null) THEN
                    'ERH_SCORE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_SCORE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_SERVICELIFE), '0'), 38, 10) is null and 
                    src:ERH_SERVICELIFE is not null) THEN
                    'ERH_SERVICELIFE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_SERVICELIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_SERVICELIFEUSAGE), '0'), 38, 10) is null and 
                    src:ERH_SERVICELIFEUSAGE is not null) THEN
                    'ERH_SERVICELIFEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_SERVICELIFEUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_TOPOINT), '0'), 38, 10) is null and 
                    src:ERH_TOPOINT is not null) THEN
                    'ERH_TOPOINT contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_TOPOINT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE01), '1900-01-01')) is null and 
                    src:ERH_UDFDATE01 is not null) THEN
                    'ERH_UDFDATE01 contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_UDFDATE01) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE02), '1900-01-01')) is null and 
                    src:ERH_UDFDATE02 is not null) THEN
                    'ERH_UDFDATE02 contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_UDFDATE02) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE03), '1900-01-01')) is null and 
                    src:ERH_UDFDATE03 is not null) THEN
                    'ERH_UDFDATE03 contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_UDFDATE03) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE04), '1900-01-01')) is null and 
                    src:ERH_UDFDATE04 is not null) THEN
                    'ERH_UDFDATE04 contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_UDFDATE04) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE05), '1900-01-01')) is null and 
                    src:ERH_UDFDATE05 is not null) THEN
                    'ERH_UDFDATE05 contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_UDFDATE05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM01), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM01 is not null) THEN
                    'ERH_UDFNUM01 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_UDFNUM01) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM02), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM02 is not null) THEN
                    'ERH_UDFNUM02 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_UDFNUM02) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM03), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM03 is not null) THEN
                    'ERH_UDFNUM03 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_UDFNUM03) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM04), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM04 is not null) THEN
                    'ERH_UDFNUM04 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_UDFNUM04) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM05), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM05 is not null) THEN
                    'ERH_UDFNUM05 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_UDFNUM05) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ERH_UPDATECOUNT is not null) THEN
                    'ERH_UPDATECOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_UPDATECOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UPDATED), '1900-01-01')) is null and 
                    src:ERH_UPDATED is not null) THEN
                    'ERH_UPDATED contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_UPDATED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_USAGE), '0'), 38, 10) is null and 
                    src:ERH_USAGE is not null) THEN
                    'ERH_USAGE contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_USAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING1), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING1 is not null) THEN
                    'ERH_VARIABLERATING1 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERATING1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING2), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING2 is not null) THEN
                    'ERH_VARIABLERATING2 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERATING2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING3), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING3 is not null) THEN
                    'ERH_VARIABLERATING3 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERATING3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING4), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING4 is not null) THEN
                    'ERH_VARIABLERATING4 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERATING4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING5), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING5 is not null) THEN
                    'ERH_VARIABLERATING5 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERATING5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING6), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING6 is not null) THEN
                    'ERH_VARIABLERATING6 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERATING6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT1), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT1 is not null) THEN
                    'ERH_VARIABLERESULT1 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERESULT1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT2), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT2 is not null) THEN
                    'ERH_VARIABLERESULT2 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERESULT2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT3), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT3 is not null) THEN
                    'ERH_VARIABLERESULT3 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERESULT3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT4), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT4 is not null) THEN
                    'ERH_VARIABLERESULT4 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERESULT4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT5), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT5 is not null) THEN
                    'ERH_VARIABLERESULT5 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERESULT5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT6), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT6 is not null) THEN
                    'ERH_VARIABLERESULT6 contains a non-numeric value : \'' || AS_VARCHAR(src:ERH_VARIABLERESULT6) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_LASTSAVED), '1900-01-01')) is null and 
                src:ERH_LASTSAVED is not null) THEN
                'ERH_LASTSAVED contains a non-timestamp value : \'' || AS_VARCHAR(src:ERH_LASTSAVED) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:ERH_PK  ORDER BY 
            src:ERH_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EQUIPMENTRANKINGHISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_AVAILABLECAPACITY), '0'), 38, 10) is null and 
                    src:ERH_AVAILABLECAPACITY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CAPACITYRATING), '0'), 38, 10) is null and 
                    src:ERH_CAPACITYRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDITIONRATING), '0'), 38, 10) is null and 
                    src:ERH_CONDITIONRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDSCOREEND), '0'), 38, 10) is null and 
                    src:ERH_CONDSCOREEND is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDSCORESTART), '0'), 38, 10) is null and 
                    src:ERH_CONDSCORESTART is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CONDSCORETHRESHOLD), '0'), 38, 10) is null and 
                    src:ERH_CONDSCORETHRESHOLD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_CORRCONDITIONDATE), '1900-01-01')) is null and 
                    src:ERH_CORRCONDITIONDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CORRCONDITIONSCORE), '0'), 38, 10) is null and 
                    src:ERH_CORRCONDITIONSCORE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_CORRCONDITIONUSAGE), '0'), 38, 10) is null and 
                    src:ERH_CORRCONDITIONUSAGE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_CREATED), '1900-01-01')) is null and 
                    src:ERH_CREATED is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_DATE), '1900-01-01')) is null and 
                    src:ERH_DATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_DESIREDCAPACITY), '0'), 38, 10) is null and 
                    src:ERH_DESIREDCAPACITY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_ENDOFUSEFULLIFE), '1900-01-01')) is null and 
                    src:ERH_ENDOFUSEFULLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_FROMPOINT), '0'), 38, 10) is null and 
                    src:ERH_FROMPOINT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_LASTSAVED), '1900-01-01')) is null and 
                    src:ERH_LASTSAVED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTBFDAYS), '0'), 38, 10) is null and 
                    src:ERH_MTBFDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTBFRATING), '0'), 38, 10) is null and 
                    src:ERH_MTBFRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTTRHRS), '0'), 38, 10) is null and 
                    src:ERH_MTTRHRS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MTTRRATING), '0'), 38, 10) is null and 
                    src:ERH_MTTRRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MUBF), '0'), 38, 10) is null and 
                    src:ERH_MUBF is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_MUBFRATING), '0'), 38, 10) is null and 
                    src:ERH_MUBFRATING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_NUMBEROFFAILURES), '0'), 38, 10) is null and 
                    src:ERH_NUMBEROFFAILURES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_PERFORMANCE), '0'), 38, 10) is null and 
                    src:ERH_PERFORMANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_RANKINGREVISION), '0'), 38, 10) is null and 
                    src:ERH_RANKINGREVISION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_SCORE), '0'), 38, 10) is null and 
                    src:ERH_SCORE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_SERVICELIFE), '0'), 38, 10) is null and 
                    src:ERH_SERVICELIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_SERVICELIFEUSAGE), '0'), 38, 10) is null and 
                    src:ERH_SERVICELIFEUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_TOPOINT), '0'), 38, 10) is null and 
                    src:ERH_TOPOINT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE01), '1900-01-01')) is null and 
                    src:ERH_UDFDATE01 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE02), '1900-01-01')) is null and 
                    src:ERH_UDFDATE02 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE03), '1900-01-01')) is null and 
                    src:ERH_UDFDATE03 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE04), '1900-01-01')) is null and 
                    src:ERH_UDFDATE04 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UDFDATE05), '1900-01-01')) is null and 
                    src:ERH_UDFDATE05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM01), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM01 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM02), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM02 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM03), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM03 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM04), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM04 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UDFNUM05), '0'), 38, 10) is null and 
                    src:ERH_UDFNUM05 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_UPDATECOUNT), '0'), 38, 10) is null and 
                    src:ERH_UPDATECOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_UPDATED), '1900-01-01')) is null and 
                    src:ERH_UPDATED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_USAGE), '0'), 38, 10) is null and 
                    src:ERH_USAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING1), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING2), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING3), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING4), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING5), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERATING6), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERATING6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT1), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT2), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT3), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT4), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT5), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ERH_VARIABLERESULT6), '0'), 38, 10) is null and 
                    src:ERH_VARIABLERESULT6 is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ERH_LASTSAVED), '1900-01-01')) is null and 
                src:ERH_LASTSAVED is not null)