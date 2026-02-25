CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHWMD215_ERROR AS SELECT src, 'LN_WHWMD215' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_item_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_item_ref_compnr is not null) THEN
                    'cwar_item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:hstd), '1900-01-01')) is null and 
                    src:hstd is not null) THEN
                    'hstd contains a non-timestamp value : \'' || AS_VARCHAR(src:hstd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) THEN
                    'item_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:item_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lsid), '1900-01-01')) is null and 
                    src:lsid is not null) THEN
                    'lsid contains a non-timestamp value : \'' || AS_VARCHAR(src:lsid) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) THEN
                    'ltdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ltdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qall), '0'), 38, 10) is null and 
                    src:qall is not null) THEN
                    'qall contains a non-numeric value : \'' || AS_VARCHAR(src:qall) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qblk), '0'), 38, 10) is null and 
                    src:qblk is not null) THEN
                    'qblk contains a non-numeric value : \'' || AS_VARCHAR(src:qblk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbpl), '0'), 38, 10) is null and 
                    src:qbpl is not null) THEN
                    'qbpl contains a non-numeric value : \'' || AS_VARCHAR(src:qbpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcal), '0'), 38, 10) is null and 
                    src:qcal is not null) THEN
                    'qcal contains a non-numeric value : \'' || AS_VARCHAR(src:qcal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qchd), '0'), 38, 10) is null and 
                    src:qchd is not null) THEN
                    'qchd contains a non-numeric value : \'' || AS_VARCHAR(src:qchd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcis), '0'), 38, 10) is null and 
                    src:qcis is not null) THEN
                    'qcis contains a non-numeric value : \'' || AS_VARCHAR(src:qcis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcit), '0'), 38, 10) is null and 
                    src:qcit is not null) THEN
                    'qcit contains a non-numeric value : \'' || AS_VARCHAR(src:qcit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcom), '0'), 38, 10) is null and 
                    src:qcom is not null) THEN
                    'qcom contains a non-numeric value : \'' || AS_VARCHAR(src:qcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcor), '0'), 38, 10) is null and 
                    src:qcor is not null) THEN
                    'qcor contains a non-numeric value : \'' || AS_VARCHAR(src:qcor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcpr), '0'), 38, 10) is null and 
                    src:qcpr is not null) THEN
                    'qcpr contains a non-numeric value : \'' || AS_VARCHAR(src:qcpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcrj), '0'), 38, 10) is null and 
                    src:qcrj is not null) THEN
                    'qcrj contains a non-numeric value : \'' || AS_VARCHAR(src:qcrj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qgit), '0'), 38, 10) is null and 
                    src:qgit is not null) THEN
                    'qgit contains a non-numeric value : \'' || AS_VARCHAR(src:qgit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhib), '0'), 38, 10) is null and 
                    src:qhib is not null) THEN
                    'qhib contains a non-numeric value : \'' || AS_VARCHAR(src:qhib) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd), '0'), 38, 10) is null and 
                    src:qhnd is not null) THEN
                    'qhnd contains a non-numeric value : \'' || AS_VARCHAR(src:qhnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhrj), '0'), 38, 10) is null and 
                    src:qhrj is not null) THEN
                    'qhrj contains a non-numeric value : \'' || AS_VARCHAR(src:qhrj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qint), '0'), 38, 10) is null and 
                    src:qint is not null) THEN
                    'qint contains a non-numeric value : \'' || AS_VARCHAR(src:qint) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qlal), '0'), 38, 10) is null and 
                    src:qlal is not null) THEN
                    'qlal contains a non-numeric value : \'' || AS_VARCHAR(src:qlal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnal), '0'), 38, 10) is null and 
                    src:qnal is not null) THEN
                    'qnal contains a non-numeric value : \'' || AS_VARCHAR(src:qnal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnbl), '0'), 38, 10) is null and 
                    src:qnbl is not null) THEN
                    'qnbl contains a non-numeric value : \'' || AS_VARCHAR(src:qnbl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnbp), '0'), 38, 10) is null and 
                    src:qnbp is not null) THEN
                    'qnbp contains a non-numeric value : \'' || AS_VARCHAR(src:qnbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnhd), '0'), 38, 10) is null and 
                    src:qnhd is not null) THEN
                    'qnhd contains a non-numeric value : \'' || AS_VARCHAR(src:qnhd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnit), '0'), 38, 10) is null and 
                    src:qnit is not null) THEN
                    'qnit contains a non-numeric value : \'' || AS_VARCHAR(src:qnit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnor), '0'), 38, 10) is null and 
                    src:qnor is not null) THEN
                    'qnor contains a non-numeric value : \'' || AS_VARCHAR(src:qnor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnrj), '0'), 38, 10) is null and 
                    src:qnrj is not null) THEN
                    'qnrj contains a non-numeric value : \'' || AS_VARCHAR(src:qnrj) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoal), '0'), 38, 10) is null and 
                    src:qoal is not null) THEN
                    'qoal contains a non-numeric value : \'' || AS_VARCHAR(src:qoal) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) THEN
                    'qoor contains a non-numeric value : \'' || AS_VARCHAR(src:qoor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) THEN
                    'qord contains a non-numeric value : \'' || AS_VARCHAR(src:qord) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null) THEN
                'sequencenumber contains a non-timestamp value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:item,
                src:cwar,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHWMD215 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_item_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_item_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:hstd), '1900-01-01')) is null and 
                    src:hstd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:item_ref_compnr), '0'), 38, 10) is null and 
                    src:item_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lsid), '1900-01-01')) is null and 
                    src:lsid is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ltdt), '1900-01-01')) is null and 
                    src:ltdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qall), '0'), 38, 10) is null and 
                    src:qall is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qblk), '0'), 38, 10) is null and 
                    src:qblk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qbpl), '0'), 38, 10) is null and 
                    src:qbpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcal), '0'), 38, 10) is null and 
                    src:qcal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qchd), '0'), 38, 10) is null and 
                    src:qchd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcis), '0'), 38, 10) is null and 
                    src:qcis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcit), '0'), 38, 10) is null and 
                    src:qcit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcom), '0'), 38, 10) is null and 
                    src:qcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcor), '0'), 38, 10) is null and 
                    src:qcor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcpr), '0'), 38, 10) is null and 
                    src:qcpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcrj), '0'), 38, 10) is null and 
                    src:qcrj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qgit), '0'), 38, 10) is null and 
                    src:qgit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhib), '0'), 38, 10) is null and 
                    src:qhib is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhnd), '0'), 38, 10) is null and 
                    src:qhnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qhrj), '0'), 38, 10) is null and 
                    src:qhrj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qint), '0'), 38, 10) is null and 
                    src:qint is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qlal), '0'), 38, 10) is null and 
                    src:qlal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnal), '0'), 38, 10) is null and 
                    src:qnal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnbl), '0'), 38, 10) is null and 
                    src:qnbl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnbp), '0'), 38, 10) is null and 
                    src:qnbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnhd), '0'), 38, 10) is null and 
                    src:qnhd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnit), '0'), 38, 10) is null and 
                    src:qnit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnor), '0'), 38, 10) is null and 
                    src:qnor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qnrj), '0'), 38, 10) is null and 
                    src:qnrj is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoal), '0'), 38, 10) is null and 
                    src:qoal is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qoor), '0'), 38, 10) is null and 
                    src:qoor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qord), '0'), 38, 10) is null and 
                    src:qord is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)