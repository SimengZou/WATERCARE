CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM460_ERROR AS SELECT src, 'LN_TSCTM460' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_1), '0'), 38, 10) is null and 
                    src:acco_1 is not null) THEN
                    'acco_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acco_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_2), '0'), 38, 10) is null and 
                    src:acco_2 is not null) THEN
                    'acco_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acco_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_3), '0'), 38, 10) is null and 
                    src:acco_3 is not null) THEN
                    'acco_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acco_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) THEN
                    'camt_1 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) THEN
                    'camt_2 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) THEN
                    'camt_3 contains a non-numeric value : \'' || AS_VARCHAR(src:camt_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) THEN
                    'cchn contains a non-numeric value : \'' || AS_VARCHAR(src:cchn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrv), '0'), 38, 10) is null and 
                    src:clrv is not null) THEN
                    'clrv contains a non-numeric value : \'' || AS_VARCHAR(src:clrv) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) THEN
                    'cmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corv), '0'), 38, 10) is null and 
                    src:corv is not null) THEN
                    'corv contains a non-numeric value : \'' || AS_VARCHAR(src:corv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corv_dwhc), '0'), 38, 10) is null and 
                    src:corv_dwhc is not null) THEN
                    'corv_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:corv_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corv_refc), '0'), 38, 10) is null and 
                    src:corv_refc is not null) THEN
                    'corv_refc contains a non-numeric value : \'' || AS_VARCHAR(src:corv_refc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) THEN
                    'crdt contains a non-timestamp value : \'' || AS_VARCHAR(src:crdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) THEN
                    'csco_cchn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_cchn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erfa), '0'), 38, 10) is null and 
                    src:erfa is not null) THEN
                    'erfa contains a non-numeric value : \'' || AS_VARCHAR(src:erfa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndrc), '0'), 38, 10) is null and 
                    src:ndrc is not null) THEN
                    'ndrc contains a non-numeric value : \'' || AS_VARCHAR(src:ndrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpr), '0'), 38, 10) is null and 
                    src:plpr is not null) THEN
                    'plpr contains a non-numeric value : \'' || AS_VARCHAR(src:plpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plyr), '0'), 38, 10) is null and 
                    src:plyr is not null) THEN
                    'plyr contains a non-numeric value : \'' || AS_VARCHAR(src:plyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:popr), '0'), 38, 10) is null and 
                    src:popr is not null) THEN
                    'popr contains a non-numeric value : \'' || AS_VARCHAR(src:popr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poyr), '0'), 38, 10) is null and 
                    src:poyr is not null) THEN
                    'poyr contains a non-numeric value : \'' || AS_VARCHAR(src:poyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prov), '0'), 38, 10) is null and 
                    src:prov is not null) THEN
                    'prov contains a non-numeric value : \'' || AS_VARCHAR(src:prov) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_1), '0'), 38, 10) is null and 
                    src:rahc_1 is not null) THEN
                    'rahc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_2), '0'), 38, 10) is null and 
                    src:rahc_2 is not null) THEN
                    'rahc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_3), '0'), 38, 10) is null and 
                    src:rahc_3 is not null) THEN
                    'rahc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rahc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) THEN
                    'ratc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) THEN
                    'ratc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) THEN
                    'ratc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratc_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rcdt), '1900-01-01')) is null and 
                    src:rcdt is not null) THEN
                    'rcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:rcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtor), '0'), 38, 10) is null and 
                    src:rtor is not null) THEN
                    'rtor contains a non-numeric value : \'' || AS_VARCHAR(src:rtor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) THEN
                    'seqn contains a non-numeric value : \'' || AS_VARCHAR(src:seqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
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
                                    
                src:csco,
                src:plpr,
                src:cchn,
                src:compnr,
                src:seqn,
                src:cfln,
                src:plyr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM460 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_1), '0'), 38, 10) is null and 
                    src:acco_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_2), '0'), 38, 10) is null and 
                    src:acco_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_3), '0'), 38, 10) is null and 
                    src:acco_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_1), '0'), 38, 10) is null and 
                    src:camt_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_2), '0'), 38, 10) is null and 
                    src:camt_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:camt_3), '0'), 38, 10) is null and 
                    src:camt_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrv), '0'), 38, 10) is null and 
                    src:clrv is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corv), '0'), 38, 10) is null and 
                    src:corv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corv_dwhc), '0'), 38, 10) is null and 
                    src:corv_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corv_refc), '0'), 38, 10) is null and 
                    src:corv_refc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:crdt), '1900-01-01')) is null and 
                    src:crdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erfa), '0'), 38, 10) is null and 
                    src:erfa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndrc), '0'), 38, 10) is null and 
                    src:ndrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpr), '0'), 38, 10) is null and 
                    src:plpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plyr), '0'), 38, 10) is null and 
                    src:plyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:popr), '0'), 38, 10) is null and 
                    src:popr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poyr), '0'), 38, 10) is null and 
                    src:poyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prov), '0'), 38, 10) is null and 
                    src:prov is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_1), '0'), 38, 10) is null and 
                    src:rahc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_2), '0'), 38, 10) is null and 
                    src:rahc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rahc_3), '0'), 38, 10) is null and 
                    src:rahc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_1), '0'), 38, 10) is null and 
                    src:ratc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_2), '0'), 38, 10) is null and 
                    src:ratc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratc_3), '0'), 38, 10) is null and 
                    src:ratc_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:rcdt), '1900-01-01')) is null and 
                    src:rcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtor), '0'), 38, 10) is null and 
                    src:rtor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:seqn), '0'), 38, 10) is null and 
                    src:seqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)