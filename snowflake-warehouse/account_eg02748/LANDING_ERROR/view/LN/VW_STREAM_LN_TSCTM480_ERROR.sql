CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSCTM480_ERROR AS SELECT src, 'LN_TSCTM480' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_1), '0'), 38, 10) is null and 
                    src:acco_1 is not null) THEN
                    'acco_1 contains a non-numeric value : \'' || AS_VARCHAR(src:acco_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_2), '0'), 38, 10) is null and 
                    src:acco_2 is not null) THEN
                    'acco_2 contains a non-numeric value : \'' || AS_VARCHAR(src:acco_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_3), '0'), 38, 10) is null and 
                    src:acco_3 is not null) THEN
                    'acco_3 contains a non-numeric value : \'' || AS_VARCHAR(src:acco_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_cntc), '0'), 38, 10) is null and 
                    src:acco_cntc is not null) THEN
                    'acco_cntc contains a non-numeric value : \'' || AS_VARCHAR(src:acco_cntc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_dwhc), '0'), 38, 10) is null and 
                    src:acco_dwhc is not null) THEN
                    'acco_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:acco_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_refc), '0'), 38, 10) is null and 
                    src:acco_refc is not null) THEN
                    'acco_refc contains a non-numeric value : \'' || AS_VARCHAR(src:acco_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) THEN
                    'cchn contains a non-numeric value : \'' || AS_VARCHAR(src:cchn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) THEN
                    'cfln contains a non-numeric value : \'' || AS_VARCHAR(src:cfln) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) THEN
                    'codt contains a non-timestamp value : \'' || AS_VARCHAR(src:codt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) THEN
                    'cotp contains a non-numeric value : \'' || AS_VARCHAR(src:cotp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) THEN
                    'csco_cchn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_cchn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) THEN
                    'csco_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:csco_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvln), '0'), 38, 10) is null and 
                    src:cvln is not null) THEN
                    'cvln contains a non-numeric value : \'' || AS_VARCHAR(src:cvln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ircl), '0'), 38, 10) is null and 
                    src:ircl is not null) THEN
                    'ircl contains a non-numeric value : \'' || AS_VARCHAR(src:ircl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivsq), '0'), 38, 10) is null and 
                    src:ivsq is not null) THEN
                    'ivsq contains a non-numeric value : \'' || AS_VARCHAR(src:ivsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) THEN
                    'ortp contains a non-numeric value : \'' || AS_VARCHAR(src:ortp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpr), '0'), 38, 10) is null and 
                    src:plpr is not null) THEN
                    'plpr contains a non-numeric value : \'' || AS_VARCHAR(src:plpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plyr), '0'), 38, 10) is null and 
                    src:plyr is not null) THEN
                    'plyr contains a non-numeric value : \'' || AS_VARCHAR(src:plyr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:popr), '0'), 38, 10) is null and 
                    src:popr is not null) THEN
                    'popr contains a non-numeric value : \'' || AS_VARCHAR(src:popr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poyr), '0'), 38, 10) is null and 
                    src:poyr is not null) THEN
                    'poyr contains a non-numeric value : \'' || AS_VARCHAR(src:poyr) || '\'' WHEN 
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
                                    
                src:cchn,
                src:csco,
                src:seqn,
                src:cfln,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSCTM480 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_1), '0'), 38, 10) is null and 
                    src:acco_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_2), '0'), 38, 10) is null and 
                    src:acco_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_3), '0'), 38, 10) is null and 
                    src:acco_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_cntc), '0'), 38, 10) is null and 
                    src:acco_cntc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_dwhc), '0'), 38, 10) is null and 
                    src:acco_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acco_refc), '0'), 38, 10) is null and 
                    src:acco_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cchn), '0'), 38, 10) is null and 
                    src:cchn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfln), '0'), 38, 10) is null and 
                    src:cfln is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:codt), '1900-01-01')) is null and 
                    src:codt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cotp), '0'), 38, 10) is null and 
                    src:cotp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_cchn_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_cchn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csco_ref_compnr), '0'), 38, 10) is null and 
                    src:csco_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvln), '0'), 38, 10) is null and 
                    src:cvln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ircl), '0'), 38, 10) is null and 
                    src:ircl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ivsq), '0'), 38, 10) is null and 
                    src:ivsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ortp), '0'), 38, 10) is null and 
                    src:ortp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plpr), '0'), 38, 10) is null and 
                    src:plpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plyr), '0'), 38, 10) is null and 
                    src:plyr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:popr), '0'), 38, 10) is null and 
                    src:popr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:poyr), '0'), 38, 10) is null and 
                    src:poyr is not null) or 
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