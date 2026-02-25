CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP270_ERROR AS SELECT src, 'LN_TFACP270' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) THEN
                    'amnt contains a non-numeric value : \'' || AS_VARCHAR(src:amnt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) THEN
                    'amth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) THEN
                    'amth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) THEN
                    'amth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clss), '0'), 38, 10) is null and 
                    src:clss is not null) THEN
                    'clss contains a non-numeric value : \'' || AS_VARCHAR(src:clss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) THEN
                    'dbcr contains a non-numeric value : \'' || AS_VARCHAR(src:dbcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcom), '0'), 38, 10) is null and 
                    src:dcom is not null) THEN
                    'dcom contains a non-numeric value : \'' || AS_VARCHAR(src:dcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) THEN
                    'docn contains a non-numeric value : \'' || AS_VARCHAR(src:docn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) THEN
                    'fcom contains a non-numeric value : \'' || AS_VARCHAR(src:fcom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ktrn), '0'), 38, 10) is null and 
                    src:ktrn is not null) THEN
                    'ktrn contains a non-numeric value : \'' || AS_VARCHAR(src:ktrn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuni), '0'), 38, 10) is null and 
                    src:nuni is not null) THEN
                    'nuni contains a non-numeric value : \'' || AS_VARCHAR(src:nuni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) THEN
                    'ocmp contains a non-numeric value : \'' || AS_VARCHAR(src:ocmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) THEN
                    'reco contains a non-numeric value : \'' || AS_VARCHAR(src:reco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recs), '0'), 38, 10) is null and 
                    src:recs is not null) THEN
                    'recs contains a non-numeric value : \'' || AS_VARCHAR(src:recs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) THEN
                    'rpon contains a non-numeric value : \'' || AS_VARCHAR(src:rpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txin), '0'), 38, 10) is null and 
                    src:txin is not null) THEN
                    'txin contains a non-numeric value : \'' || AS_VARCHAR(src:txin) || '\'' WHEN 
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
                                    
                src:compnr,
                src:guid,
                src:dbcr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP270 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clss), '0'), 38, 10) is null and 
                    src:clss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dbcr), '0'), 38, 10) is null and 
                    src:dbcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcom), '0'), 38, 10) is null and 
                    src:dcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcom), '0'), 38, 10) is null and 
                    src:fcom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ktrn), '0'), 38, 10) is null and 
                    src:ktrn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nuni), '0'), 38, 10) is null and 
                    src:nuni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ocmp), '0'), 38, 10) is null and 
                    src:ocmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reco), '0'), 38, 10) is null and 
                    src:reco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:recs), '0'), 38, 10) is null and 
                    src:recs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpon), '0'), 38, 10) is null and 
                    src:rpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txin), '0'), 38, 10) is null and 
                    src:txin is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)