CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD206_ERROR AS SELECT src, 'LN_TFGLD206' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono), '0'), 38, 10) is null and 
                    src:cono is not null) THEN
                    'cono contains a non-numeric value : \'' || AS_VARCHAR(src:cono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:duac), '0'), 38, 10) is null and 
                    src:duac is not null) THEN
                    'duac contains a non-numeric value : \'' || AS_VARCHAR(src:duac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_1), '0'), 38, 10) is null and 
                    src:fobh_1 is not null) THEN
                    'fobh_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fobh_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_2), '0'), 38, 10) is null and 
                    src:fobh_2 is not null) THEN
                    'fobh_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fobh_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_3), '0'), 38, 10) is null and 
                    src:fobh_3 is not null) THEN
                    'fobh_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fobh_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_dtwc), '0'), 38, 10) is null and 
                    src:fobh_dtwc is not null) THEN
                    'fobh_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:fobh_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_rfrc), '0'), 38, 10) is null and 
                    src:fobh_rfrc is not null) THEN
                    'fobh_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:fobh_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ftob), '0'), 38, 10) is null and 
                    src:ftob is not null) THEN
                    'ftob contains a non-numeric value : \'' || AS_VARCHAR(src:ftob) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nob1), '0'), 38, 10) is null and 
                    src:nob1 is not null) THEN
                    'nob1 contains a non-numeric value : \'' || AS_VARCHAR(src:nob1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nob2), '0'), 38, 10) is null and 
                    src:nob2 is not null) THEN
                    'nob2 contains a non-numeric value : \'' || AS_VARCHAR(src:nob2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_1), '0'), 38, 10) is null and 
                    src:nobh_1 is not null) THEN
                    'nobh_1 contains a non-numeric value : \'' || AS_VARCHAR(src:nobh_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_2), '0'), 38, 10) is null and 
                    src:nobh_2 is not null) THEN
                    'nobh_2 contains a non-numeric value : \'' || AS_VARCHAR(src:nobh_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_3), '0'), 38, 10) is null and 
                    src:nobh_3 is not null) THEN
                    'nobh_3 contains a non-numeric value : \'' || AS_VARCHAR(src:nobh_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_dtwc), '0'), 38, 10) is null and 
                    src:nobh_dtwc is not null) THEN
                    'nobh_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:nobh_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_rfrc), '0'), 38, 10) is null and 
                    src:nobh_rfrc is not null) THEN
                    'nobh_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:nobh_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntob), '0'), 38, 10) is null and 
                    src:ntob is not null) THEN
                    'ntob contains a non-numeric value : \'' || AS_VARCHAR(src:ntob) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olap), '0'), 38, 10) is null and 
                    src:olap is not null) THEN
                    'olap contains a non-numeric value : \'' || AS_VARCHAR(src:olap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qob1), '0'), 38, 10) is null and 
                    src:qob1 is not null) THEN
                    'qob1 contains a non-numeric value : \'' || AS_VARCHAR(src:qob1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qob2), '0'), 38, 10) is null and 
                    src:qob2 is not null) THEN
                    'qob2 contains a non-numeric value : \'' || AS_VARCHAR(src:qob2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) THEN
                    'year contains a non-numeric value : \'' || AS_VARCHAR(src:year) || '\'' WHEN 
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
                                    
                src:dims,
                src:dim4,
                src:duac,
                src:dim1,
                src:dim3,
                src:ccur,
                src:bpid,
                src:dim5,
                src:dim2,
                src:cono,
                src:year,
                src:leac,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD206 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cono), '0'), 38, 10) is null and 
                    src:cono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:duac), '0'), 38, 10) is null and 
                    src:duac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_1), '0'), 38, 10) is null and 
                    src:fobh_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_2), '0'), 38, 10) is null and 
                    src:fobh_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_3), '0'), 38, 10) is null and 
                    src:fobh_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_dtwc), '0'), 38, 10) is null and 
                    src:fobh_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fobh_rfrc), '0'), 38, 10) is null and 
                    src:fobh_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ftob), '0'), 38, 10) is null and 
                    src:ftob is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nob1), '0'), 38, 10) is null and 
                    src:nob1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nob2), '0'), 38, 10) is null and 
                    src:nob2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_1), '0'), 38, 10) is null and 
                    src:nobh_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_2), '0'), 38, 10) is null and 
                    src:nobh_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_3), '0'), 38, 10) is null and 
                    src:nobh_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_dtwc), '0'), 38, 10) is null and 
                    src:nobh_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nobh_rfrc), '0'), 38, 10) is null and 
                    src:nobh_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ntob), '0'), 38, 10) is null and 
                    src:ntob is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olap), '0'), 38, 10) is null and 
                    src:olap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qob1), '0'), 38, 10) is null and 
                    src:qob1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qob2), '0'), 38, 10) is null and 
                    src:qob2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)