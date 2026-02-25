CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFGLD205_ERROR AS SELECT src, 'LN_TFGLD205' as TABLE_OBJECT, CASE WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_1), '0'), 38, 10) is null and 
                    src:fcah_1 is not null) THEN
                    'fcah_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fcah_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_2), '0'), 38, 10) is null and 
                    src:fcah_2 is not null) THEN
                    'fcah_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fcah_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_3), '0'), 38, 10) is null and 
                    src:fcah_3 is not null) THEN
                    'fcah_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fcah_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_dtwc), '0'), 38, 10) is null and 
                    src:fcah_dtwc is not null) THEN
                    'fcah_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:fcah_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_rfrc), '0'), 38, 10) is null and 
                    src:fcah_rfrc is not null) THEN
                    'fcah_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:fcah_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcam), '0'), 38, 10) is null and 
                    src:fcam is not null) THEN
                    'fcam contains a non-numeric value : \'' || AS_VARCHAR(src:fcam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_1), '0'), 38, 10) is null and 
                    src:fdah_1 is not null) THEN
                    'fdah_1 contains a non-numeric value : \'' || AS_VARCHAR(src:fdah_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_2), '0'), 38, 10) is null and 
                    src:fdah_2 is not null) THEN
                    'fdah_2 contains a non-numeric value : \'' || AS_VARCHAR(src:fdah_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_3), '0'), 38, 10) is null and 
                    src:fdah_3 is not null) THEN
                    'fdah_3 contains a non-numeric value : \'' || AS_VARCHAR(src:fdah_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_dtwc), '0'), 38, 10) is null and 
                    src:fdah_dtwc is not null) THEN
                    'fdah_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:fdah_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_rfrc), '0'), 38, 10) is null and 
                    src:fdah_rfrc is not null) THEN
                    'fdah_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:fdah_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdam), '0'), 38, 10) is null and 
                    src:fdam is not null) THEN
                    'fdam contains a non-numeric value : \'' || AS_VARCHAR(src:fdam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fqt1), '0'), 38, 10) is null and 
                    src:fqt1 is not null) THEN
                    'fqt1 contains a non-numeric value : \'' || AS_VARCHAR(src:fqt1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fqt2), '0'), 38, 10) is null and 
                    src:fqt2 is not null) THEN
                    'fqt2 contains a non-numeric value : \'' || AS_VARCHAR(src:fqt2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_1), '0'), 38, 10) is null and 
                    src:ncah_1 is not null) THEN
                    'ncah_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ncah_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_2), '0'), 38, 10) is null and 
                    src:ncah_2 is not null) THEN
                    'ncah_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ncah_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_3), '0'), 38, 10) is null and 
                    src:ncah_3 is not null) THEN
                    'ncah_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ncah_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_dtwc), '0'), 38, 10) is null and 
                    src:ncah_dtwc is not null) THEN
                    'ncah_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:ncah_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_rfrc), '0'), 38, 10) is null and 
                    src:ncah_rfrc is not null) THEN
                    'ncah_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:ncah_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncam), '0'), 38, 10) is null and 
                    src:ncam is not null) THEN
                    'ncam contains a non-numeric value : \'' || AS_VARCHAR(src:ncam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_1), '0'), 38, 10) is null and 
                    src:ndah_1 is not null) THEN
                    'ndah_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ndah_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_2), '0'), 38, 10) is null and 
                    src:ndah_2 is not null) THEN
                    'ndah_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ndah_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_3), '0'), 38, 10) is null and 
                    src:ndah_3 is not null) THEN
                    'ndah_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ndah_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_dtwc), '0'), 38, 10) is null and 
                    src:ndah_dtwc is not null) THEN
                    'ndah_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:ndah_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_rfrc), '0'), 38, 10) is null and 
                    src:ndah_rfrc is not null) THEN
                    'ndah_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:ndah_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndam), '0'), 38, 10) is null and 
                    src:ndam is not null) THEN
                    'ndam contains a non-numeric value : \'' || AS_VARCHAR(src:ndam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nqt1), '0'), 38, 10) is null and 
                    src:nqt1 is not null) THEN
                    'nqt1 contains a non-numeric value : \'' || AS_VARCHAR(src:nqt1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nqt2), '0'), 38, 10) is null and 
                    src:nqt2 is not null) THEN
                    'nqt2 contains a non-numeric value : \'' || AS_VARCHAR(src:nqt2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olap), '0'), 38, 10) is null and 
                    src:olap is not null) THEN
                    'olap contains a non-numeric value : \'' || AS_VARCHAR(src:olap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno), '0'), 38, 10) is null and 
                    src:prno is not null) THEN
                    'prno contains a non-numeric value : \'' || AS_VARCHAR(src:prno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) THEN
                    'ptyp contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp) || '\'' WHEN 
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
                                    
                src:cono,
                src:prno,
                src:leac,
                src:ccur,
                src:bpid,
                src:dim4,
                src:dim3,
                src:dims,
                src:dim1,
                src:duac,
                src:compnr,
                src:dim2,
                src:dim5,
                src:year,
                src:ptyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD205 as strm)
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_1), '0'), 38, 10) is null and 
                    src:fcah_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_2), '0'), 38, 10) is null and 
                    src:fcah_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_3), '0'), 38, 10) is null and 
                    src:fcah_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_dtwc), '0'), 38, 10) is null and 
                    src:fcah_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcah_rfrc), '0'), 38, 10) is null and 
                    src:fcah_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fcam), '0'), 38, 10) is null and 
                    src:fcam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_1), '0'), 38, 10) is null and 
                    src:fdah_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_2), '0'), 38, 10) is null and 
                    src:fdah_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_3), '0'), 38, 10) is null and 
                    src:fdah_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_dtwc), '0'), 38, 10) is null and 
                    src:fdah_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdah_rfrc), '0'), 38, 10) is null and 
                    src:fdah_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdam), '0'), 38, 10) is null and 
                    src:fdam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fqt1), '0'), 38, 10) is null and 
                    src:fqt1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fqt2), '0'), 38, 10) is null and 
                    src:fqt2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_1), '0'), 38, 10) is null and 
                    src:ncah_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_2), '0'), 38, 10) is null and 
                    src:ncah_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_3), '0'), 38, 10) is null and 
                    src:ncah_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_dtwc), '0'), 38, 10) is null and 
                    src:ncah_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncah_rfrc), '0'), 38, 10) is null and 
                    src:ncah_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncam), '0'), 38, 10) is null and 
                    src:ncam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_1), '0'), 38, 10) is null and 
                    src:ndah_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_2), '0'), 38, 10) is null and 
                    src:ndah_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_3), '0'), 38, 10) is null and 
                    src:ndah_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_dtwc), '0'), 38, 10) is null and 
                    src:ndah_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndah_rfrc), '0'), 38, 10) is null and 
                    src:ndah_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ndam), '0'), 38, 10) is null and 
                    src:ndam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nqt1), '0'), 38, 10) is null and 
                    src:nqt1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nqt2), '0'), 38, 10) is null and 
                    src:nqt2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olap), '0'), 38, 10) is null and 
                    src:olap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prno), '0'), 38, 10) is null and 
                    src:prno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)