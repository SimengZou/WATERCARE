CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHWMD300_ERROR AS SELECT src, 'LN_WHWMD300' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aoit), '0'), 38, 10) is null and 
                    src:aoit is not null) THEN
                    'aoit contains a non-numeric value : \'' || AS_VARCHAR(src:aoit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aslo), '0'), 38, 10) is null and 
                    src:aslo is not null) THEN
                    'aslo contains a non-numeric value : \'' || AS_VARCHAR(src:aslo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ball), '0'), 38, 10) is null and 
                    src:ball is not null) THEN
                    'ball contains a non-numeric value : \'' || AS_VARCHAR(src:ball) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bass), '0'), 38, 10) is null and 
                    src:bass is not null) THEN
                    'bass contains a non-numeric value : \'' || AS_VARCHAR(src:bass) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:binb), '0'), 38, 10) is null and 
                    src:binb is not null) THEN
                    'binb contains a non-numeric value : \'' || AS_VARCHAR(src:binb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bout), '0'), 38, 10) is null and 
                    src:bout is not null) THEN
                    'bout contains a non-numeric value : \'' || AS_VARCHAR(src:bout) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btri), '0'), 38, 10) is null and 
                    src:btri is not null) THEN
                    'btri contains a non-numeric value : \'' || AS_VARCHAR(src:btri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btrr), '0'), 38, 10) is null and 
                    src:btrr is not null) THEN
                    'btrr contains a non-numeric value : \'' || AS_VARCHAR(src:btrr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmes_ref_compnr), '0'), 38, 10) is null and 
                    src:cmes_ref_compnr is not null) THEN
                    'cmes_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cmes_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_zone_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_zone_ref_compnr is not null) THEN
                    'cwar_zone_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_zone_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:filo), '0'), 38, 10) is null and 
                    src:filo is not null) THEN
                    'filo contains a non-numeric value : \'' || AS_VARCHAR(src:filo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"full"), '0'), 38, 10) is null and 
                    src:"full" is not null) THEN
                    '"full" contains a non-numeric value : \'' || AS_VARCHAR(src:"full") || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inca), '0'), 38, 10) is null and 
                    src:inca is not null) THEN
                    'inca contains a non-numeric value : \'' || AS_VARCHAR(src:inca) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inlo), '0'), 38, 10) is null and 
                    src:inlo is not null) THEN
                    'inlo contains a non-numeric value : \'' || AS_VARCHAR(src:inlo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itlo), '0'), 38, 10) is null and 
                    src:itlo is not null) THEN
                    'itlo contains a non-numeric value : \'' || AS_VARCHAR(src:itlo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loct), '0'), 38, 10) is null and 
                    src:loct is not null) THEN
                    'loct contains a non-numeric value : \'' || AS_VARCHAR(src:loct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lolo), '0'), 38, 10) is null and 
                    src:lolo is not null) THEN
                    'lolo contains a non-numeric value : \'' || AS_VARCHAR(src:lolo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oclo), '0'), 38, 10) is null and 
                    src:oclo is not null) THEN
                    'oclo contains a non-numeric value : \'' || AS_VARCHAR(src:oclo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:outl), '0'), 38, 10) is null and 
                    src:outl is not null) THEN
                    'outl contains a non-numeric value : \'' || AS_VARCHAR(src:outl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) THEN
                    'ownr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ownr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) THEN
                    'prio contains a non-numeric value : \'' || AS_VARCHAR(src:prio) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proo), '0'), 38, 10) is null and 
                    src:proo is not null) THEN
                    'proo contains a non-numeric value : \'' || AS_VARCHAR(src:proo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) THEN
                    'pseq contains a non-numeric value : \'' || AS_VARCHAR(src:pseq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rntl), '0'), 38, 10) is null and 
                    src:rntl is not null) THEN
                    'rntl contains a non-numeric value : \'' || AS_VARCHAR(src:rntl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) THEN
                    'rowc contains a non-numeric value : \'' || AS_VARCHAR(src:rowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) THEN
                    'rowc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rowc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) THEN
                    'rown_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rown_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sseq), '0'), 38, 10) is null and 
                    src:sseq is not null) THEN
                    'sseq contains a non-numeric value : \'' || AS_VARCHAR(src:sseq) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tran), '1900-01-01')) is null and 
                    src:tran is not null) THEN
                    'tran contains a non-timestamp value : \'' || AS_VARCHAR(src:tran) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trfr), '0'), 38, 10) is null and 
                    src:trfr is not null) THEN
                    'trfr contains a non-numeric value : \'' || AS_VARCHAR(src:trfr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trto), '0'), 38, 10) is null and 
                    src:trto is not null) THEN
                    'trto contains a non-numeric value : \'' || AS_VARCHAR(src:trto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) THEN
                    'txta contains a non-numeric value : \'' || AS_VARCHAR(src:txta) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) THEN
                    'txta_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txta_ref_compnr) || '\'' WHEN 
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
                src:loca,
                src:cwar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHWMD300 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aoit), '0'), 38, 10) is null and 
                    src:aoit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aslo), '0'), 38, 10) is null and 
                    src:aslo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ball), '0'), 38, 10) is null and 
                    src:ball is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bass), '0'), 38, 10) is null and 
                    src:bass is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:binb), '0'), 38, 10) is null and 
                    src:binb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bout), '0'), 38, 10) is null and 
                    src:bout is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btri), '0'), 38, 10) is null and 
                    src:btri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btrr), '0'), 38, 10) is null and 
                    src:btrr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cmes_ref_compnr), '0'), 38, 10) is null and 
                    src:cmes_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_zone_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_zone_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:filo), '0'), 38, 10) is null and 
                    src:filo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"full"), '0'), 38, 10) is null and 
                    src:"full" is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inca), '0'), 38, 10) is null and 
                    src:inca is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inlo), '0'), 38, 10) is null and 
                    src:inlo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itlo), '0'), 38, 10) is null and 
                    src:itlo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loct), '0'), 38, 10) is null and 
                    src:loct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lolo), '0'), 38, 10) is null and 
                    src:lolo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oclo), '0'), 38, 10) is null and 
                    src:oclo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:outl), '0'), 38, 10) is null and 
                    src:outl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ownr_ref_compnr), '0'), 38, 10) is null and 
                    src:ownr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prio), '0'), 38, 10) is null and 
                    src:prio is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proo), '0'), 38, 10) is null and 
                    src:proo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pseq), '0'), 38, 10) is null and 
                    src:pseq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rntl), '0'), 38, 10) is null and 
                    src:rntl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc), '0'), 38, 10) is null and 
                    src:rowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rowc_ref_compnr), '0'), 38, 10) is null and 
                    src:rowc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rown_ref_compnr), '0'), 38, 10) is null and 
                    src:rown_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sseq), '0'), 38, 10) is null and 
                    src:sseq is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:tran), '1900-01-01')) is null and 
                    src:tran is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trfr), '0'), 38, 10) is null and 
                    src:trfr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trto), '0'), 38, 10) is null and 
                    src:trto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta), '0'), 38, 10) is null and 
                    src:txta is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txta_ref_compnr), '0'), 38, 10) is null and 
                    src:txta_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)