CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TPPDM001_ERROR AS SELECT src, 'LN_TPPDM001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aagt), '0'), 38, 10) is null and 
                    src:aagt is not null) THEN
                    'aagt contains a non-numeric value : \'' || AS_VARCHAR(src:aagt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asct), '0'), 38, 10) is null and 
                    src:asct is not null) THEN
                    'asct contains a non-numeric value : \'' || AS_VARCHAR(src:asct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) THEN
                    'cofc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cofc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) THEN
                    'cprj_cpla_cact_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_cact_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) THEN
                    'cprj_cpla_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_cpla_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) THEN
                    'cprj_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_vers_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_vers_ref_compnr is not null) THEN
                    'cprj_vers_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cprj_vers_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cspa_ref_compnr is not null) THEN
                    'cspa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cspa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctng_ctsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ctng_ctsr_ref_compnr is not null) THEN
                    'ctng_ctsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctng_ctsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctng_ref_compnr), '0'), 38, 10) is null and 
                    src:ctng_ref_compnr is not null) THEN
                    'ctng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcty), '0'), 38, 10) is null and 
                    src:dcty is not null) THEN
                    'dcty contains a non-numeric value : \'' || AS_VARCHAR(src:dcty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delt), '0'), 38, 10) is null and 
                    src:delt is not null) THEN
                    'delt contains a non-numeric value : \'' || AS_VARCHAR(src:delt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfet), '0'), 38, 10) is null and 
                    src:dfet is not null) THEN
                    'dfet contains a non-numeric value : \'' || AS_VARCHAR(src:dfet) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dflv), '0'), 38, 10) is null and 
                    src:dflv is not null) THEN
                    'dflv contains a non-numeric value : \'' || AS_VARCHAR(src:dflv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsct), '0'), 38, 10) is null and 
                    src:dsct is not null) THEN
                    'dsct contains a non-numeric value : \'' || AS_VARCHAR(src:dsct) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erng_ersr_ref_compnr), '0'), 38, 10) is null and 
                    src:erng_ersr_ref_compnr is not null) THEN
                    'erng_ersr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:erng_ersr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erng_ref_compnr), '0'), 38, 10) is null and 
                    src:erng_ref_compnr is not null) THEN
                    'erng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:erng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnvf), '0'), 38, 10) is null and 
                    src:mnvf is not null) THEN
                    'mnvf contains a non-numeric value : \'' || AS_VARCHAR(src:mnvf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpvw), '0'), 38, 10) is null and 
                    src:mpvw is not null) THEN
                    'mpvw contains a non-numeric value : \'' || AS_VARCHAR(src:mpvw) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ests_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ests_ref_compnr is not null) THEN
                    'ngrp_ests_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrp_ests_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_prsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_prsr_ref_compnr is not null) THEN
                    'ngrp_prsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrp_prsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ref_compnr is not null) THEN
                    'ngrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olvl), '0'), 38, 10) is null and 
                    src:olvl is not null) THEN
                    'olvl contains a non-numeric value : \'' || AS_VARCHAR(src:olvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potp_ref_compnr), '0'), 38, 10) is null and 
                    src:potp_ref_compnr is not null) THEN
                    'potp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:potp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prim), '0'), 38, 10) is null and 
                    src:prim is not null) THEN
                    'prim contains a non-numeric value : \'' || AS_VARCHAR(src:prim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pung_pusr_ref_compnr), '0'), 38, 10) is null and 
                    src:pung_pusr_ref_compnr is not null) THEN
                    'pung_pusr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pung_pusr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pung_ref_compnr), '0'), 38, 10) is null and 
                    src:pung_ref_compnr is not null) THEN
                    'pung_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pung_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlcr), '0'), 38, 10) is null and 
                    src:rlcr is not null) THEN
                    'rlcr contains a non-numeric value : \'' || AS_VARCHAR(src:rlcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlgn), '0'), 38, 10) is null and 
                    src:rlgn is not null) THEN
                    'rlgn contains a non-numeric value : \'' || AS_VARCHAR(src:rlgn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:song_ref_compnr), '0'), 38, 10) is null and 
                    src:song_ref_compnr is not null) THEN
                    'song_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:song_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:song_sosr_ref_compnr), '0'), 38, 10) is null and 
                    src:song_sosr_ref_compnr is not null) THEN
                    'song_sosr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:song_sosr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_ref_compnr is not null) THEN
                    'tgrp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tgrp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_tpsr_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_tpsr_ref_compnr is not null) THEN
                    'tgrp_tpsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tgrp_tpsr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprf), '0'), 38, 10) is null and 
                    src:tprf is not null) THEN
                    'tprf contains a non-numeric value : \'' || AS_VARCHAR(src:tprf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpte_ref_compnr), '0'), 38, 10) is null and 
                    src:tpte_ref_compnr is not null) THEN
                    'tpte_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tpte_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udac), '0'), 38, 10) is null and 
                    src:udac is not null) THEN
                    'udac contains a non-numeric value : \'' || AS_VARCHAR(src:udac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udev), '0'), 38, 10) is null and 
                    src:udev is not null) THEN
                    'udev contains a non-numeric value : \'' || AS_VARCHAR(src:udev) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udpl), '0'), 38, 10) is null and 
                    src:udpl is not null) THEN
                    'udpl contains a non-numeric value : \'' || AS_VARCHAR(src:udpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udpr), '0'), 38, 10) is null and 
                    src:udpr is not null) THEN
                    'udpr contains a non-numeric value : \'' || AS_VARCHAR(src:udpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udsp), '0'), 38, 10) is null and 
                    src:udsp is not null) THEN
                    'udsp contains a non-numeric value : \'' || AS_VARCHAR(src:udsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whng_ref_compnr), '0'), 38, 10) is null and 
                    src:whng_ref_compnr is not null) THEN
                    'whng_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:whng_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whng_whsr_ref_compnr), '0'), 38, 10) is null and 
                    src:whng_whsr_ref_compnr is not null) THEN
                    'whng_whsr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:whng_whsr_ref_compnr) || '\'' WHEN 
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
                src:loco  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TPPDM001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aagt), '0'), 38, 10) is null and 
                    src:aagt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asct), '0'), 38, 10) is null and 
                    src:asct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cofc_ref_compnr), '0'), 38, 10) is null and 
                    src:cofc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_cact_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_cact_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_cpla_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_cpla_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cprj_vers_ref_compnr), '0'), 38, 10) is null and 
                    src:cprj_vers_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cspa_ref_compnr), '0'), 38, 10) is null and 
                    src:cspa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctng_ctsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ctng_ctsr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctng_ref_compnr), '0'), 38, 10) is null and 
                    src:ctng_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcty), '0'), 38, 10) is null and 
                    src:dcty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:delt), '0'), 38, 10) is null and 
                    src:delt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dfet), '0'), 38, 10) is null and 
                    src:dfet is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dflv), '0'), 38, 10) is null and 
                    src:dflv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsct), '0'), 38, 10) is null and 
                    src:dsct is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erng_ersr_ref_compnr), '0'), 38, 10) is null and 
                    src:erng_ersr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erng_ref_compnr), '0'), 38, 10) is null and 
                    src:erng_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnvf), '0'), 38, 10) is null and 
                    src:mnvf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpvw), '0'), 38, 10) is null and 
                    src:mpvw is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ests_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ests_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_prsr_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_prsr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngrp_ref_compnr), '0'), 38, 10) is null and 
                    src:ngrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olvl), '0'), 38, 10) is null and 
                    src:olvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:potp_ref_compnr), '0'), 38, 10) is null and 
                    src:potp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prim), '0'), 38, 10) is null and 
                    src:prim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pung_pusr_ref_compnr), '0'), 38, 10) is null and 
                    src:pung_pusr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pung_ref_compnr), '0'), 38, 10) is null and 
                    src:pung_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlcr), '0'), 38, 10) is null and 
                    src:rlcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlgn), '0'), 38, 10) is null and 
                    src:rlgn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:song_ref_compnr), '0'), 38, 10) is null and 
                    src:song_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:song_sosr_ref_compnr), '0'), 38, 10) is null and 
                    src:song_sosr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tgrp_tpsr_ref_compnr), '0'), 38, 10) is null and 
                    src:tgrp_tpsr_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tprf), '0'), 38, 10) is null and 
                    src:tprf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpte_ref_compnr), '0'), 38, 10) is null and 
                    src:tpte_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udac), '0'), 38, 10) is null and 
                    src:udac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udev), '0'), 38, 10) is null and 
                    src:udev is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udpl), '0'), 38, 10) is null and 
                    src:udpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udpr), '0'), 38, 10) is null and 
                    src:udpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udsp), '0'), 38, 10) is null and 
                    src:udsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whng_ref_compnr), '0'), 38, 10) is null and 
                    src:whng_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whng_whsr_ref_compnr), '0'), 38, 10) is null and 
                    src:whng_whsr_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)