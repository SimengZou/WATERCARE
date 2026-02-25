CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TIROU001_ERROR AS SELECT src, 'LN_TIROU001' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfem_ref_compnr), '0'), 38, 10) is null and 
                    src:bfem_ref_compnr is not null) THEN
                    'bfem_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bfem_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfty), '0'), 38, 10) is null and 
                    src:bfty is not null) THEN
                    'bfty contains a non-numeric value : \'' || AS_VARCHAR(src:bfty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccap), '0'), 38, 10) is null and 
                    src:ccap is not null) THEN
                    'ccap contains a non-numeric value : \'' || AS_VARCHAR(src:ccap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crmp), '0'), 38, 10) is null and 
                    src:crmp is not null) THEN
                    'crmp contains a non-numeric value : \'' || AS_VARCHAR(src:crmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtf), '0'), 38, 10) is null and 
                    src:crtf is not null) THEN
                    'crtf contains a non-numeric value : \'' || AS_VARCHAR(src:crtf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crty), '0'), 38, 10) is null and 
                    src:crty is not null) THEN
                    'crty contains a non-numeric value : \'' || AS_VARCHAR(src:crty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctwc_ref_compnr), '0'), 38, 10) is null and 
                    src:ctwc_ref_compnr is not null) THEN
                    'ctwc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ctwc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcru), '0'), 38, 10) is null and 
                    src:dcru is not null) THEN
                    'dcru contains a non-numeric value : \'' || AS_VARCHAR(src:dcru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dque), '0'), 38, 10) is null and 
                    src:dque is not null) THEN
                    'dque contains a non-numeric value : \'' || AS_VARCHAR(src:dque) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kowc), '0'), 38, 10) is null and 
                    src:kowc is not null) THEN
                    'kowc contains a non-numeric value : \'' || AS_VARCHAR(src:kowc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcgr), '0'), 38, 10) is null and 
                    src:mcgr is not null) THEN
                    'mcgr contains a non-numeric value : \'' || AS_VARCHAR(src:mcgr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnwc_ref_compnr), '0'), 38, 10) is null and 
                    src:mnwc_ref_compnr is not null) THEN
                    'mnwc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mnwc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvtm), '0'), 38, 10) is null and 
                    src:mvtm is not null) THEN
                    'mvtm contains a non-numeric value : \'' || AS_VARCHAR(src:mvtm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nomc), '0'), 38, 10) is null and 
                    src:nomc is not null) THEN
                    'nomc contains a non-numeric value : \'' || AS_VARCHAR(src:nomc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noop), '0'), 38, 10) is null and 
                    src:noop is not null) THEN
                    'noop contains a non-numeric value : \'' || AS_VARCHAR(src:noop) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norp), '0'), 38, 10) is null and 
                    src:norp is not null) THEN
                    'norp contains a non-numeric value : \'' || AS_VARCHAR(src:norp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obfu), '0'), 38, 10) is null and 
                    src:obfu is not null) THEN
                    'obfu contains a non-numeric value : \'' || AS_VARCHAR(src:obfu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprc_ref_compnr), '0'), 38, 10) is null and 
                    src:oprc_ref_compnr is not null) THEN
                    'oprc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oprc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pddp_rcmp), '0'), 38, 10) is null and 
                    src:pddp_rcmp is not null) THEN
                    'pddp_rcmp contains a non-numeric value : \'' || AS_VARCHAR(src:pddp_rcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pddp_ref_compnr), '0'), 38, 10) is null and 
                    src:pddp_ref_compnr is not null) THEN
                    'pddp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pddp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) THEN
                    'prin contains a non-numeric value : \'' || AS_VARCHAR(src:prin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) THEN
                    'qutm contains a non-numeric value : \'' || AS_VARCHAR(src:qutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlbr), '0'), 38, 10) is null and 
                    src:rlbr is not null) THEN
                    'rlbr contains a non-numeric value : \'' || AS_VARCHAR(src:rlbr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"rows"), '0'), 38, 10) is null and 
                    src:"rows" is not null) THEN
                    '"rows" contains a non-numeric value : \'' || AS_VARCHAR(src:"rows") || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprd), '0'), 38, 10) is null and 
                    src:rprd is not null) THEN
                    'rprd contains a non-numeric value : \'' || AS_VARCHAR(src:rprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpwc_ref_compnr), '0'), 38, 10) is null and 
                    src:rpwc_ref_compnr is not null) THEN
                    'rpwc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rpwc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scid), '0'), 38, 10) is null and 
                    src:scid is not null) THEN
                    'scid contains a non-numeric value : \'' || AS_VARCHAR(src:scid) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) THEN
                    'stty contains a non-numeric value : \'' || AS_VARCHAR(src:stty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swcc), '0'), 38, 10) is null and 
                    src:swcc is not null) THEN
                    'swcc contains a non-numeric value : \'' || AS_VARCHAR(src:swcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swct), '0'), 38, 10) is null and 
                    src:swct is not null) THEN
                    'swct contains a non-numeric value : \'' || AS_VARCHAR(src:swct) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuni), '0'), 38, 10) is null and 
                    src:tuni is not null) THEN
                    'tuni contains a non-numeric value : \'' || AS_VARCHAR(src:tuni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upcs), '0'), 38, 10) is null and 
                    src:upcs is not null) THEN
                    'upcs contains a non-numeric value : \'' || AS_VARCHAR(src:upcs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usco), '0'), 38, 10) is null and 
                    src:usco is not null) THEN
                    'usco contains a non-numeric value : \'' || AS_VARCHAR(src:usco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcpg_ref_compnr), '0'), 38, 10) is null and 
                    src:wcpg_ref_compnr is not null) THEN
                    'wcpg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wcpg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcru), '0'), 38, 10) is null and 
                    src:wcru is not null) THEN
                    'wcru contains a non-numeric value : \'' || AS_VARCHAR(src:wcru) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wipw_ref_compnr), '0'), 38, 10) is null and 
                    src:wipw_ref_compnr is not null) THEN
                    'wipw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wipw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wttm), '0'), 38, 10) is null and 
                    src:wttm is not null) THEN
                    'wttm contains a non-numeric value : \'' || AS_VARCHAR(src:wttm) || '\'' WHEN 
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
                src:cwoc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TIROU001 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfem_ref_compnr), '0'), 38, 10) is null and 
                    src:bfem_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bfty), '0'), 38, 10) is null and 
                    src:bfty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccap), '0'), 38, 10) is null and 
                    src:ccap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crmp), '0'), 38, 10) is null and 
                    src:crmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crtf), '0'), 38, 10) is null and 
                    src:crtf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crty), '0'), 38, 10) is null and 
                    src:crty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ctwc_ref_compnr), '0'), 38, 10) is null and 
                    src:ctwc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dcru), '0'), 38, 10) is null and 
                    src:dcru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dque), '0'), 38, 10) is null and 
                    src:dque is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kowc), '0'), 38, 10) is null and 
                    src:kowc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcgr), '0'), 38, 10) is null and 
                    src:mcgr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnwc_ref_compnr), '0'), 38, 10) is null and 
                    src:mnwc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mvtm), '0'), 38, 10) is null and 
                    src:mvtm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nomc), '0'), 38, 10) is null and 
                    src:nomc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:noop), '0'), 38, 10) is null and 
                    src:noop is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:norp), '0'), 38, 10) is null and 
                    src:norp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:obfu), '0'), 38, 10) is null and 
                    src:obfu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprc_ref_compnr), '0'), 38, 10) is null and 
                    src:oprc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pddp_rcmp), '0'), 38, 10) is null and 
                    src:pddp_rcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pddp_ref_compnr), '0'), 38, 10) is null and 
                    src:pddp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qutm), '0'), 38, 10) is null and 
                    src:qutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rlbr), '0'), 38, 10) is null and 
                    src:rlbr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:"rows"), '0'), 38, 10) is null and 
                    src:"rows" is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rprd), '0'), 38, 10) is null and 
                    src:rprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpwc_ref_compnr), '0'), 38, 10) is null and 
                    src:rpwc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scid), '0'), 38, 10) is null and 
                    src:scid is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stty), '0'), 38, 10) is null and 
                    src:stty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swcc), '0'), 38, 10) is null and 
                    src:swcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:swct), '0'), 38, 10) is null and 
                    src:swct is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tuni), '0'), 38, 10) is null and 
                    src:tuni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upcs), '0'), 38, 10) is null and 
                    src:upcs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usco), '0'), 38, 10) is null and 
                    src:usco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcpg_ref_compnr), '0'), 38, 10) is null and 
                    src:wcpg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wcru), '0'), 38, 10) is null and 
                    src:wcru is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wipw_ref_compnr), '0'), 38, 10) is null and 
                    src:wipw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wttm), '0'), 38, 10) is null and 
                    src:wttm is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)