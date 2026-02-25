CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TSMDM100_ERROR AS SELECT src, 'LN_TSMDM100' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbwc), '0'), 38, 10) is null and 
                    src:cbwc is not null) THEN
                    'cbwc contains a non-numeric value : \'' || AS_VARCHAR(src:cbwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) THEN
                    'clrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cste_ref_compnr), '0'), 38, 10) is null and 
                    src:cste_ref_compnr is not null) THEN
                    'cste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:cwrh_ref_compnr is not null) THEN
                    'cwrh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwrh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dste_ref_compnr), '0'), 38, 10) is null and 
                    src:dste_ref_compnr is not null) THEN
                    'dste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:fwrh_ref_compnr is not null) THEN
                    'fwrh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:fwrh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grca_snca_ref_compnr), '0'), 38, 10) is null and 
                    src:grca_snca_ref_compnr is not null) THEN
                    'grca_snca_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grca_snca_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grcl_sncl_ref_compnr), '0'), 38, 10) is null and 
                    src:grcl_sncl_ref_compnr is not null) THEN
                    'grcl_sncl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grcl_sncl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grcq_sncq_ref_compnr), '0'), 38, 10) is null and 
                    src:grcq_sncq_ref_compnr is not null) THEN
                    'grcq_sncq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grcq_sncq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grfc_snfc_ref_compnr), '0'), 38, 10) is null and 
                    src:grfc_snfc_ref_compnr is not null) THEN
                    'grfc_snfc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grfc_snfc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grfq_snfq_ref_compnr), '0'), 38, 10) is null and 
                    src:grfq_snfq_ref_compnr is not null) THEN
                    'grfq_snfq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grfq_snfq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grfr_snfr_ref_compnr), '0'), 38, 10) is null and 
                    src:grfr_snfr_ref_compnr is not null) THEN
                    'grfr_snfr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grfr_snfr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grmq_snmq_ref_compnr), '0'), 38, 10) is null and 
                    src:grmq_snmq_ref_compnr is not null) THEN
                    'grmq_snmq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grmq_snmq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grms_snms_ref_compnr), '0'), 38, 10) is null and 
                    src:grms_snms_ref_compnr is not null) THEN
                    'grms_snms_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grms_snms_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrn_ref_compnr), '0'), 38, 10) is null and 
                    src:grrn_ref_compnr is not null) THEN
                    'grrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrn_srrn_ref_compnr), '0'), 38, 10) is null and 
                    src:grrn_srrn_ref_compnr is not null) THEN
                    'grrn_srrn_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grrn_srrn_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrq_snrq_ref_compnr), '0'), 38, 10) is null and 
                    src:grrq_snrq_ref_compnr is not null) THEN
                    'grrq_snrq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grrq_snrq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrr_ref_compnr), '0'), 38, 10) is null and 
                    src:grrr_ref_compnr is not null) THEN
                    'grrr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grrr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrr_srrr_ref_compnr), '0'), 38, 10) is null and 
                    src:grrr_srrr_ref_compnr is not null) THEN
                    'grrr_srrr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grrr_srrr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grsb_snsb_ref_compnr), '0'), 38, 10) is null and 
                    src:grsb_snsb_ref_compnr is not null) THEN
                    'grsb_snsb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grsb_snsb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grsc_snsc_ref_compnr), '0'), 38, 10) is null and 
                    src:grsc_snsc_ref_compnr is not null) THEN
                    'grsc_snsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grsc_snsc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grso_snso_ref_compnr), '0'), 38, 10) is null and 
                    src:grso_snso_ref_compnr is not null) THEN
                    'grso_snso_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grso_snso_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grsq_snsq_ref_compnr), '0'), 38, 10) is null and 
                    src:grsq_snsq_ref_compnr is not null) THEN
                    'grsq_snsq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grsq_snsq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grwo_snwo_ref_compnr), '0'), 38, 10) is null and 
                    src:grwo_snwo_ref_compnr is not null) THEN
                    'grwo_snwo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:grwo_snwo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcip_ref_compnr), '0'), 38, 10) is null and 
                    src:lcip_ref_compnr is not null) THEN
                    'lcip_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcip_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcop_ref_compnr), '0'), 38, 10) is null and 
                    src:lcop_ref_compnr is not null) THEN
                    'lcop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:lcop_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:mwrh_ref_compnr is not null) THEN
                    'mwrh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mwrh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsc_srsc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsc_srsc_ref_compnr is not null) THEN
                    'ngsc_srsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsc_srsc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pddp_ref_compnr), '0'), 38, 10) is null and 
                    src:pddp_ref_compnr is not null) THEN
                    'pddp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pddp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtim), '0'), 38, 10) is null and 
                    src:qtim is not null) THEN
                    'qtim contains a non-numeric value : \'' || AS_VARCHAR(src:qtim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is null and 
                    src:rste_ref_compnr is not null) THEN
                    'rste_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:rste_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) THEN
                    'sfsi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sfsi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slrt_ref_compnr), '0'), 38, 10) is null and 
                    src:slrt_ref_compnr is not null) THEN
                    'slrt_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:slrt_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srep_ref_compnr), '0'), 38, 10) is null and 
                    src:srep_ref_compnr is not null) THEN
                    'srep_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:srep_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtp), '0'), 38, 10) is null and 
                    src:srtp is not null) THEN
                    'srtp contains a non-numeric value : \'' || AS_VARCHAR(src:srtp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wctp), '0'), 38, 10) is null and 
                    src:wctp is not null) THEN
                    'wctp contains a non-numeric value : \'' || AS_VARCHAR(src:wctp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whip_ref_compnr), '0'), 38, 10) is null and 
                    src:whip_ref_compnr is not null) THEN
                    'whip_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:whip_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whop_ref_compnr), '0'), 38, 10) is null and 
                    src:whop_ref_compnr is not null) THEN
                    'whop_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:whop_ref_compnr) || '\'' WHEN 
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
                                    
                src:cwoc,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TSMDM100 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbwc), '0'), 38, 10) is null and 
                    src:cbwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clrt_ref_compnr), '0'), 38, 10) is null and 
                    src:clrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cste_ref_compnr), '0'), 38, 10) is null and 
                    src:cste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:cwrh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dste_ref_compnr), '0'), 38, 10) is null and 
                    src:dste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:fwrh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grca_snca_ref_compnr), '0'), 38, 10) is null and 
                    src:grca_snca_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grcl_sncl_ref_compnr), '0'), 38, 10) is null and 
                    src:grcl_sncl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grcq_sncq_ref_compnr), '0'), 38, 10) is null and 
                    src:grcq_sncq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grfc_snfc_ref_compnr), '0'), 38, 10) is null and 
                    src:grfc_snfc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grfq_snfq_ref_compnr), '0'), 38, 10) is null and 
                    src:grfq_snfq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grfr_snfr_ref_compnr), '0'), 38, 10) is null and 
                    src:grfr_snfr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grmq_snmq_ref_compnr), '0'), 38, 10) is null and 
                    src:grmq_snmq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grms_snms_ref_compnr), '0'), 38, 10) is null and 
                    src:grms_snms_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrn_ref_compnr), '0'), 38, 10) is null and 
                    src:grrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrn_srrn_ref_compnr), '0'), 38, 10) is null and 
                    src:grrn_srrn_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrq_snrq_ref_compnr), '0'), 38, 10) is null and 
                    src:grrq_snrq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrr_ref_compnr), '0'), 38, 10) is null and 
                    src:grrr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grrr_srrr_ref_compnr), '0'), 38, 10) is null and 
                    src:grrr_srrr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grsb_snsb_ref_compnr), '0'), 38, 10) is null and 
                    src:grsb_snsb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grsc_snsc_ref_compnr), '0'), 38, 10) is null and 
                    src:grsc_snsc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grso_snso_ref_compnr), '0'), 38, 10) is null and 
                    src:grso_snso_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grsq_snsq_ref_compnr), '0'), 38, 10) is null and 
                    src:grsq_snsq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grwo_snwo_ref_compnr), '0'), 38, 10) is null and 
                    src:grwo_snwo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcip_ref_compnr), '0'), 38, 10) is null and 
                    src:lcip_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcop_ref_compnr), '0'), 38, 10) is null and 
                    src:lcop_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:mwrh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsc_srsc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsc_srsc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pddp_ref_compnr), '0'), 38, 10) is null and 
                    src:pddp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qtim), '0'), 38, 10) is null and 
                    src:qtim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rste_ref_compnr), '0'), 38, 10) is null and 
                    src:rste_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfsi_ref_compnr), '0'), 38, 10) is null and 
                    src:sfsi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slrt_ref_compnr), '0'), 38, 10) is null and 
                    src:slrt_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srep_ref_compnr), '0'), 38, 10) is null and 
                    src:srep_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srtp), '0'), 38, 10) is null and 
                    src:srtp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wctp), '0'), 38, 10) is null and 
                    src:wctp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whip_ref_compnr), '0'), 38, 10) is null and 
                    src:whip_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whop_ref_compnr), '0'), 38, 10) is null and 
                    src:whop_ref_compnr is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)