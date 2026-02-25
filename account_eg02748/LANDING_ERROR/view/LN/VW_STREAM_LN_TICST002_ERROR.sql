CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TICST002_ERROR AS SELECT src, 'LN_TICST002' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahma), '0'), 38, 10) is null and 
                    src:ahma is not null) THEN
                    'ahma contains a non-numeric value : \'' || AS_VARCHAR(src:ahma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahmc), '0'), 38, 10) is null and 
                    src:ahmc is not null) THEN
                    'ahmc contains a non-numeric value : \'' || AS_VARCHAR(src:ahmc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahmq), '0'), 38, 10) is null and 
                    src:ahmq is not null) THEN
                    'ahmq contains a non-numeric value : \'' || AS_VARCHAR(src:ahmq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahms), '0'), 38, 10) is null and 
                    src:ahms is not null) THEN
                    'ahms contains a non-numeric value : \'' || AS_VARCHAR(src:ahms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahwq), '0'), 38, 10) is null and 
                    src:ahwq is not null) THEN
                    'ahwq contains a non-numeric value : \'' || AS_VARCHAR(src:ahwq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahws), '0'), 38, 10) is null and 
                    src:ahws is not null) THEN
                    'ahws contains a non-numeric value : \'' || AS_VARCHAR(src:ahws) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_1), '0'), 38, 10) is null and 
                    src:amcc_1 is not null) THEN
                    'amcc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_2), '0'), 38, 10) is null and 
                    src:amcc_2 is not null) THEN
                    'amcc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_3), '0'), 38, 10) is null and 
                    src:amcc_3 is not null) THEN
                    'amcc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_dwhc), '0'), 38, 10) is null and 
                    src:amcc_dwhc is not null) THEN
                    'amcc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_lclc), '0'), 38, 10) is null and 
                    src:amcc_lclc is not null) THEN
                    'amcc_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_refc), '0'), 38, 10) is null and 
                    src:amcc_refc is not null) THEN
                    'amcc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_rpc1), '0'), 38, 10) is null and 
                    src:amcc_rpc1 is not null) THEN
                    'amcc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_rpc2), '0'), 38, 10) is null and 
                    src:amcc_rpc2 is not null) THEN
                    'amcc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:amcc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_1), '0'), 38, 10) is null and 
                    src:aohc_1 is not null) THEN
                    'aohc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_2), '0'), 38, 10) is null and 
                    src:aohc_2 is not null) THEN
                    'aohc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_3), '0'), 38, 10) is null and 
                    src:aohc_3 is not null) THEN
                    'aohc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_dwhc), '0'), 38, 10) is null and 
                    src:aohc_dwhc is not null) THEN
                    'aohc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_lclc), '0'), 38, 10) is null and 
                    src:aohc_lclc is not null) THEN
                    'aohc_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_refc), '0'), 38, 10) is null and 
                    src:aohc_refc is not null) THEN
                    'aohc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_rpc1), '0'), 38, 10) is null and 
                    src:aohc_rpc1 is not null) THEN
                    'aohc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_rpc2), '0'), 38, 10) is null and 
                    src:aohc_rpc2 is not null) THEN
                    'aohc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:aohc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_1), '0'), 38, 10) is null and 
                    src:ascc_1 is not null) THEN
                    'ascc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_2), '0'), 38, 10) is null and 
                    src:ascc_2 is not null) THEN
                    'ascc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_3), '0'), 38, 10) is null and 
                    src:ascc_3 is not null) THEN
                    'ascc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_dwhc), '0'), 38, 10) is null and 
                    src:ascc_dwhc is not null) THEN
                    'ascc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_lclc), '0'), 38, 10) is null and 
                    src:ascc_lclc is not null) THEN
                    'ascc_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_refc), '0'), 38, 10) is null and 
                    src:ascc_refc is not null) THEN
                    'ascc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_rpc1), '0'), 38, 10) is null and 
                    src:ascc_rpc1 is not null) THEN
                    'ascc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_rpc2), '0'), 38, 10) is null and 
                    src:ascc_rpc2 is not null) THEN
                    'ascc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ascc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_1), '0'), 38, 10) is null and 
                    src:ascq_1 is not null) THEN
                    'ascq_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_2), '0'), 38, 10) is null and 
                    src:ascq_2 is not null) THEN
                    'ascq_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_3), '0'), 38, 10) is null and 
                    src:ascq_3 is not null) THEN
                    'ascq_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_dwhc), '0'), 38, 10) is null and 
                    src:ascq_dwhc is not null) THEN
                    'ascq_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_lclc), '0'), 38, 10) is null and 
                    src:ascq_lclc is not null) THEN
                    'ascq_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_refc), '0'), 38, 10) is null and 
                    src:ascq_refc is not null) THEN
                    'ascq_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_rpc1), '0'), 38, 10) is null and 
                    src:ascq_rpc1 is not null) THEN
                    'ascq_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_rpc2), '0'), 38, 10) is null and 
                    src:ascq_rpc2 is not null) THEN
                    'ascq_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ascq_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashc), '0'), 38, 10) is null and 
                    src:ashc is not null) THEN
                    'ashc contains a non-numeric value : \'' || AS_VARCHAR(src:ashc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashm), '0'), 38, 10) is null and 
                    src:ashm is not null) THEN
                    'ashm contains a non-numeric value : \'' || AS_VARCHAR(src:ashm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashq), '0'), 38, 10) is null and 
                    src:ashq is not null) THEN
                    'ashq contains a non-numeric value : \'' || AS_VARCHAR(src:ashq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashs), '0'), 38, 10) is null and 
                    src:ashs is not null) THEN
                    'ashs contains a non-numeric value : \'' || AS_VARCHAR(src:ashs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asmq), '0'), 38, 10) is null and 
                    src:asmq is not null) THEN
                    'asmq contains a non-numeric value : \'' || AS_VARCHAR(src:asmq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asms), '0'), 38, 10) is null and 
                    src:asms is not null) THEN
                    'asms contains a non-numeric value : \'' || AS_VARCHAR(src:asms) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_1), '0'), 38, 10) is null and 
                    src:awgc_1 is not null) THEN
                    'awgc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_2), '0'), 38, 10) is null and 
                    src:awgc_2 is not null) THEN
                    'awgc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_3), '0'), 38, 10) is null and 
                    src:awgc_3 is not null) THEN
                    'awgc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_dwhc), '0'), 38, 10) is null and 
                    src:awgc_dwhc is not null) THEN
                    'awgc_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_lclc), '0'), 38, 10) is null and 
                    src:awgc_lclc is not null) THEN
                    'awgc_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_refc), '0'), 38, 10) is null and 
                    src:awgc_refc is not null) THEN
                    'awgc_refc contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_rpc1), '0'), 38, 10) is null and 
                    src:awgc_rpc1 is not null) THEN
                    'awgc_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_rpc2), '0'), 38, 10) is null and 
                    src:awgc_rpc2 is not null) THEN
                    'awgc_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:awgc_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) THEN
                    'bpid_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bpid_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) THEN
                    'cmdt contains a non-timestamp value : \'' || AS_VARCHAR(src:cmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cums), '0'), 38, 10) is null and 
                    src:cums is not null) THEN
                    'cums contains a non-numeric value : \'' || AS_VARCHAR(src:cums) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) THEN
                    'cwoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is null and 
                    src:dptm_fcmp is not null) THEN
                    'dptm_fcmp contains a non-numeric value : \'' || AS_VARCHAR(src:dptm_fcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eshc), '0'), 38, 10) is null and 
                    src:eshc is not null) THEN
                    'eshc contains a non-numeric value : \'' || AS_VARCHAR(src:eshc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eshm), '0'), 38, 10) is null and 
                    src:eshm is not null) THEN
                    'eshm contains a non-numeric value : \'' || AS_VARCHAR(src:eshm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdur), '0'), 38, 10) is null and 
                    src:fdur is not null) THEN
                    'fdur contains a non-numeric value : \'' || AS_VARCHAR(src:fdur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frso), '0'), 38, 10) is null and 
                    src:frso is not null) THEN
                    'frso contains a non-numeric value : \'' || AS_VARCHAR(src:frso) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrem), '0'), 38, 10) is null and 
                    src:hrem is not null) THEN
                    'hrem contains a non-numeric value : \'' || AS_VARCHAR(src:hrem) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrmc), '0'), 38, 10) is null and 
                    src:hrmc is not null) THEN
                    'hrmc contains a non-numeric value : \'' || AS_VARCHAR(src:hrmc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_1), '0'), 38, 10) is null and 
                    src:mccs_1 is not null) THEN
                    'mccs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_2), '0'), 38, 10) is null and 
                    src:mccs_2 is not null) THEN
                    'mccs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_3), '0'), 38, 10) is null and 
                    src:mccs_3 is not null) THEN
                    'mccs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_dwhc), '0'), 38, 10) is null and 
                    src:mccs_dwhc is not null) THEN
                    'mccs_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_lclc), '0'), 38, 10) is null and 
                    src:mccs_lclc is not null) THEN
                    'mccs_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_refc), '0'), 38, 10) is null and 
                    src:mccs_refc is not null) THEN
                    'mccs_refc contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_rpc1), '0'), 38, 10) is null and 
                    src:mccs_rpc1 is not null) THEN
                    'mccs_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_rpc2), '0'), 38, 10) is null and 
                    src:mccs_rpc2 is not null) THEN
                    'mccs_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:mccs_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcno_ref_compnr), '0'), 38, 10) is null and 
                    src:mcno_ref_compnr is not null) THEN
                    'mcno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:mcno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcoc), '0'), 38, 10) is null and 
                    src:mcoc is not null) THEN
                    'mcoc contains a non-numeric value : \'' || AS_VARCHAR(src:mcoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopr), '0'), 38, 10) is null and 
                    src:mopr is not null) THEN
                    'mopr contains a non-numeric value : \'' || AS_VARCHAR(src:mopr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:most), '0'), 38, 10) is null and 
                    src:most is not null) THEN
                    'most contains a non-numeric value : \'' || AS_VARCHAR(src:most) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nomc), '0'), 38, 10) is null and 
                    src:nomc is not null) THEN
                    'nomc contains a non-numeric value : \'' || AS_VARCHAR(src:nomc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_1), '0'), 38, 10) is null and 
                    src:ohcs_1 is not null) THEN
                    'ohcs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_2), '0'), 38, 10) is null and 
                    src:ohcs_2 is not null) THEN
                    'ohcs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_3), '0'), 38, 10) is null and 
                    src:ohcs_3 is not null) THEN
                    'ohcs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_dwhc), '0'), 38, 10) is null and 
                    src:ohcs_dwhc is not null) THEN
                    'ohcs_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_lclc), '0'), 38, 10) is null and 
                    src:ohcs_lclc is not null) THEN
                    'ohcs_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_refc), '0'), 38, 10) is null and 
                    src:ohcs_refc is not null) THEN
                    'ohcs_refc contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_rpc1), '0'), 38, 10) is null and 
                    src:ohcs_rpc1 is not null) THEN
                    'ohcs_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_rpc2), '0'), 38, 10) is null and 
                    src:ohcs_rpc2 is not null) THEN
                    'ohcs_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:ohcs_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opno), '0'), 38, 10) is null and 
                    src:opno is not null) THEN
                    'opno contains a non-numeric value : \'' || AS_VARCHAR(src:opno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprc_ref_compnr), '0'), 38, 10) is null and 
                    src:oprc_ref_compnr is not null) THEN
                    'oprc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:oprc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pcdt), '1900-01-01')) is null and 
                    src:pcdt is not null) THEN
                    'pcdt contains a non-timestamp value : \'' || AS_VARCHAR(src:pcdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) THEN
                    'pdno_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:pdno_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prte), '0'), 38, 10) is null and 
                    src:prte is not null) THEN
                    'prte contains a non-numeric value : \'' || AS_VARCHAR(src:prte) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcmp), '0'), 38, 10) is null and 
                    src:qcmp is not null) THEN
                    'qcmp contains a non-numeric value : \'' || AS_VARCHAR(src:qcmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpln), '0'), 38, 10) is null and 
                    src:qpln is not null) THEN
                    'qpln contains a non-numeric value : \'' || AS_VARCHAR(src:qpln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar), '0'), 38, 10) is null and 
                    src:qqar is not null) THEN
                    'qqar contains a non-numeric value : \'' || AS_VARCHAR(src:qqar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is null and 
                    src:qqar_buar is not null) THEN
                    'qqar_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is null and 
                    src:qqar_buln is not null) THEN
                    'qqar_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is null and 
                    src:qqar_bupc is not null) THEN
                    'qqar_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is null and 
                    src:qqar_butm is not null) THEN
                    'qqar_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is null and 
                    src:qqar_buvl is not null) THEN
                    'qqar_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is null and 
                    src:qqar_buwg is not null) THEN
                    'qqar_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qqar_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc), '0'), 38, 10) is null and 
                    src:qrjc is not null) THEN
                    'qrjc contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is null and 
                    src:qrjc_buar is not null) THEN
                    'qrjc_buar contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is null and 
                    src:qrjc_buln is not null) THEN
                    'qrjc_buln contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is null and 
                    src:qrjc_bupc is not null) THEN
                    'qrjc_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is null and 
                    src:qrjc_butm is not null) THEN
                    'qrjc_butm contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is null and 
                    src:qrjc_buvl is not null) THEN
                    'qrjc_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is null and 
                    src:qrjc_buwg is not null) THEN
                    'qrjc_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:qrjc_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) THEN
                    'runi contains a non-numeric value : \'' || AS_VARCHAR(src:runi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rutm), '0'), 38, 10) is null and 
                    src:rutm is not null) THEN
                    'rutm contains a non-numeric value : \'' || AS_VARCHAR(src:rutm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_1), '0'), 38, 10) is null and 
                    src:sccs_1 is not null) THEN
                    'sccs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_2), '0'), 38, 10) is null and 
                    src:sccs_2 is not null) THEN
                    'sccs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_3), '0'), 38, 10) is null and 
                    src:sccs_3 is not null) THEN
                    'sccs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_dwhc), '0'), 38, 10) is null and 
                    src:sccs_dwhc is not null) THEN
                    'sccs_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_lclc), '0'), 38, 10) is null and 
                    src:sccs_lclc is not null) THEN
                    'sccs_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_refc), '0'), 38, 10) is null and 
                    src:sccs_refc is not null) THEN
                    'sccs_refc contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_rpc1), '0'), 38, 10) is null and 
                    src:sccs_rpc1 is not null) THEN
                    'sccs_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_rpc2), '0'), 38, 10) is null and 
                    src:sccs_rpc2 is not null) THEN
                    'sccs_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:sccs_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) THEN
                    'scpq contains a non-numeric value : \'' || AS_VARCHAR(src:scpq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is null and 
                    src:scpq_buar is not null) THEN
                    'scpq_buar contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buar) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is null and 
                    src:scpq_buln is not null) THEN
                    'scpq_buln contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is null and 
                    src:scpq_bupc is not null) THEN
                    'scpq_bupc contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_bupc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is null and 
                    src:scpq_butm is not null) THEN
                    'scpq_butm contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_butm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is null and 
                    src:scpq_buvl is not null) THEN
                    'scpq_buvl contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buvl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is null and 
                    src:scpq_buwg is not null) THEN
                    'scpq_buwg contains a non-numeric value : \'' || AS_VARCHAR(src:scpq_buwg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subr), '0'), 38, 10) is null and 
                    src:subr is not null) THEN
                    'subr contains a non-numeric value : \'' || AS_VARCHAR(src:subr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutm), '0'), 38, 10) is null and 
                    src:sutm is not null) THEN
                    'sutm contains a non-numeric value : \'' || AS_VARCHAR(src:sutm) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_1), '0'), 38, 10) is null and 
                    src:wgcs_1 is not null) THEN
                    'wgcs_1 contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_2), '0'), 38, 10) is null and 
                    src:wgcs_2 is not null) THEN
                    'wgcs_2 contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_3), '0'), 38, 10) is null and 
                    src:wgcs_3 is not null) THEN
                    'wgcs_3 contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_dwhc), '0'), 38, 10) is null and 
                    src:wgcs_dwhc is not null) THEN
                    'wgcs_dwhc contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_dwhc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_lclc), '0'), 38, 10) is null and 
                    src:wgcs_lclc is not null) THEN
                    'wgcs_lclc contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_lclc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_refc), '0'), 38, 10) is null and 
                    src:wgcs_refc is not null) THEN
                    'wgcs_refc contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_refc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_rpc1), '0'), 38, 10) is null and 
                    src:wgcs_rpc1 is not null) THEN
                    'wgcs_rpc1 contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_rpc1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_rpc2), '0'), 38, 10) is null and 
                    src:wgcs_rpc2 is not null) THEN
                    'wgcs_rpc2 contains a non-numeric value : \'' || AS_VARCHAR(src:wgcs_rpc2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:yldp), '0'), 38, 10) is null and 
                    src:yldp is not null) THEN
                    'yldp contains a non-numeric value : \'' || AS_VARCHAR(src:yldp) || '\'' WHEN 
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
                                    
                src:pdno,
                src:compnr,
                src:opno  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TICST002 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahma), '0'), 38, 10) is null and 
                    src:ahma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahmc), '0'), 38, 10) is null and 
                    src:ahmc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahmq), '0'), 38, 10) is null and 
                    src:ahmq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahms), '0'), 38, 10) is null and 
                    src:ahms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahwq), '0'), 38, 10) is null and 
                    src:ahwq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ahws), '0'), 38, 10) is null and 
                    src:ahws is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_1), '0'), 38, 10) is null and 
                    src:amcc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_2), '0'), 38, 10) is null and 
                    src:amcc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_3), '0'), 38, 10) is null and 
                    src:amcc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_dwhc), '0'), 38, 10) is null and 
                    src:amcc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_lclc), '0'), 38, 10) is null and 
                    src:amcc_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_refc), '0'), 38, 10) is null and 
                    src:amcc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_rpc1), '0'), 38, 10) is null and 
                    src:amcc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amcc_rpc2), '0'), 38, 10) is null and 
                    src:amcc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_1), '0'), 38, 10) is null and 
                    src:aohc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_2), '0'), 38, 10) is null and 
                    src:aohc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_3), '0'), 38, 10) is null and 
                    src:aohc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_dwhc), '0'), 38, 10) is null and 
                    src:aohc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_lclc), '0'), 38, 10) is null and 
                    src:aohc_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_refc), '0'), 38, 10) is null and 
                    src:aohc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_rpc1), '0'), 38, 10) is null and 
                    src:aohc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aohc_rpc2), '0'), 38, 10) is null and 
                    src:aohc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_1), '0'), 38, 10) is null and 
                    src:ascc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_2), '0'), 38, 10) is null and 
                    src:ascc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_3), '0'), 38, 10) is null and 
                    src:ascc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_dwhc), '0'), 38, 10) is null and 
                    src:ascc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_lclc), '0'), 38, 10) is null and 
                    src:ascc_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_refc), '0'), 38, 10) is null and 
                    src:ascc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_rpc1), '0'), 38, 10) is null and 
                    src:ascc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascc_rpc2), '0'), 38, 10) is null and 
                    src:ascc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_1), '0'), 38, 10) is null and 
                    src:ascq_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_2), '0'), 38, 10) is null and 
                    src:ascq_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_3), '0'), 38, 10) is null and 
                    src:ascq_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_dwhc), '0'), 38, 10) is null and 
                    src:ascq_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_lclc), '0'), 38, 10) is null and 
                    src:ascq_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_refc), '0'), 38, 10) is null and 
                    src:ascq_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_rpc1), '0'), 38, 10) is null and 
                    src:ascq_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ascq_rpc2), '0'), 38, 10) is null and 
                    src:ascq_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashc), '0'), 38, 10) is null and 
                    src:ashc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashm), '0'), 38, 10) is null and 
                    src:ashm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashq), '0'), 38, 10) is null and 
                    src:ashq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ashs), '0'), 38, 10) is null and 
                    src:ashs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asmq), '0'), 38, 10) is null and 
                    src:asmq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asms), '0'), 38, 10) is null and 
                    src:asms is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_1), '0'), 38, 10) is null and 
                    src:awgc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_2), '0'), 38, 10) is null and 
                    src:awgc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_3), '0'), 38, 10) is null and 
                    src:awgc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_dwhc), '0'), 38, 10) is null and 
                    src:awgc_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_lclc), '0'), 38, 10) is null and 
                    src:awgc_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_refc), '0'), 38, 10) is null and 
                    src:awgc_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_rpc1), '0'), 38, 10) is null and 
                    src:awgc_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:awgc_rpc2), '0'), 38, 10) is null and 
                    src:awgc_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bpid_ref_compnr), '0'), 38, 10) is null and 
                    src:bpid_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:cmdt), '1900-01-01')) is null and 
                    src:cmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cums), '0'), 38, 10) is null and 
                    src:cums is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwoc_ref_compnr), '0'), 38, 10) is null and 
                    src:cwoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptm_fcmp), '0'), 38, 10) is null and 
                    src:dptm_fcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eshc), '0'), 38, 10) is null and 
                    src:eshc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eshm), '0'), 38, 10) is null and 
                    src:eshm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fdur), '0'), 38, 10) is null and 
                    src:fdur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frso), '0'), 38, 10) is null and 
                    src:frso is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrem), '0'), 38, 10) is null and 
                    src:hrem is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hrmc), '0'), 38, 10) is null and 
                    src:hrmc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_1), '0'), 38, 10) is null and 
                    src:mccs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_2), '0'), 38, 10) is null and 
                    src:mccs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_3), '0'), 38, 10) is null and 
                    src:mccs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_dwhc), '0'), 38, 10) is null and 
                    src:mccs_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_lclc), '0'), 38, 10) is null and 
                    src:mccs_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_refc), '0'), 38, 10) is null and 
                    src:mccs_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_rpc1), '0'), 38, 10) is null and 
                    src:mccs_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mccs_rpc2), '0'), 38, 10) is null and 
                    src:mccs_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcno_ref_compnr), '0'), 38, 10) is null and 
                    src:mcno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcoc), '0'), 38, 10) is null and 
                    src:mcoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopr), '0'), 38, 10) is null and 
                    src:mopr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:most), '0'), 38, 10) is null and 
                    src:most is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:nomc), '0'), 38, 10) is null and 
                    src:nomc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_1), '0'), 38, 10) is null and 
                    src:ohcs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_2), '0'), 38, 10) is null and 
                    src:ohcs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_3), '0'), 38, 10) is null and 
                    src:ohcs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_dwhc), '0'), 38, 10) is null and 
                    src:ohcs_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_lclc), '0'), 38, 10) is null and 
                    src:ohcs_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_refc), '0'), 38, 10) is null and 
                    src:ohcs_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_rpc1), '0'), 38, 10) is null and 
                    src:ohcs_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ohcs_rpc2), '0'), 38, 10) is null and 
                    src:ohcs_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opno), '0'), 38, 10) is null and 
                    src:opno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oprc_ref_compnr), '0'), 38, 10) is null and 
                    src:oprc_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pcdt), '1900-01-01')) is null and 
                    src:pcdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdno_ref_compnr), '0'), 38, 10) is null and 
                    src:pdno_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prte), '0'), 38, 10) is null and 
                    src:prte is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qcmp), '0'), 38, 10) is null and 
                    src:qcmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpln), '0'), 38, 10) is null and 
                    src:qpln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar), '0'), 38, 10) is null and 
                    src:qqar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buar), '0'), 38, 10) is null and 
                    src:qqar_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buln), '0'), 38, 10) is null and 
                    src:qqar_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_bupc), '0'), 38, 10) is null and 
                    src:qqar_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_butm), '0'), 38, 10) is null and 
                    src:qqar_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buvl), '0'), 38, 10) is null and 
                    src:qqar_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qqar_buwg), '0'), 38, 10) is null and 
                    src:qqar_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc), '0'), 38, 10) is null and 
                    src:qrjc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buar), '0'), 38, 10) is null and 
                    src:qrjc_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buln), '0'), 38, 10) is null and 
                    src:qrjc_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_bupc), '0'), 38, 10) is null and 
                    src:qrjc_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_butm), '0'), 38, 10) is null and 
                    src:qrjc_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buvl), '0'), 38, 10) is null and 
                    src:qrjc_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qrjc_buwg), '0'), 38, 10) is null and 
                    src:qrjc_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:runi), '0'), 38, 10) is null and 
                    src:runi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rutm), '0'), 38, 10) is null and 
                    src:rutm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_1), '0'), 38, 10) is null and 
                    src:sccs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_2), '0'), 38, 10) is null and 
                    src:sccs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_3), '0'), 38, 10) is null and 
                    src:sccs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_dwhc), '0'), 38, 10) is null and 
                    src:sccs_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_lclc), '0'), 38, 10) is null and 
                    src:sccs_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_refc), '0'), 38, 10) is null and 
                    src:sccs_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_rpc1), '0'), 38, 10) is null and 
                    src:sccs_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sccs_rpc2), '0'), 38, 10) is null and 
                    src:sccs_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq), '0'), 38, 10) is null and 
                    src:scpq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buar), '0'), 38, 10) is null and 
                    src:scpq_buar is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buln), '0'), 38, 10) is null and 
                    src:scpq_buln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_bupc), '0'), 38, 10) is null and 
                    src:scpq_bupc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_butm), '0'), 38, 10) is null and 
                    src:scpq_butm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buvl), '0'), 38, 10) is null and 
                    src:scpq_buvl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scpq_buwg), '0'), 38, 10) is null and 
                    src:scpq_buwg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subr), '0'), 38, 10) is null and 
                    src:subr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sutm), '0'), 38, 10) is null and 
                    src:sutm is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_1), '0'), 38, 10) is null and 
                    src:wgcs_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_2), '0'), 38, 10) is null and 
                    src:wgcs_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_3), '0'), 38, 10) is null and 
                    src:wgcs_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_dwhc), '0'), 38, 10) is null and 
                    src:wgcs_dwhc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_lclc), '0'), 38, 10) is null and 
                    src:wgcs_lclc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_refc), '0'), 38, 10) is null and 
                    src:wgcs_refc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_rpc1), '0'), 38, 10) is null and 
                    src:wgcs_rpc1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wgcs_rpc2), '0'), 38, 10) is null and 
                    src:wgcs_rpc2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:yldp), '0'), 38, 10) is null and 
                    src:yldp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)