CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TFACP200_ERROR AS SELECT src, 'LN_TFACP200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adty), '0'), 38, 10) is null and 
                    src:adty is not null) THEN
                    'adty contains a non-numeric value : \'' || AS_VARCHAR(src:adty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afpy), '0'), 38, 10) is null and 
                    src:afpy is not null) THEN
                    'afpy contains a non-numeric value : \'' || AS_VARCHAR(src:afpy) || '\'' WHEN 
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
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_dtwc), '0'), 38, 10) is null and 
                    src:amth_dtwc is not null) THEN
                    'amth_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:amth_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_rfrc), '0'), 38, 10) is null and 
                    src:amth_rfrc is not null) THEN
                    'amth_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:amth_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amti), '0'), 38, 10) is null and 
                    src:amti is not null) THEN
                    'amti contains a non-numeric value : \'' || AS_VARCHAR(src:amti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) THEN
                    'appr contains a non-numeric value : \'' || AS_VARCHAR(src:appr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:autm), '0'), 38, 10) is null and 
                    src:autm is not null) THEN
                    'autm contains a non-numeric value : \'' || AS_VARCHAR(src:autm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_1), '0'), 38, 10) is null and 
                    src:baca_1 is not null) THEN
                    'baca_1 contains a non-numeric value : \'' || AS_VARCHAR(src:baca_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_2), '0'), 38, 10) is null and 
                    src:baca_2 is not null) THEN
                    'baca_2 contains a non-numeric value : \'' || AS_VARCHAR(src:baca_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_3), '0'), 38, 10) is null and 
                    src:baca_3 is not null) THEN
                    'baca_3 contains a non-numeric value : \'' || AS_VARCHAR(src:baca_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_dtwc), '0'), 38, 10) is null and 
                    src:baca_dtwc is not null) THEN
                    'baca_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:baca_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_invc), '0'), 38, 10) is null and 
                    src:baca_invc is not null) THEN
                    'baca_invc contains a non-numeric value : \'' || AS_VARCHAR(src:baca_invc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_rfrc), '0'), 38, 10) is null and 
                    src:baca_rfrc is not null) THEN
                    'baca_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:baca_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baco), '0'), 38, 10) is null and 
                    src:baco is not null) THEN
                    'baco contains a non-numeric value : \'' || AS_VARCHAR(src:baco) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_1), '0'), 38, 10) is null and 
                    src:bahc_1 is not null) THEN
                    'bahc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:bahc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_2), '0'), 38, 10) is null and 
                    src:bahc_2 is not null) THEN
                    'bahc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:bahc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_3), '0'), 38, 10) is null and 
                    src:bahc_3 is not null) THEN
                    'bahc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:bahc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_dtwc), '0'), 38, 10) is null and 
                    src:bahc_dtwc is not null) THEN
                    'bahc_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:bahc_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_rfrc), '0'), 38, 10) is null and 
                    src:bahc_rfrc is not null) THEN
                    'bahc_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:bahc_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bala), '0'), 38, 10) is null and 
                    src:bala is not null) THEN
                    'bala contains a non-numeric value : \'' || AS_VARCHAR(src:bala) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balc), '0'), 38, 10) is null and 
                    src:balc is not null) THEN
                    'balc contains a non-numeric value : \'' || AS_VARCHAR(src:balc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_1), '0'), 38, 10) is null and 
                    src:balh_1 is not null) THEN
                    'balh_1 contains a non-numeric value : \'' || AS_VARCHAR(src:balh_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_2), '0'), 38, 10) is null and 
                    src:balh_2 is not null) THEN
                    'balh_2 contains a non-numeric value : \'' || AS_VARCHAR(src:balh_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_3), '0'), 38, 10) is null and 
                    src:balh_3 is not null) THEN
                    'balh_3 contains a non-numeric value : \'' || AS_VARCHAR(src:balh_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_dtwc), '0'), 38, 10) is null and 
                    src:balh_dtwc is not null) THEN
                    'balh_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:balh_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_rfrc), '0'), 38, 10) is null and 
                    src:balh_rfrc is not null) THEN
                    'balh_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:balh_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:basi), '0'), 38, 10) is null and 
                    src:basi is not null) THEN
                    'basi contains a non-numeric value : \'' || AS_VARCHAR(src:basi) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bdat), '1900-01-01')) is null and 
                    src:bdat is not null) THEN
                    'bdat contains a non-timestamp value : \'' || AS_VARCHAR(src:bdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdsp), '0'), 38, 10) is null and 
                    src:bdsp is not null) THEN
                    'bdsp contains a non-numeric value : \'' || AS_VARCHAR(src:bdsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blac), '0'), 38, 10) is null and 
                    src:blac is not null) THEN
                    'blac contains a non-numeric value : \'' || AS_VARCHAR(src:blac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc_ref_compnr), '0'), 38, 10) is null and 
                    src:bloc_ref_compnr is not null) THEN
                    'bloc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bloc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bref_ref_compnr), '0'), 38, 10) is null and 
                    src:bref_ref_compnr is not null) THEN
                    'bref_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bref_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btno), '0'), 38, 10) is null and 
                    src:btno is not null) THEN
                    'btno contains a non-numeric value : \'' || AS_VARCHAR(src:btno) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bust), '0'), 38, 10) is null and 
                    src:bust is not null) THEN
                    'bust contains a non-numeric value : \'' || AS_VARCHAR(src:bust) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) THEN
                    'ccrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) THEN
                    'ccur_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccur_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_1), '0'), 38, 10) is null and 
                    src:cdam_1 is not null) THEN
                    'cdam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:cdam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_2), '0'), 38, 10) is null and 
                    src:cdam_2 is not null) THEN
                    'cdam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:cdam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_3), '0'), 38, 10) is null and 
                    src:cdam_3 is not null) THEN
                    'cdam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:cdam_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_dtwc), '0'), 38, 10) is null and 
                    src:cdam_dtwc is not null) THEN
                    'cdam_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:cdam_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_invc), '0'), 38, 10) is null and 
                    src:cdam_invc is not null) THEN
                    'cdam_invc contains a non-numeric value : \'' || AS_VARCHAR(src:cdam_invc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_rfrc), '0'), 38, 10) is null and 
                    src:cdam_rfrc is not null) THEN
                    'cdam_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:cdam_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrs_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrs_ref_compnr is not null) THEN
                    'cfrs_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cfrs_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) THEN
                    'cpay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cpay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) THEN
                    'cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dapr), '0'), 38, 10) is null and 
                    src:dapr is not null) THEN
                    'dapr contains a non-numeric value : \'' || AS_VARCHAR(src:dapr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1a), '0'), 38, 10) is null and 
                    src:dc1a is not null) THEN
                    'dc1a contains a non-numeric value : \'' || AS_VARCHAR(src:dc1a) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_1), '0'), 38, 10) is null and 
                    src:dc1h_1 is not null) THEN
                    'dc1h_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dc1h_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_2), '0'), 38, 10) is null and 
                    src:dc1h_2 is not null) THEN
                    'dc1h_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dc1h_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_3), '0'), 38, 10) is null and 
                    src:dc1h_3 is not null) THEN
                    'dc1h_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dc1h_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_dtwc), '0'), 38, 10) is null and 
                    src:dc1h_dtwc is not null) THEN
                    'dc1h_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:dc1h_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_rfrc), '0'), 38, 10) is null and 
                    src:dc1h_rfrc is not null) THEN
                    'dc1h_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:dc1h_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1i), '0'), 38, 10) is null and 
                    src:dc1i is not null) THEN
                    'dc1i contains a non-numeric value : \'' || AS_VARCHAR(src:dc1i) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2a), '0'), 38, 10) is null and 
                    src:dc2a is not null) THEN
                    'dc2a contains a non-numeric value : \'' || AS_VARCHAR(src:dc2a) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_1), '0'), 38, 10) is null and 
                    src:dc2h_1 is not null) THEN
                    'dc2h_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dc2h_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_2), '0'), 38, 10) is null and 
                    src:dc2h_2 is not null) THEN
                    'dc2h_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dc2h_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_3), '0'), 38, 10) is null and 
                    src:dc2h_3 is not null) THEN
                    'dc2h_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dc2h_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_dtwc), '0'), 38, 10) is null and 
                    src:dc2h_dtwc is not null) THEN
                    'dc2h_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:dc2h_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_rfrc), '0'), 38, 10) is null and 
                    src:dc2h_rfrc is not null) THEN
                    'dc2h_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:dc2h_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2i), '0'), 38, 10) is null and 
                    src:dc2i is not null) THEN
                    'dc2i contains a non-numeric value : \'' || AS_VARCHAR(src:dc2i) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3a), '0'), 38, 10) is null and 
                    src:dc3a is not null) THEN
                    'dc3a contains a non-numeric value : \'' || AS_VARCHAR(src:dc3a) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_1), '0'), 38, 10) is null and 
                    src:dc3h_1 is not null) THEN
                    'dc3h_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dc3h_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_2), '0'), 38, 10) is null and 
                    src:dc3h_2 is not null) THEN
                    'dc3h_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dc3h_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_3), '0'), 38, 10) is null and 
                    src:dc3h_3 is not null) THEN
                    'dc3h_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dc3h_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_dtwc), '0'), 38, 10) is null and 
                    src:dc3h_dtwc is not null) THEN
                    'dc3h_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:dc3h_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_rfrc), '0'), 38, 10) is null and 
                    src:dc3h_rfrc is not null) THEN
                    'dc3h_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:dc3h_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3i), '0'), 38, 10) is null and 
                    src:dc3i is not null) THEN
                    'dc3i contains a non-numeric value : \'' || AS_VARCHAR(src:dc3i) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:did1), '1900-01-01')) is null and 
                    src:did1 is not null) THEN
                    'did1 contains a non-timestamp value : \'' || AS_VARCHAR(src:did1) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:did2), '1900-01-01')) is null and 
                    src:did2 is not null) THEN
                    'did2 contains a non-timestamp value : \'' || AS_VARCHAR(src:did2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:did3), '1900-01-01')) is null and 
                    src:did3 is not null) THEN
                    'did3 contains a non-timestamp value : \'' || AS_VARCHAR(src:did3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:doca), '0'), 38, 10) is null and 
                    src:doca is not null) THEN
                    'doca contains a non-numeric value : \'' || AS_VARCHAR(src:doca) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:docd), '1900-01-01')) is null and 
                    src:docd is not null) THEN
                    'docd contains a non-timestamp value : \'' || AS_VARCHAR(src:docd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) THEN
                    'docn contains a non-numeric value : \'' || AS_VARCHAR(src:docn) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dued), '1900-01-01')) is null and 
                    src:dued is not null) THEN
                    'dued contains a non-timestamp value : \'' || AS_VARCHAR(src:dued) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edtp), '0'), 38, 10) is null and 
                    src:edtp is not null) THEN
                    'edtp contains a non-numeric value : \'' || AS_VARCHAR(src:edtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enia), '0'), 38, 10) is null and 
                    src:enia is not null) THEN
                    'enia contains a non-numeric value : \'' || AS_VARCHAR(src:enia) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iadr_ref_compnr), '0'), 38, 10) is null and 
                    src:iadr_ref_compnr is not null) THEN
                    'iadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:iadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) THEN
                    'ifbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ifbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ildp), '0'), 38, 10) is null and 
                    src:ildp is not null) THEN
                    'ildp contains a non-numeric value : \'' || AS_VARCHAR(src:ildp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:irdt), '1900-01-01')) is null and 
                    src:irdt is not null) THEN
                    'irdt contains a non-timestamp value : \'' || AS_VARCHAR(src:irdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lapa), '0'), 38, 10) is null and 
                    src:lapa is not null) THEN
                    'lapa contains a non-numeric value : \'' || AS_VARCHAR(src:lapa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_1), '0'), 38, 10) is null and 
                    src:laph_1 is not null) THEN
                    'laph_1 contains a non-numeric value : \'' || AS_VARCHAR(src:laph_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_2), '0'), 38, 10) is null and 
                    src:laph_2 is not null) THEN
                    'laph_2 contains a non-numeric value : \'' || AS_VARCHAR(src:laph_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_3), '0'), 38, 10) is null and 
                    src:laph_3 is not null) THEN
                    'laph_3 contains a non-numeric value : \'' || AS_VARCHAR(src:laph_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_dtwc), '0'), 38, 10) is null and 
                    src:laph_dtwc is not null) THEN
                    'laph_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:laph_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_rfrc), '0'), 38, 10) is null and 
                    src:laph_rfrc is not null) THEN
                    'laph_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:laph_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lapi), '0'), 38, 10) is null and 
                    src:lapi is not null) THEN
                    'lapi contains a non-numeric value : \'' || AS_VARCHAR(src:lapi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leac_ref_compnr), '0'), 38, 10) is null and 
                    src:leac_ref_compnr is not null) THEN
                    'leac_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:leac_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:line), '0'), 38, 10) is null and 
                    src:line is not null) THEN
                    'line contains a non-numeric value : \'' || AS_VARCHAR(src:line) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:link), '0'), 38, 10) is null and 
                    src:link is not null) THEN
                    'link contains a non-numeric value : \'' || AS_VARCHAR(src:link) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) THEN
                    'lino contains a non-numeric value : \'' || AS_VARCHAR(src:lino) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:liqd), '1900-01-01')) is null and 
                    src:liqd is not null) THEN
                    'liqd contains a non-timestamp value : \'' || AS_VARCHAR(src:liqd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) THEN
                    'loco contains a non-numeric value : \'' || AS_VARCHAR(src:loco) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lpdt), '1900-01-01')) is null and 
                    src:lpdt is not null) THEN
                    'lpdt contains a non-timestamp value : \'' || AS_VARCHAR(src:lpdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvat), '0'), 38, 10) is null and 
                    src:lvat is not null) THEN
                    'lvat contains a non-numeric value : \'' || AS_VARCHAR(src:lvat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ninv), '0'), 38, 10) is null and 
                    src:ninv is not null) THEN
                    'ninv contains a non-numeric value : \'' || AS_VARCHAR(src:ninv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oinv), '0'), 38, 10) is null and 
                    src:oinv is not null) THEN
                    'oinv contains a non-numeric value : \'' || AS_VARCHAR(src:oinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:optb_ref_compnr), '0'), 38, 10) is null and 
                    src:optb_ref_compnr is not null) THEN
                    'optb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:optb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osch), '0'), 38, 10) is null and 
                    src:osch is not null) THEN
                    'osch contains a non-numeric value : \'' || AS_VARCHAR(src:osch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) THEN
                    'otbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp_ref_compnr), '0'), 38, 10) is null and 
                    src:otyp_ref_compnr is not null) THEN
                    'otyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:otyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pada), '0'), 38, 10) is null and 
                    src:pada is not null) THEN
                    'pada contains a non-numeric value : \'' || AS_VARCHAR(src:pada) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_1), '0'), 38, 10) is null and 
                    src:padh_1 is not null) THEN
                    'padh_1 contains a non-numeric value : \'' || AS_VARCHAR(src:padh_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_2), '0'), 38, 10) is null and 
                    src:padh_2 is not null) THEN
                    'padh_2 contains a non-numeric value : \'' || AS_VARCHAR(src:padh_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_3), '0'), 38, 10) is null and 
                    src:padh_3 is not null) THEN
                    'padh_3 contains a non-numeric value : \'' || AS_VARCHAR(src:padh_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_dtwc), '0'), 38, 10) is null and 
                    src:padh_dtwc is not null) THEN
                    'padh_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:padh_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_rfrc), '0'), 38, 10) is null and 
                    src:padh_rfrc is not null) THEN
                    'padh_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:padh_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padi), '0'), 38, 10) is null and 
                    src:padi is not null) THEN
                    'padi contains a non-numeric value : \'' || AS_VARCHAR(src:padi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:papr), '0'), 38, 10) is null and 
                    src:papr is not null) THEN
                    'papr contains a non-numeric value : \'' || AS_VARCHAR(src:papr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) THEN
                    'paya_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:paya_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payd), '0'), 38, 10) is null and 
                    src:payd is not null) THEN
                    'payd contains a non-numeric value : \'' || AS_VARCHAR(src:payd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payl), '0'), 38, 10) is null and 
                    src:payl is not null) THEN
                    'payl contains a non-numeric value : \'' || AS_VARCHAR(src:payl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym_ref_compnr), '0'), 38, 10) is null and 
                    src:paym_ref_compnr is not null) THEN
                    'paym_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:paym_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcom), '0'), 38, 10) is null and 
                    src:pcom is not null) THEN
                    'pcom contains a non-numeric value : \'' || AS_VARCHAR(src:pcom) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pdat), '1900-01-01')) is null and 
                    src:pdat is not null) THEN
                    'pdat contains a non-timestamp value : \'' || AS_VARCHAR(src:pdat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdif), '0'), 38, 10) is null and 
                    src:pdif is not null) THEN
                    'pdif contains a non-numeric value : \'' || AS_VARCHAR(src:pdif) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfre), '0'), 38, 10) is null and 
                    src:pfre is not null) THEN
                    'pfre contains a non-numeric value : \'' || AS_VARCHAR(src:pfre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) THEN
                    'post contains a non-numeric value : \'' || AS_VARCHAR(src:post) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) THEN
                    'prin contains a non-numeric value : \'' || AS_VARCHAR(src:prin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) THEN
                    'prod contains a non-numeric value : \'' || AS_VARCHAR(src:prod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is null and 
                    src:ptad_ref_compnr is not null) THEN
                    'ptad_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptad_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_bank_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_bank_ref_compnr is not null) THEN
                    'ptbp_bank_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_bank_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) THEN
                    'ptbp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptbp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptyp_ref_compnr is not null) THEN
                    'ptyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pysh), '0'), 38, 10) is null and 
                    src:pysh is not null) THEN
                    'pysh contains a non-numeric value : \'' || AS_VARCHAR(src:pysh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rade), '0'), 38, 10) is null and 
                    src:rade is not null) THEN
                    'rade contains a non-numeric value : \'' || AS_VARCHAR(src:rade) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) THEN
                    'ratd contains a non-timestamp value : \'' || AS_VARCHAR(src:ratd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) THEN
                    'rate_1 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) THEN
                    'rate_2 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) THEN
                    'rate_3 contains a non-numeric value : \'' || AS_VARCHAR(src:rate_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) THEN
                    'ratf_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) THEN
                    'ratf_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) THEN
                    'ratf_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ratf_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcpt), '0'), 38, 10) is null and 
                    src:rcpt is not null) THEN
                    'rcpt contains a non-numeric value : \'' || AS_VARCHAR(src:rcpt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) THEN
                    'reas_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:reas_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:regc_ref_compnr), '0'), 38, 10) is null and 
                    src:regc_ref_compnr is not null) THEN
                    'regc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:regc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schn), '0'), 38, 10) is null and 
                    src:schn is not null) THEN
                    'schn contains a non-numeric value : \'' || AS_VARCHAR(src:schn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgtp), '0'), 38, 10) is null and 
                    src:sgtp is not null) THEN
                    'sgtp contains a non-numeric value : \'' || AS_VARCHAR(src:sgtp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stap), '0'), 38, 10) is null and 
                    src:stap is not null) THEN
                    'stap contains a non-numeric value : \'' || AS_VARCHAR(src:stap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:step), '0'), 38, 10) is null and 
                    src:step is not null) THEN
                    'step contains a non-numeric value : \'' || AS_VARCHAR(src:step) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svah_1), '0'), 38, 10) is null and 
                    src:svah_1 is not null) THEN
                    'svah_1 contains a non-numeric value : \'' || AS_VARCHAR(src:svah_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svah_2), '0'), 38, 10) is null and 
                    src:svah_2 is not null) THEN
                    'svah_2 contains a non-numeric value : \'' || AS_VARCHAR(src:svah_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svah_3), '0'), 38, 10) is null and 
                    src:svah_3 is not null) THEN
                    'svah_3 contains a non-numeric value : \'' || AS_VARCHAR(src:svah_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svam), '0'), 38, 10) is null and 
                    src:svam is not null) THEN
                    'svam contains a non-numeric value : \'' || AS_VARCHAR(src:svam) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbrl), '0'), 38, 10) is null and 
                    src:tbrl is not null) THEN
                    'tbrl contains a non-numeric value : \'' || AS_VARCHAR(src:tbrl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdoc_ref_compnr), '0'), 38, 10) is null and 
                    src:tdoc_ref_compnr is not null) THEN
                    'tdoc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:tdoc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) THEN
                    'text contains a non-numeric value : \'' || AS_VARCHAR(src:text) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) THEN
                    'text_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:text_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_1), '0'), 38, 10) is null and 
                    src:tore_1 is not null) THEN
                    'tore_1 contains a non-numeric value : \'' || AS_VARCHAR(src:tore_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_2), '0'), 38, 10) is null and 
                    src:tore_2 is not null) THEN
                    'tore_2 contains a non-numeric value : \'' || AS_VARCHAR(src:tore_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_3), '0'), 38, 10) is null and 
                    src:tore_3 is not null) THEN
                    'tore_3 contains a non-numeric value : \'' || AS_VARCHAR(src:tore_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_dtwc), '0'), 38, 10) is null and 
                    src:tore_dtwc is not null) THEN
                    'tore_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:tore_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_invc), '0'), 38, 10) is null and 
                    src:tore_invc is not null) THEN
                    'tore_invc contains a non-numeric value : \'' || AS_VARCHAR(src:tore_invc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_rfrc), '0'), 38, 10) is null and 
                    src:tore_rfrc is not null) THEN
                    'tore_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:tore_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpay), '0'), 38, 10) is null and 
                    src:tpay is not null) THEN
                    'tpay contains a non-numeric value : \'' || AS_VARCHAR(src:tpay) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ttyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ttyp_ref_compnr is not null) THEN
                    'ttyp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ttyp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:txdt), '1900-01-01')) is null and 
                    src:txdt is not null) THEN
                    'txdt contains a non-timestamp value : \'' || AS_VARCHAR(src:txdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) THEN
                    'txtb contains a non-numeric value : \'' || AS_VARCHAR(src:txtb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) THEN
                    'txtb_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:txtb_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typa_ref_compnr), '0'), 38, 10) is null and 
                    src:typa_ref_compnr is not null) THEN
                    'typa_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:typa_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vata), '0'), 38, 10) is null and 
                    src:vata is not null) THEN
                    'vata contains a non-numeric value : \'' || AS_VARCHAR(src:vata) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_cvat_ref_compnr is not null) THEN
                    'vatc_cvat_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:vatc_cvat_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_ref_compnr is not null) THEN
                    'vatc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:vatc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_1), '0'), 38, 10) is null and 
                    src:vath_1 is not null) THEN
                    'vath_1 contains a non-numeric value : \'' || AS_VARCHAR(src:vath_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_2), '0'), 38, 10) is null and 
                    src:vath_2 is not null) THEN
                    'vath_2 contains a non-numeric value : \'' || AS_VARCHAR(src:vath_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_3), '0'), 38, 10) is null and 
                    src:vath_3 is not null) THEN
                    'vath_3 contains a non-numeric value : \'' || AS_VARCHAR(src:vath_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_dtwc), '0'), 38, 10) is null and 
                    src:vath_dtwc is not null) THEN
                    'vath_dtwc contains a non-numeric value : \'' || AS_VARCHAR(src:vath_dtwc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_rfrc), '0'), 38, 10) is null and 
                    src:vath_rfrc is not null) THEN
                    'vath_rfrc contains a non-numeric value : \'' || AS_VARCHAR(src:vath_rfrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vati), '0'), 38, 10) is null and 
                    src:vati is not null) THEN
                    'vati contains a non-numeric value : \'' || AS_VARCHAR(src:vati) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatp), '0'), 38, 10) is null and 
                    src:vatp is not null) THEN
                    'vatp contains a non-numeric value : \'' || AS_VARCHAR(src:vatp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vaty), '0'), 38, 10) is null and 
                    src:vaty is not null) THEN
                    'vaty contains a non-numeric value : \'' || AS_VARCHAR(src:vaty) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whti), '0'), 38, 10) is null and 
                    src:whti is not null) THEN
                    'whti contains a non-numeric value : \'' || AS_VARCHAR(src:whti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtph_1), '0'), 38, 10) is null and 
                    src:wtph_1 is not null) THEN
                    'wtph_1 contains a non-numeric value : \'' || AS_VARCHAR(src:wtph_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtph_2), '0'), 38, 10) is null and 
                    src:wtph_2 is not null) THEN
                    'wtph_2 contains a non-numeric value : \'' || AS_VARCHAR(src:wtph_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtph_3), '0'), 38, 10) is null and 
                    src:wtph_3 is not null) THEN
                    'wtph_3 contains a non-numeric value : \'' || AS_VARCHAR(src:wtph_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtpi), '0'), 38, 10) is null and 
                    src:wtpi is not null) THEN
                    'wtpi contains a non-numeric value : \'' || AS_VARCHAR(src:wtpi) || '\'' WHEN 
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
                                    
                src:ttyp,
                src:lino,
                src:docn,
                src:line,
                src:ninv,
                src:compnr,
                src:tdoc  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adty), '0'), 38, 10) is null and 
                    src:adty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:afpy), '0'), 38, 10) is null and 
                    src:afpy is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amnt), '0'), 38, 10) is null and 
                    src:amnt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_1), '0'), 38, 10) is null and 
                    src:amth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_2), '0'), 38, 10) is null and 
                    src:amth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_3), '0'), 38, 10) is null and 
                    src:amth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_dtwc), '0'), 38, 10) is null and 
                    src:amth_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amth_rfrc), '0'), 38, 10) is null and 
                    src:amth_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amti), '0'), 38, 10) is null and 
                    src:amti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:appr), '0'), 38, 10) is null and 
                    src:appr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:autm), '0'), 38, 10) is null and 
                    src:autm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_1), '0'), 38, 10) is null and 
                    src:baca_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_2), '0'), 38, 10) is null and 
                    src:baca_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_3), '0'), 38, 10) is null and 
                    src:baca_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_dtwc), '0'), 38, 10) is null and 
                    src:baca_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_invc), '0'), 38, 10) is null and 
                    src:baca_invc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baca_rfrc), '0'), 38, 10) is null and 
                    src:baca_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:baco), '0'), 38, 10) is null and 
                    src:baco is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_1), '0'), 38, 10) is null and 
                    src:bahc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_2), '0'), 38, 10) is null and 
                    src:bahc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_3), '0'), 38, 10) is null and 
                    src:bahc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_dtwc), '0'), 38, 10) is null and 
                    src:bahc_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bahc_rfrc), '0'), 38, 10) is null and 
                    src:bahc_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bala), '0'), 38, 10) is null and 
                    src:bala is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balc), '0'), 38, 10) is null and 
                    src:balc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_1), '0'), 38, 10) is null and 
                    src:balh_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_2), '0'), 38, 10) is null and 
                    src:balh_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_3), '0'), 38, 10) is null and 
                    src:balh_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_dtwc), '0'), 38, 10) is null and 
                    src:balh_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:balh_rfrc), '0'), 38, 10) is null and 
                    src:balh_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:basi), '0'), 38, 10) is null and 
                    src:basi is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:bdat), '1900-01-01')) is null and 
                    src:bdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bdsp), '0'), 38, 10) is null and 
                    src:bdsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:blac), '0'), 38, 10) is null and 
                    src:blac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc_ref_compnr), '0'), 38, 10) is null and 
                    src:bloc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bref_ref_compnr), '0'), 38, 10) is null and 
                    src:bref_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:btno), '0'), 38, 10) is null and 
                    src:btno is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bust), '0'), 38, 10) is null and 
                    src:bust is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccrs_ref_compnr), '0'), 38, 10) is null and 
                    src:ccrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is null and 
                    src:ccur_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_1), '0'), 38, 10) is null and 
                    src:cdam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_2), '0'), 38, 10) is null and 
                    src:cdam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_3), '0'), 38, 10) is null and 
                    src:cdam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_dtwc), '0'), 38, 10) is null and 
                    src:cdam_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_invc), '0'), 38, 10) is null and 
                    src:cdam_invc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdam_rfrc), '0'), 38, 10) is null and 
                    src:cdam_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfrs_ref_compnr), '0'), 38, 10) is null and 
                    src:cfrs_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpay_ref_compnr), '0'), 38, 10) is null and 
                    src:cpay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dapr), '0'), 38, 10) is null and 
                    src:dapr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1a), '0'), 38, 10) is null and 
                    src:dc1a is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_1), '0'), 38, 10) is null and 
                    src:dc1h_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_2), '0'), 38, 10) is null and 
                    src:dc1h_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_3), '0'), 38, 10) is null and 
                    src:dc1h_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_dtwc), '0'), 38, 10) is null and 
                    src:dc1h_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1h_rfrc), '0'), 38, 10) is null and 
                    src:dc1h_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc1i), '0'), 38, 10) is null and 
                    src:dc1i is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2a), '0'), 38, 10) is null and 
                    src:dc2a is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_1), '0'), 38, 10) is null and 
                    src:dc2h_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_2), '0'), 38, 10) is null and 
                    src:dc2h_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_3), '0'), 38, 10) is null and 
                    src:dc2h_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_dtwc), '0'), 38, 10) is null and 
                    src:dc2h_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2h_rfrc), '0'), 38, 10) is null and 
                    src:dc2h_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc2i), '0'), 38, 10) is null and 
                    src:dc2i is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3a), '0'), 38, 10) is null and 
                    src:dc3a is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_1), '0'), 38, 10) is null and 
                    src:dc3h_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_2), '0'), 38, 10) is null and 
                    src:dc3h_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_3), '0'), 38, 10) is null and 
                    src:dc3h_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_dtwc), '0'), 38, 10) is null and 
                    src:dc3h_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3h_rfrc), '0'), 38, 10) is null and 
                    src:dc3h_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dc3i), '0'), 38, 10) is null and 
                    src:dc3i is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:did1), '1900-01-01')) is null and 
                    src:did1 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:did2), '1900-01-01')) is null and 
                    src:did2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:did3), '1900-01-01')) is null and 
                    src:did3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:doca), '0'), 38, 10) is null and 
                    src:doca is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:docd), '1900-01-01')) is null and 
                    src:docd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:docn), '0'), 38, 10) is null and 
                    src:docn is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:dued), '1900-01-01')) is null and 
                    src:dued is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:edtp), '0'), 38, 10) is null and 
                    src:edtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:enia), '0'), 38, 10) is null and 
                    src:enia is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iadr_ref_compnr), '0'), 38, 10) is null and 
                    src:iadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ifbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ifbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ildp), '0'), 38, 10) is null and 
                    src:ildp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:irdt), '1900-01-01')) is null and 
                    src:irdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lapa), '0'), 38, 10) is null and 
                    src:lapa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_1), '0'), 38, 10) is null and 
                    src:laph_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_2), '0'), 38, 10) is null and 
                    src:laph_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_3), '0'), 38, 10) is null and 
                    src:laph_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_dtwc), '0'), 38, 10) is null and 
                    src:laph_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laph_rfrc), '0'), 38, 10) is null and 
                    src:laph_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lapi), '0'), 38, 10) is null and 
                    src:lapi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:leac_ref_compnr), '0'), 38, 10) is null and 
                    src:leac_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:line), '0'), 38, 10) is null and 
                    src:line is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:link), '0'), 38, 10) is null and 
                    src:link is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lino), '0'), 38, 10) is null and 
                    src:lino is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:liqd), '1900-01-01')) is null and 
                    src:liqd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:loco), '0'), 38, 10) is null and 
                    src:loco is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:lpdt), '1900-01-01')) is null and 
                    src:lpdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvat), '0'), 38, 10) is null and 
                    src:lvat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ninv), '0'), 38, 10) is null and 
                    src:ninv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oinv), '0'), 38, 10) is null and 
                    src:oinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:optb_ref_compnr), '0'), 38, 10) is null and 
                    src:optb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:osch), '0'), 38, 10) is null and 
                    src:osch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otbp_ref_compnr), '0'), 38, 10) is null and 
                    src:otbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:otyp_ref_compnr), '0'), 38, 10) is null and 
                    src:otyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pada), '0'), 38, 10) is null and 
                    src:pada is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_1), '0'), 38, 10) is null and 
                    src:padh_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_2), '0'), 38, 10) is null and 
                    src:padh_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_3), '0'), 38, 10) is null and 
                    src:padh_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_dtwc), '0'), 38, 10) is null and 
                    src:padh_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padh_rfrc), '0'), 38, 10) is null and 
                    src:padh_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:padi), '0'), 38, 10) is null and 
                    src:padi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:papr), '0'), 38, 10) is null and 
                    src:papr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paya_ref_compnr), '0'), 38, 10) is null and 
                    src:paya_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payd), '0'), 38, 10) is null and 
                    src:payd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:payl), '0'), 38, 10) is null and 
                    src:payl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:paym_ref_compnr), '0'), 38, 10) is null and 
                    src:paym_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcom), '0'), 38, 10) is null and 
                    src:pcom is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:pdat), '1900-01-01')) is null and 
                    src:pdat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdif), '0'), 38, 10) is null and 
                    src:pdif is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pfre), '0'), 38, 10) is null and 
                    src:pfre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:post), '0'), 38, 10) is null and 
                    src:post is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prin), '0'), 38, 10) is null and 
                    src:prin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prod), '0'), 38, 10) is null and 
                    src:prod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptad_ref_compnr), '0'), 38, 10) is null and 
                    src:ptad_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_bank_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_bank_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptbp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptbp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ptyp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pysh), '0'), 38, 10) is null and 
                    src:pysh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rade), '0'), 38, 10) is null and 
                    src:rade is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ratd), '1900-01-01')) is null and 
                    src:ratd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_1), '0'), 38, 10) is null and 
                    src:rate_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_2), '0'), 38, 10) is null and 
                    src:rate_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rate_3), '0'), 38, 10) is null and 
                    src:rate_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_1), '0'), 38, 10) is null and 
                    src:ratf_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_2), '0'), 38, 10) is null and 
                    src:ratf_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ratf_3), '0'), 38, 10) is null and 
                    src:ratf_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rcpt), '0'), 38, 10) is null and 
                    src:rcpt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reas_ref_compnr), '0'), 38, 10) is null and 
                    src:reas_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:regc_ref_compnr), '0'), 38, 10) is null and 
                    src:regc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schn), '0'), 38, 10) is null and 
                    src:schn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sgtp), '0'), 38, 10) is null and 
                    src:sgtp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stap), '0'), 38, 10) is null and 
                    src:stap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:step), '0'), 38, 10) is null and 
                    src:step is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svah_1), '0'), 38, 10) is null and 
                    src:svah_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svah_2), '0'), 38, 10) is null and 
                    src:svah_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svah_3), '0'), 38, 10) is null and 
                    src:svah_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:svam), '0'), 38, 10) is null and 
                    src:svam is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tbrl), '0'), 38, 10) is null and 
                    src:tbrl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tdoc_ref_compnr), '0'), 38, 10) is null and 
                    src:tdoc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text), '0'), 38, 10) is null and 
                    src:text is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is null and 
                    src:text_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_1), '0'), 38, 10) is null and 
                    src:tore_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_2), '0'), 38, 10) is null and 
                    src:tore_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_3), '0'), 38, 10) is null and 
                    src:tore_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_dtwc), '0'), 38, 10) is null and 
                    src:tore_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_invc), '0'), 38, 10) is null and 
                    src:tore_invc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tore_rfrc), '0'), 38, 10) is null and 
                    src:tore_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tpay), '0'), 38, 10) is null and 
                    src:tpay is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ttyp_ref_compnr), '0'), 38, 10) is null and 
                    src:ttyp_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:txdt), '1900-01-01')) is null and 
                    src:txdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb), '0'), 38, 10) is null and 
                    src:txtb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:txtb_ref_compnr), '0'), 38, 10) is null and 
                    src:txtb_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:typa_ref_compnr), '0'), 38, 10) is null and 
                    src:typa_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vata), '0'), 38, 10) is null and 
                    src:vata is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_cvat_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_cvat_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatc_ref_compnr), '0'), 38, 10) is null and 
                    src:vatc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_1), '0'), 38, 10) is null and 
                    src:vath_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_2), '0'), 38, 10) is null and 
                    src:vath_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_3), '0'), 38, 10) is null and 
                    src:vath_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_dtwc), '0'), 38, 10) is null and 
                    src:vath_dtwc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vath_rfrc), '0'), 38, 10) is null and 
                    src:vath_rfrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vati), '0'), 38, 10) is null and 
                    src:vati is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vatp), '0'), 38, 10) is null and 
                    src:vatp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vaty), '0'), 38, 10) is null and 
                    src:vaty is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:whti), '0'), 38, 10) is null and 
                    src:whti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtph_1), '0'), 38, 10) is null and 
                    src:wtph_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtph_2), '0'), 38, 10) is null and 
                    src:wtph_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtph_3), '0'), 38, 10) is null and 
                    src:wtph_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wtpi), '0'), 38, 10) is null and 
                    src:wtpi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:year), '0'), 38, 10) is null and 
                    src:year is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)