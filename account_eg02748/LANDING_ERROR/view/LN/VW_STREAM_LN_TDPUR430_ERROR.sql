CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR430_ERROR AS SELECT src, 'LN_TDPUR430' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) THEN
                    'amld contains a non-numeric value : \'' || AS_VARCHAR(src:amld) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) THEN
                    'amod contains a non-numeric value : \'' || AS_VARCHAR(src:amod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnig), '0'), 38, 10) is null and 
                    src:cnig is not null) THEN
                    'cnig contains a non-numeric value : \'' || AS_VARCHAR(src:cnig) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpon), '0'), 38, 10) is null and 
                    src:cpon is not null) THEN
                    'cpon contains a non-numeric value : \'' || AS_VARCHAR(src:cpon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) THEN
                    'csqn contains a non-numeric value : \'' || AS_VARCHAR(src:csqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) THEN
                    'cupp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cupp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) THEN
                    'cuva contains a non-numeric value : \'' || AS_VARCHAR(src:cuva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) THEN
                    'cuvc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cuvc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) THEN
                    'cvpp contains a non-numeric value : \'' || AS_VARCHAR(src:cvpp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_1), '0'), 38, 10) is null and 
                    src:disc_1 is not null) THEN
                    'disc_1 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_10), '0'), 38, 10) is null and 
                    src:disc_10 is not null) THEN
                    'disc_10 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_11), '0'), 38, 10) is null and 
                    src:disc_11 is not null) THEN
                    'disc_11 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_2), '0'), 38, 10) is null and 
                    src:disc_2 is not null) THEN
                    'disc_2 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_3), '0'), 38, 10) is null and 
                    src:disc_3 is not null) THEN
                    'disc_3 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_4), '0'), 38, 10) is null and 
                    src:disc_4 is not null) THEN
                    'disc_4 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_5), '0'), 38, 10) is null and 
                    src:disc_5 is not null) THEN
                    'disc_5 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_6), '0'), 38, 10) is null and 
                    src:disc_6 is not null) THEN
                    'disc_6 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_7), '0'), 38, 10) is null and 
                    src:disc_7 is not null) THEN
                    'disc_7 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_8), '0'), 38, 10) is null and 
                    src:disc_8 is not null) THEN
                    'disc_8 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_9), '0'), 38, 10) is null and 
                    src:disc_9 is not null) THEN
                    'disc_9 contains a non-numeric value : \'' || AS_VARCHAR(src:disc_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_1), '0'), 38, 10) is null and 
                    src:dmse_1 is not null) THEN
                    'dmse_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_10), '0'), 38, 10) is null and 
                    src:dmse_10 is not null) THEN
                    'dmse_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_11), '0'), 38, 10) is null and 
                    src:dmse_11 is not null) THEN
                    'dmse_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_2), '0'), 38, 10) is null and 
                    src:dmse_2 is not null) THEN
                    'dmse_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_3), '0'), 38, 10) is null and 
                    src:dmse_3 is not null) THEN
                    'dmse_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_4), '0'), 38, 10) is null and 
                    src:dmse_4 is not null) THEN
                    'dmse_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_5), '0'), 38, 10) is null and 
                    src:dmse_5 is not null) THEN
                    'dmse_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_6), '0'), 38, 10) is null and 
                    src:dmse_6 is not null) THEN
                    'dmse_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_7), '0'), 38, 10) is null and 
                    src:dmse_7 is not null) THEN
                    'dmse_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_8), '0'), 38, 10) is null and 
                    src:dmse_8 is not null) THEN
                    'dmse_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_9), '0'), 38, 10) is null and 
                    src:dmse_9 is not null) THEN
                    'dmse_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmse_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is null and 
                    src:dmth_1 is not null) THEN
                    'dmth_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is null and 
                    src:dmth_10 is not null) THEN
                    'dmth_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is null and 
                    src:dmth_11 is not null) THEN
                    'dmth_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is null and 
                    src:dmth_2 is not null) THEN
                    'dmth_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is null and 
                    src:dmth_3 is not null) THEN
                    'dmth_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is null and 
                    src:dmth_4 is not null) THEN
                    'dmth_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is null and 
                    src:dmth_5 is not null) THEN
                    'dmth_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is null and 
                    src:dmth_6 is not null) THEN
                    'dmth_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is null and 
                    src:dmth_7 is not null) THEN
                    'dmth_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is null and 
                    src:dmth_8 is not null) THEN
                    'dmth_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is null and 
                    src:dmth_9 is not null) THEN
                    'dmth_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmth_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_1), '0'), 38, 10) is null and 
                    src:dmty_1 is not null) THEN
                    'dmty_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_10), '0'), 38, 10) is null and 
                    src:dmty_10 is not null) THEN
                    'dmty_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_11), '0'), 38, 10) is null and 
                    src:dmty_11 is not null) THEN
                    'dmty_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_2), '0'), 38, 10) is null and 
                    src:dmty_2 is not null) THEN
                    'dmty_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_3), '0'), 38, 10) is null and 
                    src:dmty_3 is not null) THEN
                    'dmty_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_4), '0'), 38, 10) is null and 
                    src:dmty_4 is not null) THEN
                    'dmty_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_5), '0'), 38, 10) is null and 
                    src:dmty_5 is not null) THEN
                    'dmty_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_6), '0'), 38, 10) is null and 
                    src:dmty_6 is not null) THEN
                    'dmty_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_7), '0'), 38, 10) is null and 
                    src:dmty_7 is not null) THEN
                    'dmty_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_8), '0'), 38, 10) is null and 
                    src:dmty_8 is not null) THEN
                    'dmty_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_9), '0'), 38, 10) is null and 
                    src:dmty_9 is not null) THEN
                    'dmty_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dmty_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_1), '0'), 38, 10) is null and 
                    src:dorg_1 is not null) THEN
                    'dorg_1 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_10), '0'), 38, 10) is null and 
                    src:dorg_10 is not null) THEN
                    'dorg_10 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_11), '0'), 38, 10) is null and 
                    src:dorg_11 is not null) THEN
                    'dorg_11 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_2), '0'), 38, 10) is null and 
                    src:dorg_2 is not null) THEN
                    'dorg_2 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_3), '0'), 38, 10) is null and 
                    src:dorg_3 is not null) THEN
                    'dorg_3 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_4), '0'), 38, 10) is null and 
                    src:dorg_4 is not null) THEN
                    'dorg_4 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_5), '0'), 38, 10) is null and 
                    src:dorg_5 is not null) THEN
                    'dorg_5 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_6), '0'), 38, 10) is null and 
                    src:dorg_6 is not null) THEN
                    'dorg_6 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_7), '0'), 38, 10) is null and 
                    src:dorg_7 is not null) THEN
                    'dorg_7 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_8), '0'), 38, 10) is null and 
                    src:dorg_8 is not null) THEN
                    'dorg_8 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_9), '0'), 38, 10) is null and 
                    src:dorg_9 is not null) THEN
                    'dorg_9 contains a non-numeric value : \'' || AS_VARCHAR(src:dorg_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) THEN
                    'dtrm contains a non-numeric value : \'' || AS_VARCHAR(src:dtrm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) THEN
                    'elgb contains a non-numeric value : \'' || AS_VARCHAR(src:elgb) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ftdt), '1900-01-01')) is null and 
                    src:ftdt is not null) THEN
                    'ftdt contains a non-timestamp value : \'' || AS_VARCHAR(src:ftdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) THEN
                    'iamt contains a non-numeric value : \'' || AS_VARCHAR(src:iamt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) THEN
                    'invd contains a non-timestamp value : \'' || AS_VARCHAR(src:invd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is null and 
                    src:ldam_1 is not null) THEN
                    'ldam_1 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is null and 
                    src:ldam_10 is not null) THEN
                    'ldam_10 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_10) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is null and 
                    src:ldam_11 is not null) THEN
                    'ldam_11 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_11) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is null and 
                    src:ldam_2 is not null) THEN
                    'ldam_2 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is null and 
                    src:ldam_3 is not null) THEN
                    'ldam_3 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is null and 
                    src:ldam_4 is not null) THEN
                    'ldam_4 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is null and 
                    src:ldam_5 is not null) THEN
                    'ldam_5 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is null and 
                    src:ldam_6 is not null) THEN
                    'ldam_6 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is null and 
                    src:ldam_7 is not null) THEN
                    'ldam_7 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is null and 
                    src:ldam_8 is not null) THEN
                    'ldam_8 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_8) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is null and 
                    src:ldam_9 is not null) THEN
                    'ldam_9 contains a non-numeric value : \'' || AS_VARCHAR(src:ldam_9) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opsq), '0'), 38, 10) is null and 
                    src:opsq is not null) THEN
                    'opsq contains a non-numeric value : \'' || AS_VARCHAR(src:opsq) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pamt), '0'), 38, 10) is null and 
                    src:pamt is not null) THEN
                    'pamt contains a non-numeric value : \'' || AS_VARCHAR(src:pamt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) THEN
                    'pmse contains a non-numeric value : \'' || AS_VARCHAR(src:pmse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) THEN
                    'pono contains a non-numeric value : \'' || AS_VARCHAR(src:pono) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) THEN
                    'porg contains a non-numeric value : \'' || AS_VARCHAR(src:porg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) THEN
                    'pric contains a non-numeric value : \'' || AS_VARCHAR(src:pric) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc), '0'), 38, 10) is null and 
                    src:proc is not null) THEN
                    'proc contains a non-numeric value : \'' || AS_VARCHAR(src:proc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) THEN
                    'prsg_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:prsg_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqn), '0'), 38, 10) is null and 
                    src:psqn is not null) THEN
                    'psqn contains a non-numeric value : \'' || AS_VARCHAR(src:psqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) THEN
                    'ptyp contains a non-numeric value : \'' || AS_VARCHAR(src:ptyp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv), '0'), 38, 10) is null and 
                    src:qiiv is not null) THEN
                    'qiiv contains a non-numeric value : \'' || AS_VARCHAR(src:qiiv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qipr), '0'), 38, 10) is null and 
                    src:qipr is not null) THEN
                    'qipr contains a non-numeric value : \'' || AS_VARCHAR(src:qipr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpiv), '0'), 38, 10) is null and 
                    src:qpiv is not null) THEN
                    'qpiv contains a non-numeric value : \'' || AS_VARCHAR(src:qpiv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppr), '0'), 38, 10) is null and 
                    src:qppr is not null) THEN
                    'qppr contains a non-numeric value : \'' || AS_VARCHAR(src:qppr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract), '0'), 38, 10) is null and 
                    src:ract is not null) THEN
                    'ract contains a non-numeric value : \'' || AS_VARCHAR(src:ract) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rarc), '0'), 38, 10) is null and 
                    src:rarc is not null) THEN
                    'rarc contains a non-numeric value : \'' || AS_VARCHAR(src:rarc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsqn), '0'), 38, 10) is null and 
                    src:rsqn is not null) THEN
                    'rsqn contains a non-numeric value : \'' || AS_VARCHAR(src:rsqn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) THEN
                    'sqnb contains a non-numeric value : \'' || AS_VARCHAR(src:sqnb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsc), '0'), 38, 10) is null and 
                    src:stsc is not null) THEN
                    'stsc contains a non-numeric value : \'' || AS_VARCHAR(src:stsc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsd), '0'), 38, 10) is null and 
                    src:stsd is not null) THEN
                    'stsd contains a non-numeric value : \'' || AS_VARCHAR(src:stsd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) THEN
                    'styp contains a non-numeric value : \'' || AS_VARCHAR(src:styp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:uldt), '1900-01-01')) is null and 
                    src:uldt is not null) THEN
                    'uldt contains a non-timestamp value : \'' || AS_VARCHAR(src:uldt) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:usdt), '1900-01-01')) is null and 
                    src:usdt is not null) THEN
                    'usdt contains a non-timestamp value : \'' || AS_VARCHAR(src:usdt) || '\'' WHEN 
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
                                    
                src:psqn,
                src:compnr,
                src:orno,
                src:rsqn,
                src:pono,
                src:sqnb  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR430 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amld), '0'), 38, 10) is null and 
                    src:amld is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:amod), '0'), 38, 10) is null and 
                    src:amod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnig), '0'), 38, 10) is null and 
                    src:cnig is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpon), '0'), 38, 10) is null and 
                    src:cpon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csqn), '0'), 38, 10) is null and 
                    src:csqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cupp_ref_compnr), '0'), 38, 10) is null and 
                    src:cupp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuva), '0'), 38, 10) is null and 
                    src:cuva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cuvc_ref_compnr), '0'), 38, 10) is null and 
                    src:cuvc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cvpp), '0'), 38, 10) is null and 
                    src:cvpp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_1), '0'), 38, 10) is null and 
                    src:disc_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_10), '0'), 38, 10) is null and 
                    src:disc_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_11), '0'), 38, 10) is null and 
                    src:disc_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_2), '0'), 38, 10) is null and 
                    src:disc_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_3), '0'), 38, 10) is null and 
                    src:disc_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_4), '0'), 38, 10) is null and 
                    src:disc_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_5), '0'), 38, 10) is null and 
                    src:disc_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_6), '0'), 38, 10) is null and 
                    src:disc_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_7), '0'), 38, 10) is null and 
                    src:disc_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_8), '0'), 38, 10) is null and 
                    src:disc_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:disc_9), '0'), 38, 10) is null and 
                    src:disc_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_1), '0'), 38, 10) is null and 
                    src:dmse_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_10), '0'), 38, 10) is null and 
                    src:dmse_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_11), '0'), 38, 10) is null and 
                    src:dmse_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_2), '0'), 38, 10) is null and 
                    src:dmse_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_3), '0'), 38, 10) is null and 
                    src:dmse_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_4), '0'), 38, 10) is null and 
                    src:dmse_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_5), '0'), 38, 10) is null and 
                    src:dmse_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_6), '0'), 38, 10) is null and 
                    src:dmse_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_7), '0'), 38, 10) is null and 
                    src:dmse_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_8), '0'), 38, 10) is null and 
                    src:dmse_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmse_9), '0'), 38, 10) is null and 
                    src:dmse_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_1), '0'), 38, 10) is null and 
                    src:dmth_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_10), '0'), 38, 10) is null and 
                    src:dmth_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_11), '0'), 38, 10) is null and 
                    src:dmth_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_2), '0'), 38, 10) is null and 
                    src:dmth_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_3), '0'), 38, 10) is null and 
                    src:dmth_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_4), '0'), 38, 10) is null and 
                    src:dmth_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_5), '0'), 38, 10) is null and 
                    src:dmth_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_6), '0'), 38, 10) is null and 
                    src:dmth_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_7), '0'), 38, 10) is null and 
                    src:dmth_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_8), '0'), 38, 10) is null and 
                    src:dmth_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmth_9), '0'), 38, 10) is null and 
                    src:dmth_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_1), '0'), 38, 10) is null and 
                    src:dmty_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_10), '0'), 38, 10) is null and 
                    src:dmty_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_11), '0'), 38, 10) is null and 
                    src:dmty_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_2), '0'), 38, 10) is null and 
                    src:dmty_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_3), '0'), 38, 10) is null and 
                    src:dmty_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_4), '0'), 38, 10) is null and 
                    src:dmty_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_5), '0'), 38, 10) is null and 
                    src:dmty_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_6), '0'), 38, 10) is null and 
                    src:dmty_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_7), '0'), 38, 10) is null and 
                    src:dmty_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_8), '0'), 38, 10) is null and 
                    src:dmty_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmty_9), '0'), 38, 10) is null and 
                    src:dmty_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_1), '0'), 38, 10) is null and 
                    src:dorg_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_10), '0'), 38, 10) is null and 
                    src:dorg_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_11), '0'), 38, 10) is null and 
                    src:dorg_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_2), '0'), 38, 10) is null and 
                    src:dorg_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_3), '0'), 38, 10) is null and 
                    src:dorg_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_4), '0'), 38, 10) is null and 
                    src:dorg_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_5), '0'), 38, 10) is null and 
                    src:dorg_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_6), '0'), 38, 10) is null and 
                    src:dorg_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_7), '0'), 38, 10) is null and 
                    src:dorg_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_8), '0'), 38, 10) is null and 
                    src:dorg_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dorg_9), '0'), 38, 10) is null and 
                    src:dorg_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dtrm), '0'), 38, 10) is null and 
                    src:dtrm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:elgb), '0'), 38, 10) is null and 
                    src:elgb is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ftdt), '1900-01-01')) is null and 
                    src:ftdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iamt), '0'), 38, 10) is null and 
                    src:iamt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:invd), '1900-01-01')) is null and 
                    src:invd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_1), '0'), 38, 10) is null and 
                    src:ldam_1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_10), '0'), 38, 10) is null and 
                    src:ldam_10 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_11), '0'), 38, 10) is null and 
                    src:ldam_11 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_2), '0'), 38, 10) is null and 
                    src:ldam_2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_3), '0'), 38, 10) is null and 
                    src:ldam_3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_4), '0'), 38, 10) is null and 
                    src:ldam_4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_5), '0'), 38, 10) is null and 
                    src:ldam_5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_6), '0'), 38, 10) is null and 
                    src:ldam_6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_7), '0'), 38, 10) is null and 
                    src:ldam_7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_8), '0'), 38, 10) is null and 
                    src:ldam_8 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldam_9), '0'), 38, 10) is null and 
                    src:ldam_9 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opsq), '0'), 38, 10) is null and 
                    src:opsq is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pamt), '0'), 38, 10) is null and 
                    src:pamt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pmse), '0'), 38, 10) is null and 
                    src:pmse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pono), '0'), 38, 10) is null and 
                    src:pono is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:porg), '0'), 38, 10) is null and 
                    src:porg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pric), '0'), 38, 10) is null and 
                    src:pric is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:proc), '0'), 38, 10) is null and 
                    src:proc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prsg_ref_compnr), '0'), 38, 10) is null and 
                    src:prsg_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psqn), '0'), 38, 10) is null and 
                    src:psqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptyp), '0'), 38, 10) is null and 
                    src:ptyp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qiiv), '0'), 38, 10) is null and 
                    src:qiiv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qipr), '0'), 38, 10) is null and 
                    src:qipr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qpiv), '0'), 38, 10) is null and 
                    src:qpiv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qppr), '0'), 38, 10) is null and 
                    src:qppr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract), '0'), 38, 10) is null and 
                    src:ract is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rarc), '0'), 38, 10) is null and 
                    src:rarc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsqn), '0'), 38, 10) is null and 
                    src:rsqn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sqnb), '0'), 38, 10) is null and 
                    src:sqnb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsc), '0'), 38, 10) is null and 
                    src:stsc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stsd), '0'), 38, 10) is null and 
                    src:stsd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:styp), '0'), 38, 10) is null and 
                    src:styp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:uldt), '1900-01-01')) is null and 
                    src:uldt is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:usdt), '1900-01-01')) is null and 
                    src:usdt is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)