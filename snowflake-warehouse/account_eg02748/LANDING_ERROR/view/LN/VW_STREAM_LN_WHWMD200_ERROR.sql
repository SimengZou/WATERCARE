CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_WHWMD200_ERROR AS SELECT src, 'LN_WHWMD200' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aipd), '0'), 38, 10) is null and 
                    src:aipd is not null) THEN
                    'aipd contains a non-numeric value : \'' || AS_VARCHAR(src:aipd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aoit), '0'), 38, 10) is null and 
                    src:aoit is not null) THEN
                    'aoit contains a non-numeric value : \'' || AS_VARCHAR(src:aoit) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcyc), '0'), 38, 10) is null and 
                    src:bcyc is not null) THEN
                    'bcyc contains a non-numeric value : \'' || AS_VARCHAR(src:bcyc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:binb), '0'), 38, 10) is null and 
                    src:binb is not null) THEN
                    'binb contains a non-numeric value : \'' || AS_VARCHAR(src:binb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc_ref_compnr), '0'), 38, 10) is null and 
                    src:bloc_ref_compnr is not null) THEN
                    'bloc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:bloc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bout), '0'), 38, 10) is null and 
                    src:bout is not null) THEN
                    'bout contains a non-numeric value : \'' || AS_VARCHAR(src:bout) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) THEN
                    'ccal_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccal_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdap), '0'), 38, 10) is null and 
                    src:cdap is not null) THEN
                    'cdap contains a non-numeric value : \'' || AS_VARCHAR(src:cdap) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdcr), '0'), 38, 10) is null and 
                    src:cdcr is not null) THEN
                    'cdcr contains a non-numeric value : \'' || AS_VARCHAR(src:cdcr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdlt), '0'), 38, 10) is null and 
                    src:cdlt is not null) THEN
                    'cdlt contains a non-numeric value : \'' || AS_VARCHAR(src:cdlt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdlu), '0'), 38, 10) is null and 
                    src:cdlu is not null) THEN
                    'cdlu contains a non-numeric value : \'' || AS_VARCHAR(src:cdlu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdoa), '0'), 38, 10) is null and 
                    src:cdoa is not null) THEN
                    'cdoa contains a non-numeric value : \'' || AS_VARCHAR(src:cdoa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdpd_ref_compnr), '0'), 38, 10) is null and 
                    src:cdpd_ref_compnr is not null) THEN
                    'cdpd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdpd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdrd_ref_compnr), '0'), 38, 10) is null and 
                    src:cdrd_ref_compnr is not null) THEN
                    'cdrd_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cdrd_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdro), '0'), 38, 10) is null and 
                    src:cdro is not null) THEN
                    'cdro contains a non-numeric value : \'' || AS_VARCHAR(src:cdro) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coka), '0'), 38, 10) is null and 
                    src:coka is not null) THEN
                    'coka contains a non-numeric value : \'' || AS_VARCHAR(src:coka) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) THEN
                    'cons contains a non-numeric value : \'' || AS_VARCHAR(src:cons) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copa), '0'), 38, 10) is null and 
                    src:copa is not null) THEN
                    'copa contains a non-numeric value : \'' || AS_VARCHAR(src:copa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copc), '0'), 38, 10) is null and 
                    src:copc is not null) THEN
                    'copc contains a non-numeric value : \'' || AS_VARCHAR(src:copc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copp), '0'), 38, 10) is null and 
                    src:copp is not null) THEN
                    'copp contains a non-numeric value : \'' || AS_VARCHAR(src:copp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) THEN
                    'copr contains a non-numeric value : \'' || AS_VARCHAR(src:copr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copu), '0'), 38, 10) is null and 
                    src:copu is not null) THEN
                    'copu contains a non-numeric value : \'' || AS_VARCHAR(src:copu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_arlo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_arlo_ref_compnr is not null) THEN
                    'cwar_arlo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_arlo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_palo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_palo_ref_compnr is not null) THEN
                    'cwar_palo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_palo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) THEN
                    'cwar_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_relo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_relo_ref_compnr is not null) THEN
                    'cwar_relo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_relo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_stlo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_stlo_ref_compnr is not null) THEN
                    'cwar_stlo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cwar_stlo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddti), '0'), 38, 10) is null and 
                    src:ddti is not null) THEN
                    'ddti contains a non-numeric value : \'' || AS_VARCHAR(src:ddti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddwi), '0'), 38, 10) is null and 
                    src:ddwi is not null) THEN
                    'ddwi contains a non-numeric value : \'' || AS_VARCHAR(src:ddwi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddwr), '0'), 38, 10) is null and 
                    src:ddwr is not null) THEN
                    'ddwr contains a non-numeric value : \'' || AS_VARCHAR(src:ddwr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:diws), '0'), 38, 10) is null and 
                    src:diws is not null) THEN
                    'diws contains a non-numeric value : \'' || AS_VARCHAR(src:diws) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmdt), '0'), 38, 10) is null and 
                    src:dmdt is not null) THEN
                    'dmdt contains a non-numeric value : \'' || AS_VARCHAR(src:dmdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmph), '0'), 38, 10) is null and 
                    src:dmph is not null) THEN
                    'dmph contains a non-numeric value : \'' || AS_VARCHAR(src:dmph) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsi), '0'), 38, 10) is null and 
                    src:dmsi is not null) THEN
                    'dmsi contains a non-numeric value : \'' || AS_VARCHAR(src:dmsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsl_ref_compnr), '0'), 38, 10) is null and 
                    src:dmsl_ref_compnr is not null) THEN
                    'dmsl_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:dmsl_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsp), '0'), 38, 10) is null and 
                    src:dmsp is not null) THEN
                    'dmsp contains a non-numeric value : \'' || AS_VARCHAR(src:dmsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsr), '0'), 38, 10) is null and 
                    src:dmsr is not null) THEN
                    'dmsr contains a non-numeric value : \'' || AS_VARCHAR(src:dmsr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmss), '0'), 38, 10) is null and 
                    src:dmss is not null) THEN
                    'dmss contains a non-numeric value : \'' || AS_VARCHAR(src:dmss) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dphi), '0'), 38, 10) is null and 
                    src:dphi is not null) THEN
                    'dphi contains a non-numeric value : \'' || AS_VARCHAR(src:dphi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dprl), '0'), 38, 10) is null and 
                    src:dprl is not null) THEN
                    'dprl contains a non-numeric value : \'' || AS_VARCHAR(src:dprl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptr), '0'), 38, 10) is null and 
                    src:dptr is not null) THEN
                    'dptr contains a non-numeric value : \'' || AS_VARCHAR(src:dptr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dycd), '0'), 38, 10) is null and 
                    src:dycd is not null) THEN
                    'dycd contains a non-numeric value : \'' || AS_VARCHAR(src:dycd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) THEN
                    'fire contains a non-numeric value : \'' || AS_VARCHAR(src:fire) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flhu), '0'), 38, 10) is null and 
                    src:flhu is not null) THEN
                    'flhu contains a non-numeric value : \'' || AS_VARCHAR(src:flhu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flup), '0'), 38, 10) is null and 
                    src:flup is not null) THEN
                    'flup contains a non-numeric value : \'' || AS_VARCHAR(src:flup) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:free), '0'), 38, 10) is null and 
                    src:free is not null) THEN
                    'free contains a non-numeric value : \'' || AS_VARCHAR(src:free) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frlo), '0'), 38, 10) is null and 
                    src:frlo is not null) THEN
                    'frlo contains a non-numeric value : \'' || AS_VARCHAR(src:frlo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:geha), '0'), 38, 10) is null and 
                    src:geha is not null) THEN
                    'geha contains a non-numeric value : \'' || AS_VARCHAR(src:geha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehc), '0'), 38, 10) is null and 
                    src:gehc is not null) THEN
                    'gehc contains a non-numeric value : \'' || AS_VARCHAR(src:gehc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehp), '0'), 38, 10) is null and 
                    src:gehp is not null) THEN
                    'gehp contains a non-numeric value : \'' || AS_VARCHAR(src:gehp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehr), '0'), 38, 10) is null and 
                    src:gehr is not null) THEN
                    'gehr contains a non-numeric value : \'' || AS_VARCHAR(src:gehr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehu), '0'), 38, 10) is null and 
                    src:gehu is not null) THEN
                    'gehu contains a non-numeric value : \'' || AS_VARCHAR(src:gehu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ghps), '0'), 38, 10) is null and 
                    src:ghps is not null) THEN
                    'ghps contains a non-numeric value : \'' || AS_VARCHAR(src:ghps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gnsh), '0'), 38, 10) is null and 
                    src:gnsh is not null) THEN
                    'gnsh contains a non-numeric value : \'' || AS_VARCHAR(src:gnsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ialt), '0'), 38, 10) is null and 
                    src:ialt is not null) THEN
                    'ialt contains a non-numeric value : \'' || AS_VARCHAR(src:ialt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ieod), '0'), 38, 10) is null and 
                    src:ieod is not null) THEN
                    'ieod contains a non-numeric value : \'' || AS_VARCHAR(src:ieod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilmc_ref_compnr), '0'), 38, 10) is null and 
                    src:ilmc_ref_compnr is not null) THEN
                    'ilmc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ilmc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iltm), '0'), 38, 10) is null and 
                    src:iltm is not null) THEN
                    'iltm contains a non-numeric value : \'' || AS_VARCHAR(src:iltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iltu), '0'), 38, 10) is null and 
                    src:iltu is not null) THEN
                    'iltu contains a non-numeric value : \'' || AS_VARCHAR(src:iltu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:imdp_ref_compnr), '0'), 38, 10) is null and 
                    src:imdp_ref_compnr is not null) THEN
                    'imdp_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:imdp_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itlo), '0'), 38, 10) is null and 
                    src:itlo is not null) THEN
                    'itlo contains a non-numeric value : \'' || AS_VARCHAR(src:itlo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:karu_ref_compnr), '0'), 38, 10) is null and 
                    src:karu_ref_compnr is not null) THEN
                    'karu_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:karu_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kmsk_ref_compnr), '0'), 38, 10) is null and 
                    src:kmsk_ref_compnr is not null) THEN
                    'kmsk_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:kmsk_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laba_ref_compnr), '0'), 38, 10) is null and 
                    src:laba_ref_compnr is not null) THEN
                    'laba_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:laba_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labc_ref_compnr), '0'), 38, 10) is null and 
                    src:labc_ref_compnr is not null) THEN
                    'labc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:labc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labi_ref_compnr), '0'), 38, 10) is null and 
                    src:labi_ref_compnr is not null) THEN
                    'labi_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:labi_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labo_ref_compnr), '0'), 38, 10) is null and 
                    src:labo_ref_compnr is not null) THEN
                    'labo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:labo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labr_ref_compnr), '0'), 38, 10) is null and 
                    src:labr_ref_compnr is not null) THEN
                    'labr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:labr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lolo), '0'), 38, 10) is null and 
                    src:lolo is not null) THEN
                    'lolo contains a non-numeric value : \'' || AS_VARCHAR(src:lolo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpad), '0'), 38, 10) is null and 
                    src:lpad is not null) THEN
                    'lpad contains a non-numeric value : \'' || AS_VARCHAR(src:lpad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpas), '0'), 38, 10) is null and 
                    src:lpas is not null) THEN
                    'lpas contains a non-numeric value : \'' || AS_VARCHAR(src:lpas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpcc), '0'), 38, 10) is null and 
                    src:lpcc is not null) THEN
                    'lpcc contains a non-numeric value : \'' || AS_VARCHAR(src:lpcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lppi), '0'), 38, 10) is null and 
                    src:lppi is not null) THEN
                    'lppi contains a non-numeric value : \'' || AS_VARCHAR(src:lppi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpre), '0'), 38, 10) is null and 
                    src:lpre is not null) THEN
                    'lpre contains a non-numeric value : \'' || AS_VARCHAR(src:lpre) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mada), '0'), 38, 10) is null and 
                    src:mada is not null) THEN
                    'mada contains a non-numeric value : \'' || AS_VARCHAR(src:mada) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:masi), '0'), 38, 10) is null and 
                    src:masi is not null) THEN
                    'masi contains a non-numeric value : \'' || AS_VARCHAR(src:masi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:masu), '0'), 38, 10) is null and 
                    src:masu is not null) THEN
                    'masu contains a non-numeric value : \'' || AS_VARCHAR(src:masu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matt), '0'), 38, 10) is null and 
                    src:matt is not null) THEN
                    'matt contains a non-numeric value : \'' || AS_VARCHAR(src:matt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matu), '0'), 38, 10) is null and 
                    src:matu is not null) THEN
                    'matu contains a non-numeric value : \'' || AS_VARCHAR(src:matu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcca), '0'), 38, 10) is null and 
                    src:mcca is not null) THEN
                    'mcca contains a non-numeric value : \'' || AS_VARCHAR(src:mcca) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mipa), '0'), 38, 10) is null and 
                    src:mipa is not null) THEN
                    'mipa contains a non-numeric value : \'' || AS_VARCHAR(src:mipa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:misi), '0'), 38, 10) is null and 
                    src:misi is not null) THEN
                    'misi contains a non-numeric value : \'' || AS_VARCHAR(src:misi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:misu), '0'), 38, 10) is null and 
                    src:misu is not null) THEN
                    'misu contains a non-numeric value : \'' || AS_VARCHAR(src:misu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitt), '0'), 38, 10) is null and 
                    src:mitt is not null) THEN
                    'mitt contains a non-numeric value : \'' || AS_VARCHAR(src:mitt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitu), '0'), 38, 10) is null and 
                    src:mitu is not null) THEN
                    'mitu contains a non-numeric value : \'' || AS_VARCHAR(src:mitu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopa), '0'), 38, 10) is null and 
                    src:mopa is not null) THEN
                    'mopa contains a non-numeric value : \'' || AS_VARCHAR(src:mopa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrhu), '0'), 38, 10) is null and 
                    src:mrhu is not null) THEN
                    'mrhu contains a non-numeric value : \'' || AS_VARCHAR(src:mrhu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olmc_ref_compnr), '0'), 38, 10) is null and 
                    src:olmc_ref_compnr is not null) THEN
                    'olmc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:olmc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) THEN
                    'oltm contains a non-numeric value : \'' || AS_VARCHAR(src:oltm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltu), '0'), 38, 10) is null and 
                    src:oltu is not null) THEN
                    'oltu contains a non-numeric value : \'' || AS_VARCHAR(src:oltu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orbo), '0'), 38, 10) is null and 
                    src:orbo is not null) THEN
                    'orbo contains a non-numeric value : \'' || AS_VARCHAR(src:orbo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prla), '0'), 38, 10) is null and 
                    src:prla is not null) THEN
                    'prla contains a non-numeric value : \'' || AS_VARCHAR(src:prla) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlc), '0'), 38, 10) is null and 
                    src:prlc is not null) THEN
                    'prlc contains a non-numeric value : \'' || AS_VARCHAR(src:prlc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlp), '0'), 38, 10) is null and 
                    src:prlp is not null) THEN
                    'prlp contains a non-numeric value : \'' || AS_VARCHAR(src:prlp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlr), '0'), 38, 10) is null and 
                    src:prlr is not null) THEN
                    'prlr contains a non-numeric value : \'' || AS_VARCHAR(src:prlr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlu), '0'), 38, 10) is null and 
                    src:prlu is not null) THEN
                    'prlu contains a non-numeric value : \'' || AS_VARCHAR(src:prlu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmp), '0'), 38, 10) is null and 
                    src:prmp is not null) THEN
                    'prmp contains a non-numeric value : \'' || AS_VARCHAR(src:prmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmr), '0'), 38, 10) is null and 
                    src:prmr is not null) THEN
                    'prmr contains a non-numeric value : \'' || AS_VARCHAR(src:prmr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmu), '0'), 38, 10) is null and 
                    src:prmu is not null) THEN
                    'prmu contains a non-numeric value : \'' || AS_VARCHAR(src:prmu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva), '0'), 38, 10) is null and 
                    src:prva is not null) THEN
                    'prva contains a non-numeric value : \'' || AS_VARCHAR(src:prva) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qfma), '0'), 38, 10) is null and 
                    src:qfma is not null) THEN
                    'qfma contains a non-numeric value : \'' || AS_VARCHAR(src:qfma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qfmi), '0'), 38, 10) is null and 
                    src:qfmi is not null) THEN
                    'qfmi contains a non-numeric value : \'' || AS_VARCHAR(src:qfmi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qmoo), '0'), 38, 10) is null and 
                    src:qmoo is not null) THEN
                    'qmoo contains a non-numeric value : \'' || AS_VARCHAR(src:qmoo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:qwrh_ref_compnr is not null) THEN
                    'qwrh_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:qwrh_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) THEN
                    'ract_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ract_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reje), '0'), 38, 10) is null and 
                    src:reje is not null) THEN
                    'reje contains a non-numeric value : \'' || AS_VARCHAR(src:reje) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa), '0'), 38, 10) is null and 
                    src:rfsa is not null) THEN
                    'rfsa contains a non-numeric value : \'' || AS_VARCHAR(src:rfsa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rilt), '0'), 38, 10) is null and 
                    src:rilt is not null) THEN
                    'rilt contains a non-numeric value : \'' || AS_VARCHAR(src:rilt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rilu), '0'), 38, 10) is null and 
                    src:rilu is not null) THEN
                    'rilu contains a non-numeric value : \'' || AS_VARCHAR(src:rilu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rkbs), '0'), 38, 10) is null and 
                    src:rkbs is not null) THEN
                    'rkbs contains a non-numeric value : \'' || AS_VARCHAR(src:rkbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rolt), '0'), 38, 10) is null and 
                    src:rolt is not null) THEN
                    'rolt contains a non-numeric value : \'' || AS_VARCHAR(src:rolt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rolu), '0'), 38, 10) is null and 
                    src:rolu is not null) THEN
                    'rolu contains a non-numeric value : \'' || AS_VARCHAR(src:rolu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsdn), '0'), 38, 10) is null and 
                    src:rsdn is not null) THEN
                    'rsdn contains a non-numeric value : \'' || AS_VARCHAR(src:rsdn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsrc), '0'), 38, 10) is null and 
                    src:rsrc is not null) THEN
                    'rsrc contains a non-numeric value : \'' || AS_VARCHAR(src:rsrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcf), '0'), 38, 10) is null and 
                    src:rtcf is not null) THEN
                    'rtcf contains a non-numeric value : \'' || AS_VARCHAR(src:rtcf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scom), '0'), 38, 10) is null and 
                    src:scom is not null) THEN
                    'scom contains a non-numeric value : \'' || AS_VARCHAR(src:scom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfwh), '0'), 38, 10) is null and 
                    src:sfwh is not null) THEN
                    'sfwh contains a non-numeric value : \'' || AS_VARCHAR(src:sfwh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shhu), '0'), 38, 10) is null and 
                    src:shhu is not null) THEN
                    'shhu contains a non-numeric value : \'' || AS_VARCHAR(src:shhu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shtw_ref_compnr), '0'), 38, 10) is null and 
                    src:shtw_ref_compnr is not null) THEN
                    'shtw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:shtw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) THEN
                    'site_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:site_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sloc), '0'), 38, 10) is null and 
                    src:sloc is not null) THEN
                    'sloc contains a non-numeric value : \'' || AS_VARCHAR(src:sloc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smsh), '0'), 38, 10) is null and 
                    src:smsh is not null) THEN
                    'smsh contains a non-numeric value : \'' || AS_VARCHAR(src:smsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spps), '0'), 38, 10) is null and 
                    src:spps is not null) THEN
                    'spps contains a non-numeric value : \'' || AS_VARCHAR(src:spps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sscc), '0'), 38, 10) is null and 
                    src:sscc is not null) THEN
                    'sscc contains a non-numeric value : \'' || AS_VARCHAR(src:sscc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sups), '0'), 38, 10) is null and 
                    src:sups is not null) THEN
                    'sups contains a non-numeric value : \'' || AS_VARCHAR(src:sups) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:supw_ref_compnr), '0'), 38, 10) is null and 
                    src:supw_ref_compnr is not null) THEN
                    'supw_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:supw_ref_compnr) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdc), '0'), 38, 10) is null and 
                    src:trdc is not null) THEN
                    'trdc contains a non-numeric value : \'' || AS_VARCHAR(src:trdc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ufpl), '0'), 38, 10) is null and 
                    src:ufpl is not null) THEN
                    'ufpl contains a non-numeric value : \'' || AS_VARCHAR(src:ufpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhii), '0'), 38, 10) is null and 
                    src:uhii is not null) THEN
                    'uhii contains a non-numeric value : \'' || AS_VARCHAR(src:uhii) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhin), '0'), 38, 10) is null and 
                    src:uhin is not null) THEN
                    'uhin contains a non-numeric value : \'' || AS_VARCHAR(src:uhin) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhir), '0'), 38, 10) is null and 
                    src:uhir is not null) THEN
                    'uhir contains a non-numeric value : \'' || AS_VARCHAR(src:uhir) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhis), '0'), 38, 10) is null and 
                    src:uhis is not null) THEN
                    'uhis contains a non-numeric value : \'' || AS_VARCHAR(src:uhis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhnd), '0'), 38, 10) is null and 
                    src:uhnd is not null) THEN
                    'uhnd contains a non-numeric value : \'' || AS_VARCHAR(src:uhnd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhoa), '0'), 38, 10) is null and 
                    src:uhoa is not null) THEN
                    'uhoa contains a non-numeric value : \'' || AS_VARCHAR(src:uhoa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhri), '0'), 38, 10) is null and 
                    src:uhri is not null) THEN
                    'uhri contains a non-numeric value : \'' || AS_VARCHAR(src:uhri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uidt), '0'), 38, 10) is null and 
                    src:uidt is not null) THEN
                    'uidt contains a non-numeric value : \'' || AS_VARCHAR(src:uidt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uolo), '0'), 38, 10) is null and 
                    src:uolo is not null) THEN
                    'uolo contains a non-numeric value : \'' || AS_VARCHAR(src:uolo) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usmd), '0'), 38, 10) is null and 
                    src:usmd is not null) THEN
                    'usmd contains a non-numeric value : \'' || AS_VARCHAR(src:usmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usse), '0'), 38, 10) is null and 
                    src:usse is not null) THEN
                    'usse contains a non-numeric value : \'' || AS_VARCHAR(src:usse) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uudl), '0'), 38, 10) is null and 
                    src:uudl is not null) THEN
                    'uudl contains a non-numeric value : \'' || AS_VARCHAR(src:uudl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwtr), '0'), 38, 10) is null and 
                    src:uwtr is not null) THEN
                    'uwtr contains a non-numeric value : \'' || AS_VARCHAR(src:uwtr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vpad), '0'), 38, 10) is null and 
                    src:vpad is not null) THEN
                    'vpad contains a non-numeric value : \'' || AS_VARCHAR(src:vpad) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vpcc), '0'), 38, 10) is null and 
                    src:vpcc is not null) THEN
                    'vpcc contains a non-numeric value : \'' || AS_VARCHAR(src:vpcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wvgr_ref_compnr), '0'), 38, 10) is null and 
                    src:wvgr_ref_compnr is not null) THEN
                    'wvgr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:wvgr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zere), '0'), 38, 10) is null and 
                    src:zere is not null) THEN
                    'zere contains a non-numeric value : \'' || AS_VARCHAR(src:zere) || '\'' WHEN 
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
                src:cwar  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_WHWMD200 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aipd), '0'), 38, 10) is null and 
                    src:aipd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aoit), '0'), 38, 10) is null and 
                    src:aoit is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcyc), '0'), 38, 10) is null and 
                    src:bcyc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:binb), '0'), 38, 10) is null and 
                    src:binb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bloc_ref_compnr), '0'), 38, 10) is null and 
                    src:bloc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bout), '0'), 38, 10) is null and 
                    src:bout is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccal_ref_compnr), '0'), 38, 10) is null and 
                    src:ccal_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdap), '0'), 38, 10) is null and 
                    src:cdap is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdcr), '0'), 38, 10) is null and 
                    src:cdcr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdlt), '0'), 38, 10) is null and 
                    src:cdlt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdlu), '0'), 38, 10) is null and 
                    src:cdlu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdoa), '0'), 38, 10) is null and 
                    src:cdoa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdpd_ref_compnr), '0'), 38, 10) is null and 
                    src:cdpd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdrd_ref_compnr), '0'), 38, 10) is null and 
                    src:cdrd_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cdro), '0'), 38, 10) is null and 
                    src:cdro is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coka), '0'), 38, 10) is null and 
                    src:coka is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cons), '0'), 38, 10) is null and 
                    src:cons is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copa), '0'), 38, 10) is null and 
                    src:copa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copc), '0'), 38, 10) is null and 
                    src:copc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copp), '0'), 38, 10) is null and 
                    src:copp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copr), '0'), 38, 10) is null and 
                    src:copr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:copu), '0'), 38, 10) is null and 
                    src:copu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_arlo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_arlo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_palo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_palo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_relo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_relo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cwar_stlo_ref_compnr), '0'), 38, 10) is null and 
                    src:cwar_stlo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddti), '0'), 38, 10) is null and 
                    src:ddti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddwi), '0'), 38, 10) is null and 
                    src:ddwi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ddwr), '0'), 38, 10) is null and 
                    src:ddwr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:diws), '0'), 38, 10) is null and 
                    src:diws is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmdt), '0'), 38, 10) is null and 
                    src:dmdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmph), '0'), 38, 10) is null and 
                    src:dmph is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsi), '0'), 38, 10) is null and 
                    src:dmsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsl_ref_compnr), '0'), 38, 10) is null and 
                    src:dmsl_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsp), '0'), 38, 10) is null and 
                    src:dmsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmsr), '0'), 38, 10) is null and 
                    src:dmsr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dmss), '0'), 38, 10) is null and 
                    src:dmss is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dphi), '0'), 38, 10) is null and 
                    src:dphi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dprl), '0'), 38, 10) is null and 
                    src:dprl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dptr), '0'), 38, 10) is null and 
                    src:dptr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dycd), '0'), 38, 10) is null and 
                    src:dycd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fire), '0'), 38, 10) is null and 
                    src:fire is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flhu), '0'), 38, 10) is null and 
                    src:flhu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:flup), '0'), 38, 10) is null and 
                    src:flup is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:free), '0'), 38, 10) is null and 
                    src:free is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frlo), '0'), 38, 10) is null and 
                    src:frlo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:geha), '0'), 38, 10) is null and 
                    src:geha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehc), '0'), 38, 10) is null and 
                    src:gehc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehp), '0'), 38, 10) is null and 
                    src:gehp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehr), '0'), 38, 10) is null and 
                    src:gehr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gehu), '0'), 38, 10) is null and 
                    src:gehu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ghps), '0'), 38, 10) is null and 
                    src:ghps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gnsh), '0'), 38, 10) is null and 
                    src:gnsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ialt), '0'), 38, 10) is null and 
                    src:ialt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ieod), '0'), 38, 10) is null and 
                    src:ieod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilmc_ref_compnr), '0'), 38, 10) is null and 
                    src:ilmc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iltm), '0'), 38, 10) is null and 
                    src:iltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:iltu), '0'), 38, 10) is null and 
                    src:iltu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:imdp_ref_compnr), '0'), 38, 10) is null and 
                    src:imdp_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itlo), '0'), 38, 10) is null and 
                    src:itlo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:karu_ref_compnr), '0'), 38, 10) is null and 
                    src:karu_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:kmsk_ref_compnr), '0'), 38, 10) is null and 
                    src:kmsk_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:laba_ref_compnr), '0'), 38, 10) is null and 
                    src:laba_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labc_ref_compnr), '0'), 38, 10) is null and 
                    src:labc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labi_ref_compnr), '0'), 38, 10) is null and 
                    src:labi_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labo_ref_compnr), '0'), 38, 10) is null and 
                    src:labo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:labr_ref_compnr), '0'), 38, 10) is null and 
                    src:labr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lolo), '0'), 38, 10) is null and 
                    src:lolo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpad), '0'), 38, 10) is null and 
                    src:lpad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpas), '0'), 38, 10) is null and 
                    src:lpas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpcc), '0'), 38, 10) is null and 
                    src:lpcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lppi), '0'), 38, 10) is null and 
                    src:lppi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpre), '0'), 38, 10) is null and 
                    src:lpre is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mada), '0'), 38, 10) is null and 
                    src:mada is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:masi), '0'), 38, 10) is null and 
                    src:masi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:masu), '0'), 38, 10) is null and 
                    src:masu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matt), '0'), 38, 10) is null and 
                    src:matt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:matu), '0'), 38, 10) is null and 
                    src:matu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mcca), '0'), 38, 10) is null and 
                    src:mcca is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mipa), '0'), 38, 10) is null and 
                    src:mipa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:misi), '0'), 38, 10) is null and 
                    src:misi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:misu), '0'), 38, 10) is null and 
                    src:misu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitt), '0'), 38, 10) is null and 
                    src:mitt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mitu), '0'), 38, 10) is null and 
                    src:mitu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mopa), '0'), 38, 10) is null and 
                    src:mopa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mrhu), '0'), 38, 10) is null and 
                    src:mrhu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:olmc_ref_compnr), '0'), 38, 10) is null and 
                    src:olmc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltm), '0'), 38, 10) is null and 
                    src:oltm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:oltu), '0'), 38, 10) is null and 
                    src:oltu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:orbo), '0'), 38, 10) is null and 
                    src:orbo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prla), '0'), 38, 10) is null and 
                    src:prla is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlc), '0'), 38, 10) is null and 
                    src:prlc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlp), '0'), 38, 10) is null and 
                    src:prlp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlr), '0'), 38, 10) is null and 
                    src:prlr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prlu), '0'), 38, 10) is null and 
                    src:prlu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmp), '0'), 38, 10) is null and 
                    src:prmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmr), '0'), 38, 10) is null and 
                    src:prmr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prmu), '0'), 38, 10) is null and 
                    src:prmu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prva), '0'), 38, 10) is null and 
                    src:prva is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qfma), '0'), 38, 10) is null and 
                    src:qfma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qfmi), '0'), 38, 10) is null and 
                    src:qfmi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qmoo), '0'), 38, 10) is null and 
                    src:qmoo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qwrh_ref_compnr), '0'), 38, 10) is null and 
                    src:qwrh_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ract_ref_compnr), '0'), 38, 10) is null and 
                    src:ract_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reje), '0'), 38, 10) is null and 
                    src:reje is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rfsa), '0'), 38, 10) is null and 
                    src:rfsa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rilt), '0'), 38, 10) is null and 
                    src:rilt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rilu), '0'), 38, 10) is null and 
                    src:rilu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rkbs), '0'), 38, 10) is null and 
                    src:rkbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rolt), '0'), 38, 10) is null and 
                    src:rolt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rolu), '0'), 38, 10) is null and 
                    src:rolu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsdn), '0'), 38, 10) is null and 
                    src:rsdn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsrc), '0'), 38, 10) is null and 
                    src:rsrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rtcf), '0'), 38, 10) is null and 
                    src:rtcf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scom), '0'), 38, 10) is null and 
                    src:scom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sfwh), '0'), 38, 10) is null and 
                    src:sfwh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shhu), '0'), 38, 10) is null and 
                    src:shhu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:shtw_ref_compnr), '0'), 38, 10) is null and 
                    src:shtw_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:site_ref_compnr), '0'), 38, 10) is null and 
                    src:site_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sloc), '0'), 38, 10) is null and 
                    src:sloc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smsh), '0'), 38, 10) is null and 
                    src:smsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spps), '0'), 38, 10) is null and 
                    src:spps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sscc), '0'), 38, 10) is null and 
                    src:sscc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sups), '0'), 38, 10) is null and 
                    src:sups is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:supw_ref_compnr), '0'), 38, 10) is null and 
                    src:supw_ref_compnr is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trdc), '0'), 38, 10) is null and 
                    src:trdc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ufpl), '0'), 38, 10) is null and 
                    src:ufpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhii), '0'), 38, 10) is null and 
                    src:uhii is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhin), '0'), 38, 10) is null and 
                    src:uhin is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhir), '0'), 38, 10) is null and 
                    src:uhir is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhis), '0'), 38, 10) is null and 
                    src:uhis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhnd), '0'), 38, 10) is null and 
                    src:uhnd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhoa), '0'), 38, 10) is null and 
                    src:uhoa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uhri), '0'), 38, 10) is null and 
                    src:uhri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uidt), '0'), 38, 10) is null and 
                    src:uidt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uolo), '0'), 38, 10) is null and 
                    src:uolo is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usmd), '0'), 38, 10) is null and 
                    src:usmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usse), '0'), 38, 10) is null and 
                    src:usse is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uudl), '0'), 38, 10) is null and 
                    src:uudl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwtr), '0'), 38, 10) is null and 
                    src:uwtr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vpad), '0'), 38, 10) is null and 
                    src:vpad is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vpcc), '0'), 38, 10) is null and 
                    src:vpcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wvgr_ref_compnr), '0'), 38, 10) is null and 
                    src:wvgr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:zere), '0'), 38, 10) is null and 
                    src:zere is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)