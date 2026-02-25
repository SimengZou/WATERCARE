CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TCCOM000_ERROR AS SELECT src, 'LN_TCCOM000' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfg), '0'), 38, 10) is null and 
                    src:acfg is not null) THEN
                    'acfg contains a non-numeric value : \'' || AS_VARCHAR(src:acfg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adcc), '0'), 38, 10) is null and 
                    src:adcc is not null) THEN
                    'adcc contains a non-numeric value : \'' || AS_VARCHAR(src:adcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alhp), '0'), 38, 10) is null and 
                    src:alhp is not null) THEN
                    'alhp contains a non-numeric value : \'' || AS_VARCHAR(src:alhp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altm), '0'), 38, 10) is null and 
                    src:altm is not null) THEN
                    'altm contains a non-numeric value : \'' || AS_VARCHAR(src:altm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arcc), '0'), 38, 10) is null and 
                    src:arcc is not null) THEN
                    'arcc contains a non-numeric value : \'' || AS_VARCHAR(src:arcc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ardc), '0'), 38, 10) is null and 
                    src:ardc is not null) THEN
                    'ardc contains a non-numeric value : \'' || AS_VARCHAR(src:ardc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asci), '0'), 38, 10) is null and 
                    src:asci is not null) THEN
                    'asci contains a non-numeric value : \'' || AS_VARCHAR(src:asci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asec), '0'), 38, 10) is null and 
                    src:asec is not null) THEN
                    'asec contains a non-numeric value : \'' || AS_VARCHAR(src:asec) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bape), '0'), 38, 10) is null and 
                    src:bape is not null) THEN
                    'bape contains a non-numeric value : \'' || AS_VARCHAR(src:bape) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcmi), '0'), 38, 10) is null and 
                    src:bcmi is not null) THEN
                    'bcmi contains a non-numeric value : \'' || AS_VARCHAR(src:bcmi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgci), '0'), 38, 10) is null and 
                    src:bgci is not null) THEN
                    'bgci contains a non-numeric value : \'' || AS_VARCHAR(src:bgci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bulg), '0'), 38, 10) is null and 
                    src:bulg is not null) THEN
                    'bulg contains a non-numeric value : \'' || AS_VARCHAR(src:bulg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) THEN
                    'cadr_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:cadr_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) THEN
                    'capp contains a non-numeric value : \'' || AS_VARCHAR(src:capp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrk), '0'), 38, 10) is null and 
                    src:cbrk is not null) THEN
                    'cbrk contains a non-numeric value : \'' || AS_VARCHAR(src:cbrk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) THEN
                    'ccty_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ccty_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfbs), '0'), 38, 10) is null and 
                    src:cfbs is not null) THEN
                    'cfbs contains a non-numeric value : \'' || AS_VARCHAR(src:cfbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfri), '0'), 38, 10) is null and 
                    src:cfri is not null) THEN
                    'cfri contains a non-numeric value : \'' || AS_VARCHAR(src:cfri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) THEN
                    'cinv contains a non-numeric value : \'' || AS_VARCHAR(src:cinv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) THEN
                    'clan_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:clan_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clbi), '0'), 38, 10) is null and 
                    src:clbi is not null) THEN
                    'clbi contains a non-numeric value : \'' || AS_VARCHAR(src:clbi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) THEN
                    'corg contains a non-numeric value : \'' || AS_VARCHAR(src:corg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpli), '0'), 38, 10) is null and 
                    src:cpli is not null) THEN
                    'cpli contains a non-numeric value : \'' || AS_VARCHAR(src:cpli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csli), '0'), 38, 10) is null and 
                    src:csli is not null) THEN
                    'csli contains a non-numeric value : \'' || AS_VARCHAR(src:csli) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:darc), '0'), 38, 10) is null and 
                    src:darc is not null) THEN
                    'darc contains a non-numeric value : \'' || AS_VARCHAR(src:darc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dasr), '0'), 38, 10) is null and 
                    src:dasr is not null) THEN
                    'dasr contains a non-numeric value : \'' || AS_VARCHAR(src:dasr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:deln), '0'), 38, 10) is null and 
                    src:deln is not null) THEN
                    'deln contains a non-numeric value : \'' || AS_VARCHAR(src:deln) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsri), '0'), 38, 10) is null and 
                    src:dsri is not null) THEN
                    'dsri contains a non-numeric value : \'' || AS_VARCHAR(src:dsri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:duns), '0'), 38, 10) is null and 
                    src:duns is not null) THEN
                    'duns contains a non-numeric value : \'' || AS_VARCHAR(src:duns) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eaas), '0'), 38, 10) is null and 
                    src:eaas is not null) THEN
                    'eaas contains a non-numeric value : \'' || AS_VARCHAR(src:eaas) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecmi), '0'), 38, 10) is null and 
                    src:ecmi is not null) THEN
                    'ecmi contains a non-numeric value : \'' || AS_VARCHAR(src:ecmi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erac), '0'), 38, 10) is null and 
                    src:erac is not null) THEN
                    'erac contains a non-numeric value : \'' || AS_VARCHAR(src:erac) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eupl), '0'), 38, 10) is null and 
                    src:eupl is not null) THEN
                    'eupl contains a non-numeric value : \'' || AS_VARCHAR(src:eupl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eusl), '0'), 38, 10) is null and 
                    src:eusl is not null) THEN
                    'eusl contains a non-numeric value : \'' || AS_VARCHAR(src:eusl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eust), '0'), 38, 10) is null and 
                    src:eust is not null) THEN
                    'eust contains a non-numeric value : \'' || AS_VARCHAR(src:eust) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffpl), '0'), 38, 10) is null and 
                    src:ffpl is not null) THEN
                    'ffpl contains a non-numeric value : \'' || AS_VARCHAR(src:ffpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fini), '0'), 38, 10) is null and 
                    src:fini is not null) THEN
                    'fini contains a non-numeric value : \'' || AS_VARCHAR(src:fini) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frmi), '0'), 38, 10) is null and 
                    src:frmi is not null) THEN
                    'frmi contains a non-numeric value : \'' || AS_VARCHAR(src:frmi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstk), '0'), 38, 10) is null and 
                    src:fstk is not null) THEN
                    'fstk contains a non-numeric value : \'' || AS_VARCHAR(src:fstk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grpl), '0'), 38, 10) is null and 
                    src:grpl is not null) THEN
                    'grpl contains a non-numeric value : \'' || AS_VARCHAR(src:grpl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gtci), '0'), 38, 10) is null and 
                    src:gtci is not null) THEN
                    'gtci contains a non-numeric value : \'' || AS_VARCHAR(src:gtci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gtro), '0'), 38, 10) is null and 
                    src:gtro is not null) THEN
                    'gtro contains a non-numeric value : \'' || AS_VARCHAR(src:gtro) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hcst), '0'), 38, 10) is null and 
                    src:hcst is not null) THEN
                    'hcst contains a non-numeric value : \'' || AS_VARCHAR(src:hcst) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilwh), '0'), 38, 10) is null and 
                    src:ilwh is not null) THEN
                    'ilwh contains a non-numeric value : \'' || AS_VARCHAR(src:ilwh) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) THEN
                    'indt contains a non-timestamp value : \'' || AS_VARCHAR(src:indt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpr), '0'), 38, 10) is null and 
                    src:inpr is not null) THEN
                    'inpr contains a non-numeric value : \'' || AS_VARCHAR(src:inpr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intr), '0'), 38, 10) is null and 
                    src:intr is not null) THEN
                    'intr contains a non-numeric value : \'' || AS_VARCHAR(src:intr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itpd), '0'), 38, 10) is null and 
                    src:itpd is not null) THEN
                    'itpd contains a non-numeric value : \'' || AS_VARCHAR(src:itpd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itri), '0'), 38, 10) is null and 
                    src:itri is not null) THEN
                    'itri contains a non-numeric value : \'' || AS_VARCHAR(src:itri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jspm), '0'), 38, 10) is null and 
                    src:jspm is not null) THEN
                    'jspm contains a non-numeric value : \'' || AS_VARCHAR(src:jspm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lago), '0'), 38, 10) is null and 
                    src:lago is not null) THEN
                    'lago contains a non-numeric value : \'' || AS_VARCHAR(src:lago) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:larg), '0'), 38, 10) is null and 
                    src:larg is not null) THEN
                    'larg contains a non-numeric value : \'' || AS_VARCHAR(src:larg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbra), '0'), 38, 10) is null and 
                    src:lbra is not null) THEN
                    'lbra contains a non-numeric value : \'' || AS_VARCHAR(src:lbra) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lche), '0'), 38, 10) is null and 
                    src:lche is not null) THEN
                    'lche contains a non-numeric value : \'' || AS_VARCHAR(src:lche) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lchl), '0'), 38, 10) is null and 
                    src:lchl is not null) THEN
                    'lchl contains a non-numeric value : \'' || AS_VARCHAR(src:lchl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lchn), '0'), 38, 10) is null and 
                    src:lchn is not null) THEN
                    'lchn contains a non-numeric value : \'' || AS_VARCHAR(src:lchn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcol), '0'), 38, 10) is null and 
                    src:lcol is not null) THEN
                    'lcol contains a non-numeric value : \'' || AS_VARCHAR(src:lcol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcsi), '0'), 38, 10) is null and 
                    src:lcsi is not null) THEN
                    'lcsi contains a non-numeric value : \'' || AS_VARCHAR(src:lcsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcze), '0'), 38, 10) is null and 
                    src:lcze is not null) THEN
                    'lcze contains a non-numeric value : \'' || AS_VARCHAR(src:lcze) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldeu), '0'), 38, 10) is null and 
                    src:ldeu is not null) THEN
                    'ldeu contains a non-numeric value : \'' || AS_VARCHAR(src:ldeu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lesp), '0'), 38, 10) is null and 
                    src:lesp is not null) THEN
                    'lesp contains a non-numeric value : \'' || AS_VARCHAR(src:lesp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lgbr), '0'), 38, 10) is null and 
                    src:lgbr is not null) THEN
                    'lgbr contains a non-numeric value : \'' || AS_VARCHAR(src:lgbr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lhrv), '0'), 38, 10) is null and 
                    src:lhrv is not null) THEN
                    'lhrv contains a non-numeric value : \'' || AS_VARCHAR(src:lhrv) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lhun), '0'), 38, 10) is null and 
                    src:lhun is not null) THEN
                    'lhun contains a non-numeric value : \'' || AS_VARCHAR(src:lhun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lidn), '0'), 38, 10) is null and 
                    src:lidn is not null) THEN
                    'lidn contains a non-numeric value : \'' || AS_VARCHAR(src:lidn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lind), '0'), 38, 10) is null and 
                    src:lind is not null) THEN
                    'lind contains a non-numeric value : \'' || AS_VARCHAR(src:lind) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lisr), '0'), 38, 10) is null and 
                    src:lisr is not null) THEN
                    'lisr contains a non-numeric value : \'' || AS_VARCHAR(src:lisr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lita), '0'), 38, 10) is null and 
                    src:lita is not null) THEN
                    'lita contains a non-numeric value : \'' || AS_VARCHAR(src:lita) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmex), '0'), 38, 10) is null and 
                    src:lmex is not null) THEN
                    'lmex contains a non-numeric value : \'' || AS_VARCHAR(src:lmex) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmys), '0'), 38, 10) is null and 
                    src:lmys is not null) THEN
                    'lmys contains a non-numeric value : \'' || AS_VARCHAR(src:lmys) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnor), '0'), 38, 10) is null and 
                    src:lnor is not null) THEN
                    'lnor contains a non-numeric value : \'' || AS_VARCHAR(src:lnor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnwi), '0'), 38, 10) is null and 
                    src:lnwi is not null) THEN
                    'lnwi contains a non-numeric value : \'' || AS_VARCHAR(src:lnwi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lper), '0'), 38, 10) is null and 
                    src:lper is not null) THEN
                    'lper contains a non-numeric value : \'' || AS_VARCHAR(src:lper) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lphl), '0'), 38, 10) is null and 
                    src:lphl is not null) THEN
                    'lphl contains a non-numeric value : \'' || AS_VARCHAR(src:lphl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpol), '0'), 38, 10) is null and 
                    src:lpol is not null) THEN
                    'lpol contains a non-numeric value : \'' || AS_VARCHAR(src:lpol) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpor), '0'), 38, 10) is null and 
                    src:lpor is not null) THEN
                    'lpor contains a non-numeric value : \'' || AS_VARCHAR(src:lpor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lrom), '0'), 38, 10) is null and 
                    src:lrom is not null) THEN
                    'lrom contains a non-numeric value : \'' || AS_VARCHAR(src:lrom) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lrus), '0'), 38, 10) is null and 
                    src:lrus is not null) THEN
                    'lrus contains a non-numeric value : \'' || AS_VARCHAR(src:lrus) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsau), '0'), 38, 10) is null and 
                    src:lsau is not null) THEN
                    'lsau contains a non-numeric value : \'' || AS_VARCHAR(src:lsau) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsrb), '0'), 38, 10) is null and 
                    src:lsrb is not null) THEN
                    'lsrb contains a non-numeric value : \'' || AS_VARCHAR(src:lsrb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsvk), '0'), 38, 10) is null and 
                    src:lsvk is not null) THEN
                    'lsvk contains a non-numeric value : \'' || AS_VARCHAR(src:lsvk) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsvn), '0'), 38, 10) is null and 
                    src:lsvn is not null) THEN
                    'lsvn contains a non-numeric value : \'' || AS_VARCHAR(src:lsvn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltha), '0'), 38, 10) is null and 
                    src:ltha is not null) THEN
                    'ltha contains a non-numeric value : \'' || AS_VARCHAR(src:ltha) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltur), '0'), 38, 10) is null and 
                    src:ltur is not null) THEN
                    'ltur contains a non-numeric value : \'' || AS_VARCHAR(src:ltur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvnm), '0'), 38, 10) is null and 
                    src:lvnm is not null) THEN
                    'lvnm contains a non-numeric value : \'' || AS_VARCHAR(src:lvnm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mejs), '0'), 38, 10) is null and 
                    src:mejs is not null) THEN
                    'mejs contains a non-numeric value : \'' || AS_VARCHAR(src:mejs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mesa), '0'), 38, 10) is null and 
                    src:mesa is not null) THEN
                    'mesa contains a non-numeric value : \'' || AS_VARCHAR(src:mesa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfsi), '0'), 38, 10) is null and 
                    src:mfsi is not null) THEN
                    'mfsi contains a non-numeric value : \'' || AS_VARCHAR(src:mfsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnfc), '0'), 38, 10) is null and 
                    src:mnfc is not null) THEN
                    'mnfc contains a non-numeric value : \'' || AS_VARCHAR(src:mnfc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpni), '0'), 38, 10) is null and 
                    src:mpni is not null) THEN
                    'mpni contains a non-numeric value : \'' || AS_VARCHAR(src:mpni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mprs), '0'), 38, 10) is null and 
                    src:mprs is not null) THEN
                    'mprs contains a non-numeric value : \'' || AS_VARCHAR(src:mprs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mstc), '0'), 38, 10) is null and 
                    src:mstc is not null) THEN
                    'mstc contains a non-numeric value : \'' || AS_VARCHAR(src:mstc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) THEN
                    'ncmp contains a non-numeric value : \'' || AS_VARCHAR(src:ncmp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odmu), '0'), 38, 10) is null and 
                    src:odmu is not null) THEN
                    'odmu contains a non-numeric value : \'' || AS_VARCHAR(src:odmu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opsi), '0'), 38, 10) is null and 
                    src:opsi is not null) THEN
                    'opsi contains a non-numeric value : \'' || AS_VARCHAR(src:opsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owne), '0'), 38, 10) is null and 
                    src:owne is not null) THEN
                    'owne contains a non-numeric value : \'' || AS_VARCHAR(src:owne) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owni), '0'), 38, 10) is null and 
                    src:owni is not null) THEN
                    'owni contains a non-numeric value : \'' || AS_VARCHAR(src:owni) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcfi), '0'), 38, 10) is null and 
                    src:pcfi is not null) THEN
                    'pcfi contains a non-numeric value : \'' || AS_VARCHAR(src:pcfi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmd), '0'), 38, 10) is null and 
                    src:pcmd is not null) THEN
                    'pcmd contains a non-numeric value : \'' || AS_VARCHAR(src:pcmd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsi), '0'), 38, 10) is null and 
                    src:pcsi is not null) THEN
                    'pcsi contains a non-numeric value : \'' || AS_VARCHAR(src:pcsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdim), '0'), 38, 10) is null and 
                    src:pdim is not null) THEN
                    'pdim contains a non-numeric value : \'' || AS_VARCHAR(src:pdim) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pedi), '0'), 38, 10) is null and 
                    src:pedi is not null) THEN
                    'pedi contains a non-numeric value : \'' || AS_VARCHAR(src:pedi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plai), '0'), 38, 10) is null and 
                    src:plai is not null) THEN
                    'plai contains a non-numeric value : \'' || AS_VARCHAR(src:plai) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plma), '0'), 38, 10) is null and 
                    src:plma is not null) THEN
                    'plma contains a non-numeric value : \'' || AS_VARCHAR(src:plma) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plnp), '0'), 38, 10) is null and 
                    src:plnp is not null) THEN
                    'plnp contains a non-numeric value : \'' || AS_VARCHAR(src:plnp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppeg), '0'), 38, 10) is null and 
                    src:ppeg is not null) THEN
                    'ppeg contains a non-numeric value : \'' || AS_VARCHAR(src:ppeg) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prci), '0'), 38, 10) is null and 
                    src:prci is not null) THEN
                    'prci contains a non-numeric value : \'' || AS_VARCHAR(src:prci) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prji), '0'), 38, 10) is null and 
                    src:prji is not null) THEN
                    'prji contains a non-numeric value : \'' || AS_VARCHAR(src:prji) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psch), '0'), 38, 10) is null and 
                    src:psch is not null) THEN
                    'psch contains a non-numeric value : \'' || AS_VARCHAR(src:psch) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptri), '0'), 38, 10) is null and 
                    src:ptri is not null) THEN
                    'ptri contains a non-numeric value : \'' || AS_VARCHAR(src:ptri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qumi), '0'), 38, 10) is null and 
                    src:qumi is not null) THEN
                    'qumi contains a non-numeric value : \'' || AS_VARCHAR(src:qumi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdes), '0'), 38, 10) is null and 
                    src:rdes is not null) THEN
                    'rdes contains a non-numeric value : \'' || AS_VARCHAR(src:rdes) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpti), '0'), 38, 10) is null and 
                    src:rpti is not null) THEN
                    'rpti contains a non-numeric value : \'' || AS_VARCHAR(src:rpti) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsbs), '0'), 38, 10) is null and 
                    src:rsbs is not null) THEN
                    'rsbs contains a non-numeric value : \'' || AS_VARCHAR(src:rsbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scbs), '0'), 38, 10) is null and 
                    src:scbs is not null) THEN
                    'scbs contains a non-numeric value : \'' || AS_VARCHAR(src:scbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schi), '0'), 38, 10) is null and 
                    src:schi is not null) THEN
                    'schi contains a non-numeric value : \'' || AS_VARCHAR(src:schi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:simd), '0'), 38, 10) is null and 
                    src:simd is not null) THEN
                    'simd contains a non-numeric value : \'' || AS_VARCHAR(src:simd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slbp), '0'), 38, 10) is null and 
                    src:slbp is not null) THEN
                    'slbp contains a non-numeric value : \'' || AS_VARCHAR(src:slbp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slbs), '0'), 38, 10) is null and 
                    src:slbs is not null) THEN
                    'slbs contains a non-numeric value : \'' || AS_VARCHAR(src:slbs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfm), '0'), 38, 10) is null and 
                    src:smfm is not null) THEN
                    'smfm contains a non-numeric value : \'' || AS_VARCHAR(src:smfm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfs), '0'), 38, 10) is null and 
                    src:smfs is not null) THEN
                    'smfs contains a non-numeric value : \'' || AS_VARCHAR(src:smfs) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smii), '0'), 38, 10) is null and 
                    src:smii is not null) THEN
                    'smii contains a non-numeric value : \'' || AS_VARCHAR(src:smii) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spri), '0'), 38, 10) is null and 
                    src:spri is not null) THEN
                    'spri contains a non-numeric value : \'' || AS_VARCHAR(src:spri) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsp), '0'), 38, 10) is null and 
                    src:spsp is not null) THEN
                    'spsp contains a non-numeric value : \'' || AS_VARCHAR(src:spsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srvi), '0'), 38, 10) is null and 
                    src:srvi is not null) THEN
                    'srvi contains a non-numeric value : \'' || AS_VARCHAR(src:srvi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssmf), '0'), 38, 10) is null and 
                    src:ssmf is not null) THEN
                    'ssmf contains a non-numeric value : \'' || AS_VARCHAR(src:ssmf) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) THEN
                    'stat contains a non-numeric value : \'' || AS_VARCHAR(src:stat) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbi), '0'), 38, 10) is null and 
                    src:stbi is not null) THEN
                    'stbi contains a non-numeric value : \'' || AS_VARCHAR(src:stbi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tati), '0'), 38, 10) is null and 
                    src:tati is not null) THEN
                    'tati contains a non-numeric value : \'' || AS_VARCHAR(src:tati) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxd), '0'), 38, 10) is null and 
                    src:taxd is not null) THEN
                    'taxd contains a non-numeric value : \'' || AS_VARCHAR(src:taxd) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlgi), '0'), 38, 10) is null and 
                    src:tlgi is not null) THEN
                    'tlgi contains a non-numeric value : \'' || AS_VARCHAR(src:tlgi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trcn), '0'), 38, 10) is null and 
                    src:trcn is not null) THEN
                    'trcn contains a non-numeric value : \'' || AS_VARCHAR(src:trcn) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucpu), '0'), 38, 10) is null and 
                    src:ucpu is not null) THEN
                    'ucpu contains a non-numeric value : \'' || AS_VARCHAR(src:ucpu) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucsl), '0'), 38, 10) is null and 
                    src:ucsl is not null) THEN
                    'ucsl contains a non-numeric value : \'' || AS_VARCHAR(src:ucsl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unef), '0'), 38, 10) is null and 
                    src:unef is not null) THEN
                    'unef contains a non-numeric value : \'' || AS_VARCHAR(src:unef) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usup), '0'), 38, 10) is null and 
                    src:usup is not null) THEN
                    'usup contains a non-numeric value : \'' || AS_VARCHAR(src:usup) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwht), '0'), 38, 10) is null and 
                    src:uwht is not null) THEN
                    'uwht contains a non-numeric value : \'' || AS_VARCHAR(src:uwht) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vmic), '0'), 38, 10) is null and 
                    src:vmic is not null) THEN
                    'vmic contains a non-numeric value : \'' || AS_VARCHAR(src:vmic) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vmis), '0'), 38, 10) is null and 
                    src:vmis is not null) THEN
                    'vmis contains a non-numeric value : \'' || AS_VARCHAR(src:vmis) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wmbi), '0'), 38, 10) is null and 
                    src:wmbi is not null) THEN
                    'wmbi contains a non-numeric value : \'' || AS_VARCHAR(src:wmbi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrhi), '0'), 38, 10) is null and 
                    src:wrhi is not null) THEN
                    'wrhi contains a non-numeric value : \'' || AS_VARCHAR(src:wrhi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xtmm), '0'), 38, 10) is null and 
                    src:xtmm is not null) THEN
                    'xtmm contains a non-numeric value : \'' || AS_VARCHAR(src:xtmm) || '\'' WHEN 
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
                                    
                src:indt,
                src:ncmp,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TCCOM000 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:acfg), '0'), 38, 10) is null and 
                    src:acfg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:adcc), '0'), 38, 10) is null and 
                    src:adcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:alhp), '0'), 38, 10) is null and 
                    src:alhp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:altm), '0'), 38, 10) is null and 
                    src:altm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:arcc), '0'), 38, 10) is null and 
                    src:arcc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ardc), '0'), 38, 10) is null and 
                    src:ardc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asci), '0'), 38, 10) is null and 
                    src:asci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:asec), '0'), 38, 10) is null and 
                    src:asec is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bape), '0'), 38, 10) is null and 
                    src:bape is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bcmi), '0'), 38, 10) is null and 
                    src:bcmi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bgci), '0'), 38, 10) is null and 
                    src:bgci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:bulg), '0'), 38, 10) is null and 
                    src:bulg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cadr_ref_compnr), '0'), 38, 10) is null and 
                    src:cadr_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:capp), '0'), 38, 10) is null and 
                    src:capp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbrk), '0'), 38, 10) is null and 
                    src:cbrk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ccty_ref_compnr), '0'), 38, 10) is null and 
                    src:ccty_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfbs), '0'), 38, 10) is null and 
                    src:cfbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfri), '0'), 38, 10) is null and 
                    src:cfri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cinv), '0'), 38, 10) is null and 
                    src:cinv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clan_ref_compnr), '0'), 38, 10) is null and 
                    src:clan_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:clbi), '0'), 38, 10) is null and 
                    src:clbi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:corg), '0'), 38, 10) is null and 
                    src:corg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cpli), '0'), 38, 10) is null and 
                    src:cpli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:csli), '0'), 38, 10) is null and 
                    src:csli is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:darc), '0'), 38, 10) is null and 
                    src:darc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dasr), '0'), 38, 10) is null and 
                    src:dasr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:deln), '0'), 38, 10) is null and 
                    src:deln is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:dsri), '0'), 38, 10) is null and 
                    src:dsri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:duns), '0'), 38, 10) is null and 
                    src:duns is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eaas), '0'), 38, 10) is null and 
                    src:eaas is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ecmi), '0'), 38, 10) is null and 
                    src:ecmi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:erac), '0'), 38, 10) is null and 
                    src:erac is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eupl), '0'), 38, 10) is null and 
                    src:eupl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eusl), '0'), 38, 10) is null and 
                    src:eusl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:eust), '0'), 38, 10) is null and 
                    src:eust is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ffpl), '0'), 38, 10) is null and 
                    src:ffpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fini), '0'), 38, 10) is null and 
                    src:fini is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:frmi), '0'), 38, 10) is null and 
                    src:frmi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:fstk), '0'), 38, 10) is null and 
                    src:fstk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:grpl), '0'), 38, 10) is null and 
                    src:grpl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gtci), '0'), 38, 10) is null and 
                    src:gtci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gtro), '0'), 38, 10) is null and 
                    src:gtro is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:hcst), '0'), 38, 10) is null and 
                    src:hcst is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ilwh), '0'), 38, 10) is null and 
                    src:ilwh is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:indt), '1900-01-01')) is null and 
                    src:indt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:inpr), '0'), 38, 10) is null and 
                    src:inpr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:intr), '0'), 38, 10) is null and 
                    src:intr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itpd), '0'), 38, 10) is null and 
                    src:itpd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:itri), '0'), 38, 10) is null and 
                    src:itri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:jspm), '0'), 38, 10) is null and 
                    src:jspm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lago), '0'), 38, 10) is null and 
                    src:lago is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:larg), '0'), 38, 10) is null and 
                    src:larg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lbra), '0'), 38, 10) is null and 
                    src:lbra is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lche), '0'), 38, 10) is null and 
                    src:lche is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lchl), '0'), 38, 10) is null and 
                    src:lchl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lchn), '0'), 38, 10) is null and 
                    src:lchn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcol), '0'), 38, 10) is null and 
                    src:lcol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcsi), '0'), 38, 10) is null and 
                    src:lcsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lcze), '0'), 38, 10) is null and 
                    src:lcze is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ldeu), '0'), 38, 10) is null and 
                    src:ldeu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lesp), '0'), 38, 10) is null and 
                    src:lesp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lgbr), '0'), 38, 10) is null and 
                    src:lgbr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lhrv), '0'), 38, 10) is null and 
                    src:lhrv is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lhun), '0'), 38, 10) is null and 
                    src:lhun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lidn), '0'), 38, 10) is null and 
                    src:lidn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lind), '0'), 38, 10) is null and 
                    src:lind is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lisr), '0'), 38, 10) is null and 
                    src:lisr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lita), '0'), 38, 10) is null and 
                    src:lita is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmex), '0'), 38, 10) is null and 
                    src:lmex is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lmys), '0'), 38, 10) is null and 
                    src:lmys is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnor), '0'), 38, 10) is null and 
                    src:lnor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lnwi), '0'), 38, 10) is null and 
                    src:lnwi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lper), '0'), 38, 10) is null and 
                    src:lper is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lphl), '0'), 38, 10) is null and 
                    src:lphl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpol), '0'), 38, 10) is null and 
                    src:lpol is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lpor), '0'), 38, 10) is null and 
                    src:lpor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lrom), '0'), 38, 10) is null and 
                    src:lrom is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lrus), '0'), 38, 10) is null and 
                    src:lrus is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsau), '0'), 38, 10) is null and 
                    src:lsau is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsrb), '0'), 38, 10) is null and 
                    src:lsrb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsvk), '0'), 38, 10) is null and 
                    src:lsvk is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lsvn), '0'), 38, 10) is null and 
                    src:lsvn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltha), '0'), 38, 10) is null and 
                    src:ltha is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ltur), '0'), 38, 10) is null and 
                    src:ltur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:lvnm), '0'), 38, 10) is null and 
                    src:lvnm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mejs), '0'), 38, 10) is null and 
                    src:mejs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mesa), '0'), 38, 10) is null and 
                    src:mesa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mfsi), '0'), 38, 10) is null and 
                    src:mfsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mnfc), '0'), 38, 10) is null and 
                    src:mnfc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mpni), '0'), 38, 10) is null and 
                    src:mpni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mprs), '0'), 38, 10) is null and 
                    src:mprs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mstc), '0'), 38, 10) is null and 
                    src:mstc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ncmp), '0'), 38, 10) is null and 
                    src:ncmp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:odmu), '0'), 38, 10) is null and 
                    src:odmu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:opsi), '0'), 38, 10) is null and 
                    src:opsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owne), '0'), 38, 10) is null and 
                    src:owne is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:owni), '0'), 38, 10) is null and 
                    src:owni is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcfi), '0'), 38, 10) is null and 
                    src:pcfi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcmd), '0'), 38, 10) is null and 
                    src:pcmd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pcsi), '0'), 38, 10) is null and 
                    src:pcsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pdim), '0'), 38, 10) is null and 
                    src:pdim is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:pedi), '0'), 38, 10) is null and 
                    src:pedi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plai), '0'), 38, 10) is null and 
                    src:plai is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plma), '0'), 38, 10) is null and 
                    src:plma is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:plnp), '0'), 38, 10) is null and 
                    src:plnp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ppeg), '0'), 38, 10) is null and 
                    src:ppeg is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prci), '0'), 38, 10) is null and 
                    src:prci is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:prji), '0'), 38, 10) is null and 
                    src:prji is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:psch), '0'), 38, 10) is null and 
                    src:psch is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ptri), '0'), 38, 10) is null and 
                    src:ptri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:qumi), '0'), 38, 10) is null and 
                    src:qumi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rdes), '0'), 38, 10) is null and 
                    src:rdes is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rpti), '0'), 38, 10) is null and 
                    src:rpti is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:rsbs), '0'), 38, 10) is null and 
                    src:rsbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scbs), '0'), 38, 10) is null and 
                    src:scbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:schi), '0'), 38, 10) is null and 
                    src:schi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:simd), '0'), 38, 10) is null and 
                    src:simd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slbp), '0'), 38, 10) is null and 
                    src:slbp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slbs), '0'), 38, 10) is null and 
                    src:slbs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfm), '0'), 38, 10) is null and 
                    src:smfm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smfs), '0'), 38, 10) is null and 
                    src:smfs is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:smii), '0'), 38, 10) is null and 
                    src:smii is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spri), '0'), 38, 10) is null and 
                    src:spri is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spsp), '0'), 38, 10) is null and 
                    src:spsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:srvi), '0'), 38, 10) is null and 
                    src:srvi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssmf), '0'), 38, 10) is null and 
                    src:ssmf is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stat), '0'), 38, 10) is null and 
                    src:stat is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:stbi), '0'), 38, 10) is null and 
                    src:stbi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tati), '0'), 38, 10) is null and 
                    src:tati is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:taxd), '0'), 38, 10) is null and 
                    src:taxd is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:tlgi), '0'), 38, 10) is null and 
                    src:tlgi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:trcn), '0'), 38, 10) is null and 
                    src:trcn is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucpu), '0'), 38, 10) is null and 
                    src:ucpu is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ucsl), '0'), 38, 10) is null and 
                    src:ucsl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:unef), '0'), 38, 10) is null and 
                    src:unef is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:usup), '0'), 38, 10) is null and 
                    src:usup is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:uwht), '0'), 38, 10) is null and 
                    src:uwht is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vmic), '0'), 38, 10) is null and 
                    src:vmic is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:vmis), '0'), 38, 10) is null and 
                    src:vmis is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wmbi), '0'), 38, 10) is null and 
                    src:wmbi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wrhi), '0'), 38, 10) is null and 
                    src:wrhi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:xtmm), '0'), 38, 10) is null and 
                    src:xtmm is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)