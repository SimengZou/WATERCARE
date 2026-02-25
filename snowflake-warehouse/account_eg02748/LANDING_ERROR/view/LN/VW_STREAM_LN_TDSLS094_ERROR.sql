CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDSLS094_ERROR AS SELECT src, 'LN_TDSLS094' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaps), '0'), 38, 10) is null and 
                    src:aaps is not null) THEN
                    'aaps contains a non-numeric value : \'' || AS_VARCHAR(src:aaps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprd), '0'), 38, 10) is null and 
                    src:aprd is not null) THEN
                    'aprd contains a non-numeric value : \'' || AS_VARCHAR(src:aprd) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apur), '0'), 38, 10) is null and 
                    src:apur is not null) THEN
                    'apur contains a non-numeric value : \'' || AS_VARCHAR(src:apur) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsi), '0'), 38, 10) is null and 
                    src:cnsi is not null) THEN
                    'cnsi contains a non-numeric value : \'' || AS_VARCHAR(src:cnsi) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsr), '0'), 38, 10) is null and 
                    src:cnsr is not null) THEN
                    'cnsr contains a non-numeric value : \'' || AS_VARCHAR(src:cnsr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coun), '0'), 38, 10) is null and 
                    src:coun is not null) THEN
                    'coun contains a non-numeric value : \'' || AS_VARCHAR(src:coun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cphl), '0'), 38, 10) is null and 
                    src:cphl is not null) THEN
                    'cphl contains a non-numeric value : \'' || AS_VARCHAR(src:cphl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crdc), '0'), 38, 10) is null and 
                    src:crdc is not null) THEN
                    'crdc contains a non-numeric value : \'' || AS_VARCHAR(src:crdc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:einc), '0'), 38, 10) is null and 
                    src:einc is not null) THEN
                    'einc contains a non-numeric value : \'' || AS_VARCHAR(src:einc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gmrc), '0'), 38, 10) is null and 
                    src:gmrc is not null) THEN
                    'gmrc contains a non-numeric value : \'' || AS_VARCHAR(src:gmrc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incm), '0'), 38, 10) is null and 
                    src:incm is not null) THEN
                    'incm contains a non-numeric value : \'' || AS_VARCHAR(src:incm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mwdl), '0'), 38, 10) is null and 
                    src:mwdl is not null) THEN
                    'mwdl contains a non-numeric value : \'' || AS_VARCHAR(src:mwdl) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsc_ref_compnr is not null) THEN
                    'ngsc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsc_sesc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsc_sesc_ref_compnr is not null) THEN
                    'ngsc_sesc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsc_sesc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngso_ref_compnr), '0'), 38, 10) is null and 
                    src:ngso_ref_compnr is not null) THEN
                    'ngso_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngso_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngso_seso_ref_compnr), '0'), 38, 10) is null and 
                    src:ngso_seso_ref_compnr is not null) THEN
                    'ngso_seso_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngso_seso_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsq_ref_compnr is not null) THEN
                    'ngsq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsq_sesq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsq_sesq_ref_compnr is not null) THEN
                    'ngsq_sesq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngsq_sesq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retb), '0'), 38, 10) is null and 
                    src:retb is not null) THEN
                    'retb contains a non-numeric value : \'' || AS_VARCHAR(src:retb) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reto), '0'), 38, 10) is null and 
                    src:reto is not null) THEN
                    'reto contains a non-numeric value : \'' || AS_VARCHAR(src:reto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) THEN
                    'scon contains a non-numeric value : \'' || AS_VARCHAR(src:scon) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdec_ref_compnr), '0'), 38, 10) is null and 
                    src:sdec_ref_compnr is not null) THEN
                    'sdec_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:sdec_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spay_ref_compnr), '0'), 38, 10) is null and 
                    src:spay_ref_compnr is not null) THEN
                    'spay_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:spay_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssoa), '0'), 38, 10) is null and 
                    src:ssoa is not null) THEN
                    'ssoa contains a non-numeric value : \'' || AS_VARCHAR(src:ssoa) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssob), '0'), 38, 10) is null and 
                    src:ssob is not null) THEN
                    'ssob contains a non-numeric value : \'' || AS_VARCHAR(src:ssob) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssoc), '0'), 38, 10) is null and 
                    src:ssoc is not null) THEN
                    'ssoc contains a non-numeric value : \'' || AS_VARCHAR(src:ssoc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssod), '0'), 38, 10) is null and 
                    src:ssod is not null) THEN
                    'ssod contains a non-numeric value : \'' || AS_VARCHAR(src:ssod) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssoe), '0'), 38, 10) is null and 
                    src:ssoe is not null) THEN
                    'ssoe contains a non-numeric value : \'' || AS_VARCHAR(src:ssoe) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssof), '0'), 38, 10) is null and 
                    src:ssof is not null) THEN
                    'ssof contains a non-numeric value : \'' || AS_VARCHAR(src:ssof) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sund), '0'), 38, 10) is null and 
                    src:sund is not null) THEN
                    'sund contains a non-numeric value : \'' || AS_VARCHAR(src:sund) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udps), '0'), 38, 10) is null and 
                    src:udps is not null) THEN
                    'udps contains a non-numeric value : \'' || AS_VARCHAR(src:udps) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upls), '0'), 38, 10) is null and 
                    src:upls is not null) THEN
                    'upls contains a non-numeric value : \'' || AS_VARCHAR(src:upls) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upsh), '0'), 38, 10) is null and 
                    src:upsh is not null) THEN
                    'upsh contains a non-numeric value : \'' || AS_VARCHAR(src:upsh) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wpal), '0'), 38, 10) is null and 
                    src:wpal is not null) THEN
                    'wpal contains a non-numeric value : \'' || AS_VARCHAR(src:wpal) || '\'' WHEN 
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
                src:sotp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDSLS094 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aaps), '0'), 38, 10) is null and 
                    src:aaps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:aprd), '0'), 38, 10) is null and 
                    src:aprd is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:apur), '0'), 38, 10) is null and 
                    src:apur is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsi), '0'), 38, 10) is null and 
                    src:cnsi is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsr), '0'), 38, 10) is null and 
                    src:cnsr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coun), '0'), 38, 10) is null and 
                    src:coun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cphl), '0'), 38, 10) is null and 
                    src:cphl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:crdc), '0'), 38, 10) is null and 
                    src:crdc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:einc), '0'), 38, 10) is null and 
                    src:einc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:gmrc), '0'), 38, 10) is null and 
                    src:gmrc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:incm), '0'), 38, 10) is null and 
                    src:incm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:mwdl), '0'), 38, 10) is null and 
                    src:mwdl is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsc_sesc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsc_sesc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngso_ref_compnr), '0'), 38, 10) is null and 
                    src:ngso_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngso_seso_ref_compnr), '0'), 38, 10) is null and 
                    src:ngso_seso_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngsq_sesq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngsq_sesq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:retb), '0'), 38, 10) is null and 
                    src:retb is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reto), '0'), 38, 10) is null and 
                    src:reto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:scon), '0'), 38, 10) is null and 
                    src:scon is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sdec_ref_compnr), '0'), 38, 10) is null and 
                    src:sdec_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:spay_ref_compnr), '0'), 38, 10) is null and 
                    src:spay_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssoa), '0'), 38, 10) is null and 
                    src:ssoa is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssob), '0'), 38, 10) is null and 
                    src:ssob is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssoc), '0'), 38, 10) is null and 
                    src:ssoc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssod), '0'), 38, 10) is null and 
                    src:ssod is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssoe), '0'), 38, 10) is null and 
                    src:ssoe is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ssof), '0'), 38, 10) is null and 
                    src:ssof is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sund), '0'), 38, 10) is null and 
                    src:sund is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:udps), '0'), 38, 10) is null and 
                    src:udps is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upls), '0'), 38, 10) is null and 
                    src:upls is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:upsh), '0'), 38, 10) is null and 
                    src:upsh is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:wpal), '0'), 38, 10) is null and 
                    src:wpal is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)