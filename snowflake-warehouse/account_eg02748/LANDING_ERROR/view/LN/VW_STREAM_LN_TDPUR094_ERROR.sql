CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_LN_TDPUR094_ERROR AS SELECT src, 'LN_TDPUR094' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbor), '0'), 38, 10) is null and 
                    src:cbor is not null) THEN
                    'cbor contains a non-numeric value : \'' || AS_VARCHAR(src:cbor) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfnm), '0'), 38, 10) is null and 
                    src:cfnm is not null) THEN
                    'cfnm contains a non-numeric value : \'' || AS_VARCHAR(src:cfnm) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsp), '0'), 38, 10) is null and 
                    src:cnsp is not null) THEN
                    'cnsp contains a non-numeric value : \'' || AS_VARCHAR(src:cnsp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsr), '0'), 38, 10) is null and 
                    src:cnsr is not null) THEN
                    'cnsr contains a non-numeric value : \'' || AS_VARCHAR(src:cnsr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) THEN
                    'compnr contains a non-numeric value : \'' || AS_VARCHAR(src:compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coun), '0'), 38, 10) is null and 
                    src:coun is not null) THEN
                    'coun contains a non-numeric value : \'' || AS_VARCHAR(src:coun) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drct), '0'), 38, 10) is null and 
                    src:drct is not null) THEN
                    'drct contains a non-numeric value : \'' || AS_VARCHAR(src:drct) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) THEN
                    'efdt contains a non-timestamp value : \'' || AS_VARCHAR(src:efdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) THEN
                    'etpc contains a non-numeric value : \'' || AS_VARCHAR(src:etpc) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) THEN
                    'exdt contains a non-timestamp value : \'' || AS_VARCHAR(src:exdt) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_ref_compnr is not null) THEN
                    'ngpc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_sepc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_sepc_ref_compnr is not null) THEN
                    'ngpc_sepc_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpc_sepc_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_ref_compnr is not null) THEN
                    'ngpo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_sepo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_sepo_ref_compnr is not null) THEN
                    'ngpo_sepo_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpo_sepo_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_ref_compnr is not null) THEN
                    'ngpq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_sepq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_sepq_ref_compnr is not null) THEN
                    'ngpq_sepq_ref_compnr contains a non-numeric value : \'' || AS_VARCHAR(src:ngpq_sepq_ref_compnr) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reto), '0'), 38, 10) is null and 
                    src:reto is not null) THEN
                    'reto contains a non-numeric value : \'' || AS_VARCHAR(src:reto) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) THEN
                    'sequencenumber contains a non-numeric value : \'' || AS_VARCHAR(src:sequencenumber) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slcp), '0'), 38, 10) is null and 
                    src:slcp is not null) THEN
                    'slcp contains a non-numeric value : \'' || AS_VARCHAR(src:slcp) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) THEN
                    'subc contains a non-numeric value : \'' || AS_VARCHAR(src:subc) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sund), '0'), 38, 10) is null and 
                    src:sund is not null) THEN
                    'sund contains a non-numeric value : \'' || AS_VARCHAR(src:sund) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) THEN
                    'timestamp contains a non-timestamp value : \'' || AS_VARCHAR(src:timestamp) || '\'' WHEN 
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
                src:potp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TDPUR094 as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cbor), '0'), 38, 10) is null and 
                    src:cbor is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cfnm), '0'), 38, 10) is null and 
                    src:cfnm is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsp), '0'), 38, 10) is null and 
                    src:cnsp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:cnsr), '0'), 38, 10) is null and 
                    src:cnsr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:compnr), '0'), 38, 10) is null and 
                    src:compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:coun), '0'), 38, 10) is null and 
                    src:coun is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:drct), '0'), 38, 10) is null and 
                    src:drct is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:efdt), '1900-01-01')) is null and 
                    src:efdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:etpc), '0'), 38, 10) is null and 
                    src:etpc is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:exdt), '1900-01-01')) is null and 
                    src:exdt is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpc_sepc_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpc_sepc_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpo_sepo_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpo_sepo_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ngpq_sepq_ref_compnr), '0'), 38, 10) is null and 
                    src:ngpq_sepq_ref_compnr is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:reto), '0'), 38, 10) is null and 
                    src:reto is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is null and 
                    src:sequencenumber is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:slcp), '0'), 38, 10) is null and 
                    src:slcp is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:subc), '0'), 38, 10) is null and 
                    src:subc is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:sund), '0'), 38, 10) is null and 
                    src:sund is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:timestamp), '1900-01-01')) is null and 
                    src:timestamp is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is null and 
                src:sequencenumber is not null)