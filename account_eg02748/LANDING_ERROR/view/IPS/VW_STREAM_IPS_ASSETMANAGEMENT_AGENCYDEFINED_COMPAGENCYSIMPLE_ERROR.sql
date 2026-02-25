CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_AGENCYDEFINED_COMPAGENCYSIMPLE_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_AGENCYDEFINED_COMPAGENCYSIMPLE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGENCYDATE1), '1900-01-01')) is null and 
                    src:AGENCYDATE1 is not null) THEN
                    'AGENCYDATE1 contains a non-timestamp value : \'' || AS_VARCHAR(src:AGENCYDATE1) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGENCYDATE2), '1900-01-01')) is null and 
                    src:AGENCYDATE2 is not null) THEN
                    'AGENCYDATE2 contains a non-timestamp value : \'' || AS_VARCHAR(src:AGENCYDATE2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGENCYINTEGER1), '0'), 38, 10) is null and 
                    src:AGENCYINTEGER1 is not null) THEN
                    'AGENCYINTEGER1 contains a non-numeric value : \'' || AS_VARCHAR(src:AGENCYINTEGER1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGENCYINTEGER2), '0'), 38, 10) is null and 
                    src:AGENCYINTEGER2 is not null) THEN
                    'AGENCYINTEGER2 contains a non-numeric value : \'' || AS_VARCHAR(src:AGENCYINTEGER2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVGMONUSG), '0'), 38, 10) is null and 
                    src:AVGMONUSG is not null) THEN
                    'AVGMONUSG contains a non-numeric value : \'' || AS_VARCHAR(src:AVGMONUSG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) THEN
                    'BUDGETKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BUDGETKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) THEN
                    'COMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) THEN
                    'COMPTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIAM), '0'), 38, 10) is null and 
                    src:DIAM is not null) THEN
                    'DIAM contains a non-numeric value : \'' || AS_VARCHAR(src:DIAM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DPTH), '0'), 38, 10) is null and 
                    src:DPTH is not null) THEN
                    'DPTH contains a non-numeric value : \'' || AS_VARCHAR(src:DPTH) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ELEV), '0'), 38, 10) is null and 
                    src:ELEV is not null) THEN
                    'ELEV contains a non-numeric value : \'' || AS_VARCHAR(src:ELEV) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPECTEDLIFE), '0'), 38, 10) is null and 
                    src:EXPECTEDLIFE is not null) THEN
                    'EXPECTEDLIFE contains a non-numeric value : \'' || AS_VARCHAR(src:EXPECTEDLIFE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is null and 
                    src:GISSTATIC is not null) THEN
                    'GISSTATIC contains a non-numeric value : \'' || AS_VARCHAR(src:GISSTATIC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HT), '0'), 38, 10) is null and 
                    src:HT is not null) THEN
                    'HT contains a non-numeric value : \'' || AS_VARCHAR(src:HT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is null and 
                    src:INSTDATE is not null) THEN
                    'INSTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:INSTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LEN), '0'), 38, 10) is null and 
                    src:LEN is not null) THEN
                    'LEN contains a non-numeric value : \'' || AS_VARCHAR(src:LEN) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is null and 
                    src:MFGDATE is not null) THEN
                    'MFGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:MFGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is null and 
                    src:MFGKEY is not null) THEN
                    'MFGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MFGKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PURCCOST), '0'), 38, 10) is null and 
                    src:PURCCOST is not null) THEN
                    'PURCCOST contains a non-numeric value : \'' || AS_VARCHAR(src:PURCCOST) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is null and 
                    src:PURCDATE is not null) THEN
                    'PURCDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:PURCDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBUNITOF), '0'), 38, 10) is null and 
                    src:SUBUNITOF is not null) THEN
                    'SUBUNITOF contains a non-numeric value : \'' || AS_VARCHAR(src:SUBUNITOF) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USGDATE), '1900-01-01')) is null and 
                    src:USGDATE is not null) THEN
                    'USGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:USGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is null and 
                    src:USGTOT is not null) THEN
                    'USGTOT contains a non-numeric value : \'' || AS_VARCHAR(src:USGTOT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WID), '0'), 38, 10) is null and 
                    src:WID is not null) THEN
                    'WID contains a non-numeric value : \'' || AS_VARCHAR(src:WID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WT), '0'), 38, 10) is null and 
                    src:WT is not null) THEN
                    'WT contains a non-numeric value : \'' || AS_VARCHAR(src:WT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is null and 
                    src:XCOORD is not null) THEN
                    'XCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:XCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is null and 
                    src:YCOORD is not null) THEN
                    'YCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:YCOORD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is null and 
                    src:ZCOORD is not null) THEN
                    'ZCOORD contains a non-numeric value : \'' || AS_VARCHAR(src:ZCOORD) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:COMPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_AGENCYDEFINED_COMPAGENCYSIMPLE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGENCYDATE1), '1900-01-01')) is null and 
                    src:AGENCYDATE1 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:AGENCYDATE2), '1900-01-01')) is null and 
                    src:AGENCYDATE2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGENCYINTEGER1), '0'), 38, 10) is null and 
                    src:AGENCYINTEGER1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AGENCYINTEGER2), '0'), 38, 10) is null and 
                    src:AGENCYINTEGER2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AVGMONUSG), '0'), 38, 10) is null and 
                    src:AVGMONUSG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is null and 
                    src:BUDGETKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is null and 
                    src:COMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is null and 
                    src:COMPTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DIAM), '0'), 38, 10) is null and 
                    src:DIAM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DPTH), '0'), 38, 10) is null and 
                    src:DPTH is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ELEV), '0'), 38, 10) is null and 
                    src:ELEV is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXPECTEDLIFE), '0'), 38, 10) is null and 
                    src:EXPECTEDLIFE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is null and 
                    src:GISSTATIC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HT), '0'), 38, 10) is null and 
                    src:HT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is null and 
                    src:INSTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LEN), '0'), 38, 10) is null and 
                    src:LEN is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is null and 
                    src:MFGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is null and 
                    src:MFGKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PURCCOST), '0'), 38, 10) is null and 
                    src:PURCCOST is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is null and 
                    src:PURCDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SUBUNITOF), '0'), 38, 10) is null and 
                    src:SUBUNITOF is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:USGDATE), '1900-01-01')) is null and 
                    src:USGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is null and 
                    src:USGTOT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WID), '0'), 38, 10) is null and 
                    src:WID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WT), '0'), 38, 10) is null and 
                    src:WT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is null and 
                    src:XCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is null and 
                    src:YCOORD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is null and 
                    src:ZCOORD is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)