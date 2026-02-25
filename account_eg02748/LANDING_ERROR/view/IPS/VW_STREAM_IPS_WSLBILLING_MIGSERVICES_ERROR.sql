CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_WSLBILLING_MIGSERVICES_ERROR AS SELECT src, 'IPS_WSLBILLING_MIGSERVICES' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) THEN
                    'ACCOUNTSERVICEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ACCOUNTSERVICEKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY1), '0'), 38, 10) is null and 
                    src:ADDRESSKEY1 is not null) THEN
                    'ADDRESSKEY1 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY2), '0'), 38, 10) is null and 
                    src:ADDRESSKEY2 is not null) THEN
                    'ADDRESSKEY2 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY3), '0'), 38, 10) is null and 
                    src:ADDRESSKEY3 is not null) THEN
                    'ADDRESSKEY3 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY4), '0'), 38, 10) is null and 
                    src:ADDRESSKEY4 is not null) THEN
                    'ADDRESSKEY4 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY5), '0'), 38, 10) is null and 
                    src:ADDRESSKEY5 is not null) THEN
                    'ADDRESSKEY5 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY6), '0'), 38, 10) is null and 
                    src:ADDRESSKEY6 is not null) THEN
                    'ADDRESSKEY6 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY7), '0'), 38, 10) is null and 
                    src:ADDRESSKEY7 is not null) THEN
                    'ADDRESSKEY7 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ANNUALFIX), '0'), 38, 10) is null and 
                    src:ANNUALFIX is not null) THEN
                    'ANNUALFIX contains a non-numeric value : \'' || AS_VARCHAR(src:ANNUALFIX) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC), '0'), 38, 10) is null and 
                    src:BILLABLEPERC is not null) THEN
                    'BILLABLEPERC contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC1), '0'), 38, 10) is null and 
                    src:BILLABLEPERC1 is not null) THEN
                    'BILLABLEPERC1 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC2), '0'), 38, 10) is null and 
                    src:BILLABLEPERC2 is not null) THEN
                    'BILLABLEPERC2 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC3), '0'), 38, 10) is null and 
                    src:BILLABLEPERC3 is not null) THEN
                    'BILLABLEPERC3 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC4), '0'), 38, 10) is null and 
                    src:BILLABLEPERC4 is not null) THEN
                    'BILLABLEPERC4 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC5), '0'), 38, 10) is null and 
                    src:BILLABLEPERC5 is not null) THEN
                    'BILLABLEPERC5 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC6), '0'), 38, 10) is null and 
                    src:BILLABLEPERC6 is not null) THEN
                    'BILLABLEPERC6 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC7), '0'), 38, 10) is null and 
                    src:BILLABLEPERC7 is not null) THEN
                    'BILLABLEPERC7 contains a non-numeric value : \'' || AS_VARCHAR(src:BILLABLEPERC7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MIGSERVICESKEY), '0'), 38, 10) is null and 
                    src:MIGSERVICESKEY is not null) THEN
                    'MIGSERVICESKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MIGSERVICESKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMMETERS), '0'), 38, 10) is null and 
                    src:NUMMETERS is not null) THEN
                    'NUMMETERS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMMETERS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION1), '0'), 38, 10) is null and 
                    src:POSITION1 is not null) THEN
                    'POSITION1 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION2), '0'), 38, 10) is null and 
                    src:POSITION2 is not null) THEN
                    'POSITION2 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION3), '0'), 38, 10) is null and 
                    src:POSITION3 is not null) THEN
                    'POSITION3 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION4), '0'), 38, 10) is null and 
                    src:POSITION4 is not null) THEN
                    'POSITION4 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION5), '0'), 38, 10) is null and 
                    src:POSITION5 is not null) THEN
                    'POSITION5 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION6), '0'), 38, 10) is null and 
                    src:POSITION6 is not null) THEN
                    'POSITION6 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION7), '0'), 38, 10) is null and 
                    src:POSITION7 is not null) THEN
                    'POSITION7 contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION7) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE1), '1900-01-01')) is null and 
                    src:READDATE1 is not null) THEN
                    'READDATE1 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE1) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE2), '1900-01-01')) is null and 
                    src:READDATE2 is not null) THEN
                    'READDATE2 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE2) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE3), '1900-01-01')) is null and 
                    src:READDATE3 is not null) THEN
                    'READDATE3 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE3) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE4), '1900-01-01')) is null and 
                    src:READDATE4 is not null) THEN
                    'READDATE4 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE4) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE5), '1900-01-01')) is null and 
                    src:READDATE5 is not null) THEN
                    'READDATE5 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE5) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE6), '1900-01-01')) is null and 
                    src:READDATE6 is not null) THEN
                    'READDATE6 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE6) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE7), '1900-01-01')) is null and 
                    src:READDATE7 is not null) THEN
                    'READDATE7 contains a non-timestamp value : \'' || AS_VARCHAR(src:READDATE7) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDATE), '1900-01-01')) is null and 
                    src:STARTDATE is not null) THEN
                    'STARTDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:STARTDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY1), '0'), 38, 10) is null and 
                    src:STARTKEY1 is not null) THEN
                    'STARTKEY1 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY2), '0'), 38, 10) is null and 
                    src:STARTKEY2 is not null) THEN
                    'STARTKEY2 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY2) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY3), '0'), 38, 10) is null and 
                    src:STARTKEY3 is not null) THEN
                    'STARTKEY3 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY3) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY4), '0'), 38, 10) is null and 
                    src:STARTKEY4 is not null) THEN
                    'STARTKEY4 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY4) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY5), '0'), 38, 10) is null and 
                    src:STARTKEY5 is not null) THEN
                    'STARTKEY5 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY5) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY6), '0'), 38, 10) is null and 
                    src:STARTKEY6 is not null) THEN
                    'STARTKEY6 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY6) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY7), '0'), 38, 10) is null and 
                    src:STARTKEY7 is not null) THEN
                    'STARTKEY7 contains a non-numeric value : \'' || AS_VARCHAR(src:STARTKEY7) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
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
                                    
                src:MIGSERVICESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLBILLING_MIGSERVICES as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is null and 
                    src:ACCOUNTSERVICEKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY1), '0'), 38, 10) is null and 
                    src:ADDRESSKEY1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY2), '0'), 38, 10) is null and 
                    src:ADDRESSKEY2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY3), '0'), 38, 10) is null and 
                    src:ADDRESSKEY3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY4), '0'), 38, 10) is null and 
                    src:ADDRESSKEY4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY5), '0'), 38, 10) is null and 
                    src:ADDRESSKEY5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY6), '0'), 38, 10) is null and 
                    src:ADDRESSKEY6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY7), '0'), 38, 10) is null and 
                    src:ADDRESSKEY7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ANNUALFIX), '0'), 38, 10) is null and 
                    src:ANNUALFIX is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC), '0'), 38, 10) is null and 
                    src:BILLABLEPERC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC1), '0'), 38, 10) is null and 
                    src:BILLABLEPERC1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC2), '0'), 38, 10) is null and 
                    src:BILLABLEPERC2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC3), '0'), 38, 10) is null and 
                    src:BILLABLEPERC3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC4), '0'), 38, 10) is null and 
                    src:BILLABLEPERC4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC5), '0'), 38, 10) is null and 
                    src:BILLABLEPERC5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC6), '0'), 38, 10) is null and 
                    src:BILLABLEPERC6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLABLEPERC7), '0'), 38, 10) is null and 
                    src:BILLABLEPERC7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MIGSERVICESKEY), '0'), 38, 10) is null and 
                    src:MIGSERVICESKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMMETERS), '0'), 38, 10) is null and 
                    src:NUMMETERS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION1), '0'), 38, 10) is null and 
                    src:POSITION1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION2), '0'), 38, 10) is null and 
                    src:POSITION2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION3), '0'), 38, 10) is null and 
                    src:POSITION3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION4), '0'), 38, 10) is null and 
                    src:POSITION4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION5), '0'), 38, 10) is null and 
                    src:POSITION5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION6), '0'), 38, 10) is null and 
                    src:POSITION6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION7), '0'), 38, 10) is null and 
                    src:POSITION7 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE1), '1900-01-01')) is null and 
                    src:READDATE1 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE2), '1900-01-01')) is null and 
                    src:READDATE2 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE3), '1900-01-01')) is null and 
                    src:READDATE3 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE4), '1900-01-01')) is null and 
                    src:READDATE4 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE5), '1900-01-01')) is null and 
                    src:READDATE5 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE6), '1900-01-01')) is null and 
                    src:READDATE6 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:READDATE7), '1900-01-01')) is null and 
                    src:READDATE7 is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:STARTDATE), '1900-01-01')) is null and 
                    src:STARTDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY1), '0'), 38, 10) is null and 
                    src:STARTKEY1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY2), '0'), 38, 10) is null and 
                    src:STARTKEY2 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY3), '0'), 38, 10) is null and 
                    src:STARTKEY3 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY4), '0'), 38, 10) is null and 
                    src:STARTKEY4 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY5), '0'), 38, 10) is null and 
                    src:STARTKEY5 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY6), '0'), 38, 10) is null and 
                    src:STARTKEY6 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:STARTKEY7), '0'), 38, 10) is null and 
                    src:STARTKEY7 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)