CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_READINGREVIEWSETUP_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_READINGREVIEWSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSFORCURRD), '0'), 38, 10) is null and 
                    src:DAYSFORCURRD is not null) THEN
                    'DAYSFORCURRD contains a non-numeric value : \'' || AS_VARCHAR(src:DAYSFORCURRD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTBKG), '0'), 38, 10) is null and 
                    src:ESTBKG is not null) THEN
                    'ESTBKG contains a non-numeric value : \'' || AS_VARCHAR(src:ESTBKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTTXT), '0'), 38, 10) is null and 
                    src:ESTTXT is not null) THEN
                    'ESTTXT contains a non-numeric value : \'' || AS_VARCHAR(src:ESTTXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1BKG), '0'), 38, 10) is null and 
                    src:HI1BKG is not null) THEN
                    'HI1BKG contains a non-numeric value : \'' || AS_VARCHAR(src:HI1BKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1DEFACTION), '0'), 38, 10) is null and 
                    src:HI1DEFACTION is not null) THEN
                    'HI1DEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:HI1DEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1PCT), '0'), 38, 10) is null and 
                    src:HI1PCT is not null) THEN
                    'HI1PCT contains a non-numeric value : \'' || AS_VARCHAR(src:HI1PCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1PROCESS), '0'), 38, 10) is null and 
                    src:HI1PROCESS is not null) THEN
                    'HI1PROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:HI1PROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1TXT), '0'), 38, 10) is null and 
                    src:HI1TXT is not null) THEN
                    'HI1TXT contains a non-numeric value : \'' || AS_VARCHAR(src:HI1TXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2BKG), '0'), 38, 10) is null and 
                    src:HI2BKG is not null) THEN
                    'HI2BKG contains a non-numeric value : \'' || AS_VARCHAR(src:HI2BKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2DEFACTION), '0'), 38, 10) is null and 
                    src:HI2DEFACTION is not null) THEN
                    'HI2DEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:HI2DEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2PCT), '0'), 38, 10) is null and 
                    src:HI2PCT is not null) THEN
                    'HI2PCT contains a non-numeric value : \'' || AS_VARCHAR(src:HI2PCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2PROCESS), '0'), 38, 10) is null and 
                    src:HI2PROCESS is not null) THEN
                    'HI2PROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:HI2PROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2TXT), '0'), 38, 10) is null and 
                    src:HI2TXT is not null) THEN
                    'HI2TXT contains a non-numeric value : \'' || AS_VARCHAR(src:HI2TXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3BKG), '0'), 38, 10) is null and 
                    src:HI3BKG is not null) THEN
                    'HI3BKG contains a non-numeric value : \'' || AS_VARCHAR(src:HI3BKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3DEFACTION), '0'), 38, 10) is null and 
                    src:HI3DEFACTION is not null) THEN
                    'HI3DEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:HI3DEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3PCT), '0'), 38, 10) is null and 
                    src:HI3PCT is not null) THEN
                    'HI3PCT contains a non-numeric value : \'' || AS_VARCHAR(src:HI3PCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3PROCESS), '0'), 38, 10) is null and 
                    src:HI3PROCESS is not null) THEN
                    'HI3PROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:HI3PROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3TXT), '0'), 38, 10) is null and 
                    src:HI3TXT is not null) THEN
                    'HI3TXT contains a non-numeric value : \'' || AS_VARCHAR(src:HI3TXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1BKG), '0'), 38, 10) is null and 
                    src:LO1BKG is not null) THEN
                    'LO1BKG contains a non-numeric value : \'' || AS_VARCHAR(src:LO1BKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1DEFACTION), '0'), 38, 10) is null and 
                    src:LO1DEFACTION is not null) THEN
                    'LO1DEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:LO1DEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1PCT), '0'), 38, 10) is null and 
                    src:LO1PCT is not null) THEN
                    'LO1PCT contains a non-numeric value : \'' || AS_VARCHAR(src:LO1PCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1PROCESS), '0'), 38, 10) is null and 
                    src:LO1PROCESS is not null) THEN
                    'LO1PROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:LO1PROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1TXT), '0'), 38, 10) is null and 
                    src:LO1TXT is not null) THEN
                    'LO1TXT contains a non-numeric value : \'' || AS_VARCHAR(src:LO1TXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2BKG), '0'), 38, 10) is null and 
                    src:LO2BKG is not null) THEN
                    'LO2BKG contains a non-numeric value : \'' || AS_VARCHAR(src:LO2BKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2DEFACTION), '0'), 38, 10) is null and 
                    src:LO2DEFACTION is not null) THEN
                    'LO2DEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:LO2DEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2PCT), '0'), 38, 10) is null and 
                    src:LO2PCT is not null) THEN
                    'LO2PCT contains a non-numeric value : \'' || AS_VARCHAR(src:LO2PCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2PROCESS), '0'), 38, 10) is null and 
                    src:LO2PROCESS is not null) THEN
                    'LO2PROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:LO2PROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2TXT), '0'), 38, 10) is null and 
                    src:LO2TXT is not null) THEN
                    'LO2TXT contains a non-numeric value : \'' || AS_VARCHAR(src:LO2TXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3BKG), '0'), 38, 10) is null and 
                    src:LO3BKG is not null) THEN
                    'LO3BKG contains a non-numeric value : \'' || AS_VARCHAR(src:LO3BKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3DEFACTION), '0'), 38, 10) is null and 
                    src:LO3DEFACTION is not null) THEN
                    'LO3DEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:LO3DEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3PCT), '0'), 38, 10) is null and 
                    src:LO3PCT is not null) THEN
                    'LO3PCT contains a non-numeric value : \'' || AS_VARCHAR(src:LO3PCT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3PROCESS), '0'), 38, 10) is null and 
                    src:LO3PROCESS is not null) THEN
                    'LO3PROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:LO3PROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3TXT), '0'), 38, 10) is null and 
                    src:LO3TXT is not null) THEN
                    'LO3TXT contains a non-numeric value : \'' || AS_VARCHAR(src:LO3TXT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGBKG), '0'), 38, 10) is null and 
                    src:NEGUSGBKG is not null) THEN
                    'NEGUSGBKG contains a non-numeric value : \'' || AS_VARCHAR(src:NEGUSGBKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGDEFACTION), '0'), 38, 10) is null and 
                    src:NEGUSGDEFACTION is not null) THEN
                    'NEGUSGDEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:NEGUSGDEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGPROCESS), '0'), 38, 10) is null and 
                    src:NEGUSGPROCESS is not null) THEN
                    'NEGUSGPROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:NEGUSGPROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGTXT), '0'), 38, 10) is null and 
                    src:NEGUSGTXT is not null) THEN
                    'NEGUSGTXT contains a non-numeric value : \'' || AS_VARCHAR(src:NEGUSGTXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTBKG), '0'), 38, 10) is null and 
                    src:NOACCTBKG is not null) THEN
                    'NOACCTBKG contains a non-numeric value : \'' || AS_VARCHAR(src:NOACCTBKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTDEFACTION), '0'), 38, 10) is null and 
                    src:NOACCTDEFACTION is not null) THEN
                    'NOACCTDEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:NOACCTDEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTPROCESS), '0'), 38, 10) is null and 
                    src:NOACCTPROCESS is not null) THEN
                    'NOACCTPROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:NOACCTPROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTTXT), '0'), 38, 10) is null and 
                    src:NOACCTTXT is not null) THEN
                    'NOACCTTXT contains a non-numeric value : \'' || AS_VARCHAR(src:NOACCTTXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NORDBKG), '0'), 38, 10) is null and 
                    src:NORDBKG is not null) THEN
                    'NORDBKG contains a non-numeric value : \'' || AS_VARCHAR(src:NORDBKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NORDDEFACTION), '0'), 38, 10) is null and 
                    src:NORDDEFACTION is not null) THEN
                    'NORDDEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:NORDDEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NORMALPROCESS), '0'), 38, 10) is null and 
                    src:NORMALPROCESS is not null) THEN
                    'NORMALPROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:NORMALPROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGBKG), '0'), 38, 10) is null and 
                    src:NOUSGBKG is not null) THEN
                    'NOUSGBKG contains a non-numeric value : \'' || AS_VARCHAR(src:NOUSGBKG) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGDEFACTION), '0'), 38, 10) is null and 
                    src:NOUSGDEFACTION is not null) THEN
                    'NOUSGDEFACTION contains a non-numeric value : \'' || AS_VARCHAR(src:NOUSGDEFACTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGPROCESS), '0'), 38, 10) is null and 
                    src:NOUSGPROCESS is not null) THEN
                    'NOUSGPROCESS contains a non-numeric value : \'' || AS_VARCHAR(src:NOUSGPROCESS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGTXT), '0'), 38, 10) is null and 
                    src:NOUSGTXT is not null) THEN
                    'NOUSGTXT contains a non-numeric value : \'' || AS_VARCHAR(src:NOUSGTXT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RDPROBKEY), '0'), 38, 10) is null and 
                    src:RDPROBKEY is not null) THEN
                    'RDPROBKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RDPROBKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RDREVIEWSUKEY), '0'), 38, 10) is null and 
                    src:RDREVIEWSUKEY is not null) THEN
                    'RDREVIEWSUKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RDREVIEWSUKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPONADDIREADAPPROVAL), '0'), 38, 10) is null and 
                    src:UPONADDIREADAPPROVAL is not null) THEN
                    'UPONADDIREADAPPROVAL contains a non-numeric value : \'' || AS_VARCHAR(src:UPONADDIREADAPPROVAL) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPONCYREADAPPROVAL), '0'), 38, 10) is null and 
                    src:UPONCYREADAPPROVAL is not null) THEN
                    'UPONCYREADAPPROVAL contains a non-numeric value : \'' || AS_VARCHAR(src:UPONCYREADAPPROVAL) || '\'' WHEN 
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
                                    
                src:RDREVIEWSUKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_READINGREVIEWSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DAYSFORCURRD), '0'), 38, 10) is null and 
                    src:DAYSFORCURRD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTBKG), '0'), 38, 10) is null and 
                    src:ESTBKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ESTTXT), '0'), 38, 10) is null and 
                    src:ESTTXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1BKG), '0'), 38, 10) is null and 
                    src:HI1BKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1DEFACTION), '0'), 38, 10) is null and 
                    src:HI1DEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1PCT), '0'), 38, 10) is null and 
                    src:HI1PCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1PROCESS), '0'), 38, 10) is null and 
                    src:HI1PROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI1TXT), '0'), 38, 10) is null and 
                    src:HI1TXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2BKG), '0'), 38, 10) is null and 
                    src:HI2BKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2DEFACTION), '0'), 38, 10) is null and 
                    src:HI2DEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2PCT), '0'), 38, 10) is null and 
                    src:HI2PCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2PROCESS), '0'), 38, 10) is null and 
                    src:HI2PROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI2TXT), '0'), 38, 10) is null and 
                    src:HI2TXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3BKG), '0'), 38, 10) is null and 
                    src:HI3BKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3DEFACTION), '0'), 38, 10) is null and 
                    src:HI3DEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3PCT), '0'), 38, 10) is null and 
                    src:HI3PCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3PROCESS), '0'), 38, 10) is null and 
                    src:HI3PROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HI3TXT), '0'), 38, 10) is null and 
                    src:HI3TXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1BKG), '0'), 38, 10) is null and 
                    src:LO1BKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1DEFACTION), '0'), 38, 10) is null and 
                    src:LO1DEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1PCT), '0'), 38, 10) is null and 
                    src:LO1PCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1PROCESS), '0'), 38, 10) is null and 
                    src:LO1PROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO1TXT), '0'), 38, 10) is null and 
                    src:LO1TXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2BKG), '0'), 38, 10) is null and 
                    src:LO2BKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2DEFACTION), '0'), 38, 10) is null and 
                    src:LO2DEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2PCT), '0'), 38, 10) is null and 
                    src:LO2PCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2PROCESS), '0'), 38, 10) is null and 
                    src:LO2PROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO2TXT), '0'), 38, 10) is null and 
                    src:LO2TXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3BKG), '0'), 38, 10) is null and 
                    src:LO3BKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3DEFACTION), '0'), 38, 10) is null and 
                    src:LO3DEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3PCT), '0'), 38, 10) is null and 
                    src:LO3PCT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3PROCESS), '0'), 38, 10) is null and 
                    src:LO3PROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LO3TXT), '0'), 38, 10) is null and 
                    src:LO3TXT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGBKG), '0'), 38, 10) is null and 
                    src:NEGUSGBKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGDEFACTION), '0'), 38, 10) is null and 
                    src:NEGUSGDEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGPROCESS), '0'), 38, 10) is null and 
                    src:NEGUSGPROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NEGUSGTXT), '0'), 38, 10) is null and 
                    src:NEGUSGTXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTBKG), '0'), 38, 10) is null and 
                    src:NOACCTBKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTDEFACTION), '0'), 38, 10) is null and 
                    src:NOACCTDEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTPROCESS), '0'), 38, 10) is null and 
                    src:NOACCTPROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOACCTTXT), '0'), 38, 10) is null and 
                    src:NOACCTTXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NORDBKG), '0'), 38, 10) is null and 
                    src:NORDBKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NORDDEFACTION), '0'), 38, 10) is null and 
                    src:NORDDEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NORMALPROCESS), '0'), 38, 10) is null and 
                    src:NORMALPROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGBKG), '0'), 38, 10) is null and 
                    src:NOUSGBKG is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGDEFACTION), '0'), 38, 10) is null and 
                    src:NOUSGDEFACTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGPROCESS), '0'), 38, 10) is null and 
                    src:NOUSGPROCESS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NOUSGTXT), '0'), 38, 10) is null and 
                    src:NOUSGTXT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RDPROBKEY), '0'), 38, 10) is null and 
                    src:RDPROBKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RDREVIEWSUKEY), '0'), 38, 10) is null and 
                    src:RDREVIEWSUKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPONADDIREADAPPROVAL), '0'), 38, 10) is null and 
                    src:UPONADDIREADAPPROVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:UPONCYREADAPPROVAL), '0'), 38, 10) is null and 
                    src:UPONCYREADAPPROVAL is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)