CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT_ERROR AS SELECT src, 'IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECPE), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECPE is not null) THEN
                    'ACCUMDEPRECPE contains a non-numeric value : \'' || AS_VARCHAR(src:ACCUMDEPRECPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECPS), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECPS is not null) THEN
                    'ACCUMDEPRECPS contains a non-numeric value : \'' || AS_VARCHAR(src:ACCUMDEPRECPS) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) THEN
                    'ASSVALKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALMOVEMENTKEY), '0'), 38, 10) is null and 
                    src:ASSVALMOVEMENTKEY is not null) THEN
                    'ASSVALMOVEMENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ASSVALMOVEMENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPTCOSTCP), '0'), 38, 10) is null and 
                    src:CAPTCOSTCP is not null) THEN
                    'CAPTCOSTCP contains a non-numeric value : \'' || AS_VARCHAR(src:CAPTCOSTCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTPE), '0'), 38, 10) is null and 
                    src:CURRENTCOSTPE is not null) THEN
                    'CURRENTCOSTPE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTCOSTPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTPS), '0'), 38, 10) is null and 
                    src:CURRENTCOSTPS is not null) THEN
                    'CURRENTCOSTPS contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTCOSTPS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECCP), '0'), 38, 10) is null and 
                    src:DEPRECCP is not null) THEN
                    'DEPRECCP contains a non-numeric value : \'' || AS_VARCHAR(src:DEPRECCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPDEPRECWRTNBCKCP), '0'), 38, 10) is null and 
                    src:DISPDEPRECWRTNBCKCP is not null) THEN
                    'DISPDEPRECWRTNBCKCP contains a non-numeric value : \'' || AS_VARCHAR(src:DISPDEPRECWRTNBCKCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALSCD), '0'), 38, 10) is null and 
                    src:DISPOSALSCD is not null) THEN
                    'DISPOSALSCD contains a non-numeric value : \'' || AS_VARCHAR(src:DISPOSALSCD) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is null and 
                    src:EFFECTIVEDTTM is not null) THEN
                    'EFFECTIVEDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPVALPRIORTOREVALPE), '0'), 38, 10) is null and 
                    src:IMPVALPRIORTOREVALPE is not null) THEN
                    'IMPVALPRIORTOREVALPE contains a non-numeric value : \'' || AS_VARCHAR(src:IMPVALPRIORTOREVALPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITCOSTCP), '0'), 38, 10) is null and 
                    src:INITCOSTCP is not null) THEN
                    'INITCOSTCP contains a non-numeric value : \'' || AS_VARCHAR(src:INITCOSTCP) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLONSALECP), '0'), 38, 10) is null and 
                    src:PLONSALECP is not null) THEN
                    'PLONSALECP contains a non-numeric value : \'' || AS_VARCHAR(src:PLONSALECP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROCONDISPCP), '0'), 38, 10) is null and 
                    src:PROCONDISPCP is not null) THEN
                    'PROCONDISPCP contains a non-numeric value : \'' || AS_VARCHAR(src:PROCONDISPCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALACCUMDEPRECBGHTTOACCTCP), '0'), 38, 10) is null and 
                    src:REVALACCUMDEPRECBGHTTOACCTCP is not null) THEN
                    'REVALACCUMDEPRECBGHTTOACCTCP contains a non-numeric value : \'' || AS_VARCHAR(src:REVALACCUMDEPRECBGHTTOACCTCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKGROSSCP), '0'), 38, 10) is null and 
                    src:REVALACCUMDEPRECWRTNBCKGROSSCP is not null) THEN
                    'REVALACCUMDEPRECWRTNBCKGROSSCP contains a non-numeric value : \'' || AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKGROSSCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKNETCP), '0'), 38, 10) is null and 
                    src:REVALACCUMDEPRECWRTNBCKNETCP is not null) THEN
                    'REVALACCUMDEPRECWRTNBCKNETCP contains a non-numeric value : \'' || AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKNETCP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALINCDEC), '0'), 38, 10) is null and 
                    src:REVALINCDEC is not null) THEN
                    'REVALINCDEC contains a non-numeric value : \'' || AS_VARCHAR(src:REVALINCDEC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WDVPE), '0'), 38, 10) is null and 
                    src:WDVPE is not null) THEN
                    'WDVPE contains a non-numeric value : \'' || AS_VARCHAR(src:WDVPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WDVPS), '0'), 38, 10) is null and 
                    src:WDVPS is not null) THEN
                    'WDVPS contains a non-numeric value : \'' || AS_VARCHAR(src:WDVPS) || '\'' WHEN 
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
                                    
                src:ASSVALMOVEMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECPE), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ACCUMDEPRECPS), '0'), 38, 10) is null and 
                    src:ACCUMDEPRECPS is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is null and 
                    src:ASSVALKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ASSVALMOVEMENTKEY), '0'), 38, 10) is null and 
                    src:ASSVALMOVEMENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CAPTCOSTCP), '0'), 38, 10) is null and 
                    src:CAPTCOSTCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTPE), '0'), 38, 10) is null and 
                    src:CURRENTCOSTPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOSTPS), '0'), 38, 10) is null and 
                    src:CURRENTCOSTPS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEPRECCP), '0'), 38, 10) is null and 
                    src:DEPRECCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPDEPRECWRTNBCKCP), '0'), 38, 10) is null and 
                    src:DISPDEPRECWRTNBCKCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISPOSALSCD), '0'), 38, 10) is null and 
                    src:DISPOSALSCD is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is null and 
                    src:EFFECTIVEDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:IMPVALPRIORTOREVALPE), '0'), 38, 10) is null and 
                    src:IMPVALPRIORTOREVALPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITCOSTCP), '0'), 38, 10) is null and 
                    src:INITCOSTCP is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PLONSALECP), '0'), 38, 10) is null and 
                    src:PLONSALECP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROCONDISPCP), '0'), 38, 10) is null and 
                    src:PROCONDISPCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALACCUMDEPRECBGHTTOACCTCP), '0'), 38, 10) is null and 
                    src:REVALACCUMDEPRECBGHTTOACCTCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKGROSSCP), '0'), 38, 10) is null and 
                    src:REVALACCUMDEPRECWRTNBCKGROSSCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKNETCP), '0'), 38, 10) is null and 
                    src:REVALACCUMDEPRECWRTNBCKNETCP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVALINCDEC), '0'), 38, 10) is null and 
                    src:REVALINCDEC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WDVPE), '0'), 38, 10) is null and 
                    src:WDVPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WDVPS), '0'), 38, 10) is null and 
                    src:WDVPS is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)