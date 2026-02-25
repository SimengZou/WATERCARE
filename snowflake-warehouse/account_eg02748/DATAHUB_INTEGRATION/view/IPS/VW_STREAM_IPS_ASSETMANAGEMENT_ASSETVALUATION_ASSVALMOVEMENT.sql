CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT AS SELECT
                        src:ACCUMDEPRECPE::numeric(38, 10) AS ACCUMDEPRECPE, 
                        src:ACCUMDEPRECPS::numeric(38, 10) AS ACCUMDEPRECPS, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALKEY::integer AS ASSVALKEY, 
                        src:ASSVALMOVEMENTKEY::integer AS ASSVALMOVEMENTKEY, 
                        src:CAPTCOSTCP::numeric(38, 10) AS CAPTCOSTCP, 
                        src:CURRENTCOSTPE::numeric(38, 10) AS CURRENTCOSTPE, 
                        src:CURRENTCOSTPS::numeric(38, 10) AS CURRENTCOSTPS, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPRECCP::numeric(38, 10) AS DEPRECCP, 
                        src:DISPDEPRECWRTNBCKCP::numeric(38, 10) AS DISPDEPRECWRTNBCKCP, 
                        src:DISPOSALSCD::numeric(38, 10) AS DISPOSALSCD, 
                        src:EFFECTIVEDTTM::datetime AS EFFECTIVEDTTM, 
                        src:IMPVALPRIORTOREVALPE::numeric(38, 10) AS IMPVALPRIORTOREVALPE, 
                        src:INITCOSTCP::numeric(38, 10) AS INITCOSTCP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PLONSALECP::numeric(38, 10) AS PLONSALECP, 
                        src:PROCONDISPCP::numeric(38, 10) AS PROCONDISPCP, 
                        src:REVALACCUMDEPRECBGHTTOACCTCP::numeric(38, 10) AS REVALACCUMDEPRECBGHTTOACCTCP, 
                        src:REVALACCUMDEPRECWRTNBCKGROSSCP::numeric(38, 10) AS REVALACCUMDEPRECWRTNBCKGROSSCP, 
                        src:REVALACCUMDEPRECWRTNBCKNETCP::numeric(38, 10) AS REVALACCUMDEPRECWRTNBCKNETCP, 
                        src:REVALINCDEC::numeric(38, 10) AS REVALINCDEC, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WDVPE::numeric(38, 10) AS WDVPE, 
                        src:WDVPS::numeric(38, 10) AS WDVPS, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCUMDEPRECPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCUMDEPRECPS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALMOVEMENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CAPTCOSTCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTCOSTPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTCOSTPS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPDEPRECWRTNBCKCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPOSALSCD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPVALPRIORTOREVALPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INITCOSTCP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PLONSALECP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROCONDISPCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALACCUMDEPRECBGHTTOACCTCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKGROSSCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALACCUMDEPRECWRTNBCKNETCP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALINCDEC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WDVPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WDVPS), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null