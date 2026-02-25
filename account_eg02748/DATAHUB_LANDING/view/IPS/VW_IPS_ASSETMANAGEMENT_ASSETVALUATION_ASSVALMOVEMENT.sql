CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT AS SELECT
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
                        src:WDVPS::numeric(38, 10) AS WDVPS, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:ASSVALMOVEMENTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASSVALMOVEMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALMOVEMENT as strm))