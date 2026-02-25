CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE AS SELECT
                        src:ACCDEPREC::numeric(38, 10) AS ACCDEPREC, 
                        src:ACCPERIOD::integer AS ACCPERIOD, 
                        src:ACQDATE::datetime AS ACQDATE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALDATE::datetime AS ASSVALDATE, 
                        src:ASSVALKEY::integer AS ASSVALKEY, 
                        src:ASSVALRUNKEY::integer AS ASSVALRUNKEY, 
                        src:BACKDATEADJFLAG::varchar AS BACKDATEADJFLAG, 
                        src:BGTKEY::integer AS BGTKEY, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:CAPTLEXP::numeric(38, 10) AS CAPTLEXP, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPRECAMT::numeric(38, 10) AS DEPRECAMT, 
                        src:DEPRECDATE::datetime AS DEPRECDATE, 
                        src:DEPRECKEY::integer AS DEPRECKEY, 
                        src:DEPRECPERD::numeric(38, 10) AS DEPRECPERD, 
                        src:DEPRECRATE::numeric(38, 10) AS DEPRECRATE, 
                        src:DEPRECTYPE::integer AS DEPRECTYPE, 
                        src:DEPRECYTD::numeric(38, 10) AS DEPRECYTD, 
                        src:DISPSLFLAG::varchar AS DISPSLFLAG, 
                        src:EFFECTIVEDTTM::datetime AS EFFECTIVEDTTM, 
                        src:EXPLIFE::numeric(38, 10) AS EXPLIFE, 
                        src:JOURNALDATE::datetime AS JOURNALDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PERIODADJDEPREC::numeric(38, 10) AS PERIODADJDEPREC, 
                        src:RECLDEPKEY::integer AS RECLDEPKEY, 
                        src:RECLNO::integer AS RECLNO, 
                        src:RESIDLIFE::numeric(38, 10) AS RESIDLIFE, 
                        src:RESIDVAL::numeric(38, 10) AS RESIDVAL, 
                        src:REVALFINALCOST::numeric(38, 10) AS REVALFINALCOST, 
                        src:REVALFLAG::varchar AS REVALFLAG, 
                        src:TOBESOLDFLAG::varchar AS TOBESOLDFLAG, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WRITEDNVAL::numeric(38, 10) AS WRITEDNVAL, 
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
    
                        
                src:DEPRECKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DEPRECKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as strm))