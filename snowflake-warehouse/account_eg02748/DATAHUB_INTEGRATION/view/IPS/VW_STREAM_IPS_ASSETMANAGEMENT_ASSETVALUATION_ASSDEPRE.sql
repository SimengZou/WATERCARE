CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE AS SELECT
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
                        src:WRITEDNVAL::numeric(38, 10) AS WRITEDNVAL, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:DEPRECKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSDEPRE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCDEPREC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCPERIOD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACQDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ASSVALDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSVALRUNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BGTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CAPTLEXP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DEPRECDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECPERD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECRATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DEPRECYTD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:JOURNALDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PERIODADJDEPREC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RECLDEPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RECLNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESIDLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESIDVAL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVALFINALCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WRITEDNVAL), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null