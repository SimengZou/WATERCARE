CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSVALGRPKEY::integer AS ASSVALGRPKEY, 
                        src:ASSVALRUNKEY::integer AS ASSVALRUNKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:INITIALBATCHDTTM::datetime AS INITIALBATCHDTTM, 
                        src:INITIALRUNCOMPLETEDTTM::datetime AS INITIALRUNCOMPLETEDTTM, 
                        src:INITIALRUNDDTTM::datetime AS INITIALRUNDDTTM, 
                        src:JOURNALCOMMITDTTM::datetime AS JOURNALCOMMITDTTM, 
                        src:JOURNALDTTM::datetime AS JOURNALDTTM, 
                        src:JOURNALREVERSALDTTM::datetime AS JOURNALREVERSALDTTM, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NUMDEPRE::integer AS NUMDEPRE, 
                        src:NUMREVAL::integer AS NUMREVAL, 
                        src:NUMVAL::integer AS NUMVAL, 
                        src:RUNTYPE::integer AS RUNTYPE, 
                        src:SCHEDULEDDATE::datetime AS SCHEDULEDDATE, 
                        src:STAGE::integer AS STAGE, 
                        src:STATUSCOMP::varchar AS STATUSCOMP, 
                        src:VALDTTM::datetime AS VALDTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
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
    
                        
                src:ASSVALRUNKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASSVALRUNKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_ASSETMANAGEMENT_ASSETVALUATION_ASSVALRUN as strm))