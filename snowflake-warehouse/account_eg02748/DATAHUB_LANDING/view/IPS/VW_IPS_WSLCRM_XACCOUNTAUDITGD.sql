CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XACCOUNTAUDITGD AS SELECT
                        src:"ACCOUNT"::integer AS "ACCOUNT", 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHANGEDDATE::datetime AS CHANGEDDATE, 
                        src:CHANGEDVALUE::varchar AS CHANGEDVALUE, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENTITYKEY::integer AS ENTITYKEY, 
                        src:ENTITYNAME::varchar AS ENTITYNAME, 
                        src:IPSFIELDNAME::varchar AS IPSFIELDNAME, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODIFIEDBYEMPLOYEE::varchar AS MODIFIEDBYEMPLOYEE, 
                        src:ORIGINALVALUE::varchar AS ORIGINALVALUE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XACCOUNTAUDITDP::integer AS XACCOUNTAUDITDP, 
                        src:XACCOUNTAUDITGDKEY::integer AS XACCOUNTAUDITGDKEY, 
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
    
                        
                src:XACCOUNTAUDITGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XACCOUNTAUDITGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XACCOUNTAUDITGD as strm))