CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETVALUATION_ASSETVALUATIONEXT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AQUISITIONDATE::datetime AS AQUISITIONDATE, 
                        src:AQUISITIONTYPE::varchar AS AQUISITIONTYPE, 
                        src:ASSETCLASS::varchar AS ASSETCLASS, 
                        src:ASSETVALUATIONEXTKEY::integer AS ASSETVALUATIONEXTKEY, 
                        src:BOOKID::varchar AS BOOKID, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPTYPE::integer AS COMPTYPE, 
                        src:COSTCENTRE::varchar AS COSTCENTRE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEPRECIATIONMETHOD::integer AS DEPRECIATIONMETHOD, 
                        src:EXPECTEDLIFE::numeric(38, 10) AS EXPECTEDLIFE, 
                        src:ID::varchar AS ID, 
                        src:INITIALCOST::numeric(38, 10) AS INITIALCOST, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
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
    
                        
                src:ASSETVALUATIONEXTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ASSETVALUATIONEXTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETVALUATION_ASSETVALUATIONEXT as strm))