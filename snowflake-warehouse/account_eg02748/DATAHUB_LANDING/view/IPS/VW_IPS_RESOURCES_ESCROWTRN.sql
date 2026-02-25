CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_ESCROWTRN AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADJREAS::varchar AS ADJREAS, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESCROWACCTKEY::integer AS ESCROWACCTKEY, 
                        src:ESCROWPAYKEY::integer AS ESCROWPAYKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:TRNAMT::numeric(38, 10) AS TRNAMT, 
                        src:TRNBY::varchar AS TRNBY, 
                        src:TRNDTTM::datetime AS TRNDTTM, 
                        src:TRNNO::integer AS TRNNO, 
                        src:TRNTYPE::integer AS TRNTYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XFERREFTRNNO::integer AS XFERREFTRNNO, 
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
    
                        
                src:TRNNO,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:TRNNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_ESCROWTRN as strm))