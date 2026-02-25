CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ARRANGEMENTSTATUSHISTORY AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ARRANGEMENTSTATUSHISTKEY::integer AS ARRANGEMENTSTATUSHISTKEY, 
                        src:CHANGEDBY::varchar AS CHANGEDBY, 
                        src:CHANGEDDTTM::datetime AS CHANGEDDTTM, 
                        src:CLOSEDFLAG::varchar AS CLOSEDFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:HISTORICARRANGEMENTKEY::integer AS HISTORICARRANGEMENTKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OVERRIDEFLAG::varchar AS OVERRIDEFLAG, 
                        src:PAYMENTARRANGEMENTKEY::integer AS PAYMENTARRANGEMENTKEY, 
                        src:PREVIOUSSTATUS::varchar AS PREVIOUSSTATUS, 
                        src:PYMNTARRANGEMENTREVIEWKEY::integer AS PYMNTARRANGEMENTREVIEWKEY, 
                        src:REASON::varchar AS REASON, 
                        src:STATUS::varchar AS STATUS, 
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
    
                        
                src:ARRANGEMENTSTATUSHISTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ARRANGEMENTSTATUSHISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ARRANGEMENTSTATUSHISTORY as strm))