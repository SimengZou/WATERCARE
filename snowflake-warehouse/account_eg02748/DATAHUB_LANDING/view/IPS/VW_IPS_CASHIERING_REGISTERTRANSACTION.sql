CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CASHIERING_REGISTERTRANSACTION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ASSOCIATEDEMPLOYEE::varchar AS ASSOCIATEDEMPLOYEE, 
                        src:CLEANUPSESSIONID::varchar AS CLEANUPSESSIONID, 
                        src:CONTACT::integer AS CONTACT, 
                        src:DELETED::boolean AS DELETED, 
                        src:DRWRKEY::integer AS DRWRKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RECEIPTNUMBER::varchar AS RECEIPTNUMBER, 
                        src:REFTRANNO::integer AS REFTRANNO, 
                        src:REGKEY::integer AS REGKEY, 
                        src:REGTRANNO::integer AS REGTRANNO, 
                        src:STATUS::varchar AS STATUS, 
                        src:TRANAMT::numeric(38, 10) AS TRANAMT, 
                        src:TRANBY::varchar AS TRANBY, 
                        src:TRANDTTM::datetime AS TRANDTTM, 
                        src:TRANPAYER::varchar AS TRANPAYER, 
                        src:TRANTYPE::varchar AS TRANTYPE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOIDBY::varchar AS VOIDBY, 
                        src:VOIDDTTM::datetime AS VOIDDTTM, 
                        src:VOIDREAS::varchar AS VOIDREAS, 
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
    
                        
                src:REGTRANNO,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:REGTRANNO  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CASHIERING_REGISTERTRANSACTION as strm))