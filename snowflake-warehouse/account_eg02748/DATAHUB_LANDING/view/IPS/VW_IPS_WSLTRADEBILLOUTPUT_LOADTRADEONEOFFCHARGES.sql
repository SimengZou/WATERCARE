CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLTRADEBILLOUTPUT_LOADTRADEONEOFFCHARGES AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CHARGEAMOUNT::varchar AS CHARGEAMOUNT, 
                        src:CHARGEDATE::varchar AS CHARGEDATE, 
                        src:CHARGETYPE::varchar AS CHARGETYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:FILEEXTENSION::varchar AS FILEEXTENSION, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:LINEITEMSETUPKEY::integer AS LINEITEMSETUPKEY, 
                        src:LOADTRADEONEOFFCHARGESKEY::integer AS LOADTRADEONEOFFCHARGESKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONEOFFCHARGEKEY::integer AS ONEOFFCHARGEKEY, 
                        src:STATUS::varchar AS STATUS, 
                        src:STATUSMSG::varchar AS STATUSMSG, 
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
    
                        
                src:LOADTRADEONEOFFCHARGESKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LOADTRADEONEOFFCHARGESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLTRADEBILLOUTPUT_LOADTRADEONEOFFCHARGES as strm))