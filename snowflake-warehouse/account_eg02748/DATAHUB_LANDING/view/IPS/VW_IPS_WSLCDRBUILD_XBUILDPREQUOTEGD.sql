CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRBUILD_XBUILDPREQUOTEGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CUSTOMERREFERENCE::varchar AS CUSTOMERREFERENCE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:FEE::varchar AS FEE, 
                        src:FEEAMOUNT::numeric(38, 10) AS FEEAMOUNT, 
                        src:FEEQTY::numeric(38, 10) AS FEEQTY, 
                        src:FEEUNIT::varchar AS FEEUNIT, 
                        src:FEEVALUE::numeric(38, 10) AS FEEVALUE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XBUILDAPPESTDPKEY::integer AS XBUILDAPPESTDPKEY, 
                        src:XBUILDPREQUOTEGDKEY::integer AS XBUILDPREQUOTEGDKEY, 
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
    
                        
                src:XBUILDPREQUOTEGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XBUILDPREQUOTEGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRBUILD_XBUILDPREQUOTEGD as strm))