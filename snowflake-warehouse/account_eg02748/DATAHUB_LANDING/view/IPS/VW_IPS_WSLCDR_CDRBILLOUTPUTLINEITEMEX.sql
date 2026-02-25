CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDR_CDRBILLOUTPUTLINEITEMEX AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BILLRUNNUMBER::integer AS BILLRUNNUMBER, 
                        src:CDRBILLOUTPUTLINEITEMEXKEY::integer AS CDRBILLOUTPUTLINEITEMEXKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:FEECODE::varchar AS FEECODE, 
                        src:FEEDESC::varchar AS FEEDESC, 
                        src:FEEREFERENCE::varchar AS FEEREFERENCE, 
                        src:FEETYPEKEY::integer AS FEETYPEKEY, 
                        src:LINEITEM::varchar AS LINEITEM, 
                        src:LINEITEMDESC::varchar AS LINEITEMDESC, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:QTY::numeric(38, 10) AS QTY, 
                        src:TOTALAMOUNT::numeric(38, 10) AS TOTALAMOUNT, 
                        src:VALUE::numeric(38, 10) AS VALUE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WAIVED::varchar AS WAIVED, 
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
    
                        
                src:CDRBILLOUTPUTLINEITEMEXKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CDRBILLOUTPUTLINEITEMEXKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDR_CDRBILLOUTPUTLINEITEMEX as strm))