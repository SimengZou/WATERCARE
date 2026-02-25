CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_ACCOUNTTRADEWASTE AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ACCOUNTTRADEWASTEKEY::integer AS ACCOUNTTRADEWASTEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPEARANCE::varchar AS APPEARANCE, 
                        src:AREA::varchar AS AREA, 
                        src:BILLEDFLAG::varchar AS BILLEDFLAG, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:COMMENTKEY::integer AS COMMENTKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DOCKETNUMBER::varchar AS DOCKETNUMBER, 
                        src:EMPLOYEEID::varchar AS EMPLOYEEID, 
                        src:FACILITY::varchar AS FACILITY, 
                        src:GROSS::numeric(38, 10) AS GROSS, 
                        src:INDTTM::datetime AS INDTTM, 
                        src:LATEFEEAMOUNT::numeric(38, 10) AS LATEFEEAMOUNT, 
                        src:LATEFEEFLAG::varchar AS LATEFEEFLAG, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:LOCATION::varchar AS LOCATION, 
                        src:MANIFESTFLAG::varchar AS MANIFESTFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NET::numeric(38, 10) AS NET, 
                        src:NOMINATEDONEOFF::integer AS NOMINATEDONEOFF, 
                        src:ODOR::varchar AS ODOR, 
                        src:ORIGIN::varchar AS ORIGIN, 
                        src:OUTDTTM::datetime AS OUTDTTM, 
                        src:PURCHASENUMBER::varchar AS PURCHASENUMBER, 
                        src:TARE::numeric(38, 10) AS TARE, 
                        src:TRAILERREGKEY::integer AS TRAILERREGKEY, 
                        src:TWVEHICLEREGKEY::integer AS TWVEHICLEREGKEY, 
                        src:UNIT::integer AS UNIT, 
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
    
                        
                src:ACCOUNTTRADEWASTEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:ACCOUNTTRADEWASTEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_ACCOUNTTRADEWASTE as strm))