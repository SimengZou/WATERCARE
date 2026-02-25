CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLBILLING_MIGSERVICES AS SELECT
                        src:ACCOUNTNUMBER::varchar AS ACCOUNTNUMBER, 
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSKEY1::integer AS ADDRESSKEY1, 
                        src:ADDRESSKEY2::integer AS ADDRESSKEY2, 
                        src:ADDRESSKEY3::integer AS ADDRESSKEY3, 
                        src:ADDRESSKEY4::integer AS ADDRESSKEY4, 
                        src:ADDRESSKEY5::integer AS ADDRESSKEY5, 
                        src:ADDRESSKEY6::integer AS ADDRESSKEY6, 
                        src:ADDRESSKEY7::integer AS ADDRESSKEY7, 
                        src:ANNUALFIX::integer AS ANNUALFIX, 
                        src:BILLABLEPERC::numeric(38, 10) AS BILLABLEPERC, 
                        src:BILLABLEPERC1::numeric(38, 10) AS BILLABLEPERC1, 
                        src:BILLABLEPERC2::numeric(38, 10) AS BILLABLEPERC2, 
                        src:BILLABLEPERC3::numeric(38, 10) AS BILLABLEPERC3, 
                        src:BILLABLEPERC4::numeric(38, 10) AS BILLABLEPERC4, 
                        src:BILLABLEPERC5::numeric(38, 10) AS BILLABLEPERC5, 
                        src:BILLABLEPERC6::numeric(38, 10) AS BILLABLEPERC6, 
                        src:BILLABLEPERC7::numeric(38, 10) AS BILLABLEPERC7, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:LASTBILLDATE::varchar AS LASTBILLDATE, 
                        src:MIGSERVICESKEY::integer AS MIGSERVICESKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTI::varchar AS MULTI, 
                        src:NUMMETERS::integer AS NUMMETERS, 
                        src:POSITION1::integer AS POSITION1, 
                        src:POSITION2::integer AS POSITION2, 
                        src:POSITION3::integer AS POSITION3, 
                        src:POSITION4::integer AS POSITION4, 
                        src:POSITION5::integer AS POSITION5, 
                        src:POSITION6::integer AS POSITION6, 
                        src:POSITION7::integer AS POSITION7, 
                        src:PRICINGPLAN::varchar AS PRICINGPLAN, 
                        src:READDATE1::datetime AS READDATE1, 
                        src:READDATE2::datetime AS READDATE2, 
                        src:READDATE3::datetime AS READDATE3, 
                        src:READDATE4::datetime AS READDATE4, 
                        src:READDATE5::datetime AS READDATE5, 
                        src:READDATE6::datetime AS READDATE6, 
                        src:READDATE7::datetime AS READDATE7, 
                        src:SRVTYPE::varchar AS SRVTYPE, 
                        src:STARTDATE::datetime AS STARTDATE, 
                        src:STARTKEY1::integer AS STARTKEY1, 
                        src:STARTKEY2::integer AS STARTKEY2, 
                        src:STARTKEY3::integer AS STARTKEY3, 
                        src:STARTKEY4::integer AS STARTKEY4, 
                        src:STARTKEY5::integer AS STARTKEY5, 
                        src:STARTKEY6::integer AS STARTKEY6, 
                        src:STARTKEY7::integer AS STARTKEY7, 
                        src:STATUSMESG::varchar AS STATUSMESG, 
                        src:SUBFLAG1::varchar AS SUBFLAG1, 
                        src:SUBFLAG2::varchar AS SUBFLAG2, 
                        src:SUBFLAG3::varchar AS SUBFLAG3, 
                        src:SUBFLAG4::varchar AS SUBFLAG4, 
                        src:SUBFLAG5::varchar AS SUBFLAG5, 
                        src:SUBFLAG6::varchar AS SUBFLAG6, 
                        src:SUBFLAG7::varchar AS SUBFLAG7, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WTRDATA1::varchar AS WTRDATA1, 
                        src:WTRDATA2::varchar AS WTRDATA2, 
                        src:WTRDATA3::varchar AS WTRDATA3, 
                        src:WTRDATA4::varchar AS WTRDATA4, 
                        src:WTRDATA5::varchar AS WTRDATA5, 
                        src:WTRDATA6::varchar AS WTRDATA6, 
                        src:WTRDATA7::varchar AS WTRDATA7, 
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
    
                        
                src:MIGSERVICESKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MIGSERVICESKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLBILLING_MIGSERVICES as strm))