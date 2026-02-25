CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_RESOURCES_CONTACT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDR1::varchar AS ADDR1, 
                        src:ADDR2::varchar AS ADDR2, 
                        src:CARRT::varchar AS CARRT, 
                        src:CASSBARCODE::integer AS CASSBARCODE, 
                        src:CASSVALIDATIONDESC::varchar AS CASSVALIDATIONDESC, 
                        src:CASSVALIDATIONDT::datetime AS CASSVALIDATIONDT, 
                        src:CASSVALIDATIONSTATUS::varchar AS CASSVALIDATIONSTATUS, 
                        src:CASSVER::integer AS CASSVER, 
                        src:CITY::varchar AS CITY, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:CONAME::varchar AS CONAME, 
                        src:CONTACTTYPE::varchar AS CONTACTTYPE, 
                        src:CONTENTASATTACHMENT::varchar AS CONTENTASATTACHMENT, 
                        src:CORRDELIVERY::integer AS CORRDELIVERY, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DAYPHN::varchar AS DAYPHN, 
                        src:DPC::varchar AS DPC, 
                        src:EMAIL::varchar AS EMAIL, 
                        src:EVEPHN::varchar AS EVEPHN, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FAX::varchar AS FAX, 
                        src:FORGN::varchar AS FORGN, 
                        src:IDKEY::integer AS IDKEY, 
                        src:INTERNETID1::varchar AS INTERNETID1, 
                        src:INTERNETID2::varchar AS INTERNETID2, 
                        src:INTERNETID3::varchar AS INTERNETID3, 
                        src:INTERNETID4::varchar AS INTERNETID4, 
                        src:INTERNETTYPE1::varchar AS INTERNETTYPE1, 
                        src:INTERNETTYPE2::varchar AS INTERNETTYPE2, 
                        src:INTERNETTYPE3::varchar AS INTERNETTYPE3, 
                        src:INTERNETTYPE4::varchar AS INTERNETTYPE4, 
                        src:ISSERVICEPROVIDER::varchar AS ISSERVICEPROVIDER, 
                        src:LOT::varchar AS LOT, 
                        src:MOBILE::varchar AS MOBILE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PGR::varchar AS PGR, 
                        src:PGRPIN::varchar AS PGRPIN, 
                        src:POSITION::varchar AS POSITION, 
                        src:PROFESSION::varchar AS PROFESSION, 
                        src:REFERENCEVALUE1::varchar AS REFERENCEVALUE1, 
                        src:REFERENCEVALUE2::varchar AS REFERENCEVALUE2, 
                        src:REFNO::varchar AS REFNO, 
                        src:SALARY::numeric(38, 10) AS SALARY, 
                        src:SEASADDR1::varchar AS SEASADDR1, 
                        src:SEASADDR2::varchar AS SEASADDR2, 
                        src:SEASCARRT::varchar AS SEASCARRT, 
                        src:SEASCASSBARCODE::integer AS SEASCASSBARCODE, 
                        src:SEASCASSVALIDATIONDESC::varchar AS SEASCASSVALIDATIONDESC, 
                        src:SEASCASSVALIDATIONDT::datetime AS SEASCASSVALIDATIONDT, 
                        src:SEASCASSVALIDATIONSTATUS::varchar AS SEASCASSVALIDATIONSTATUS, 
                        src:SEASCASSVER::integer AS SEASCASSVER, 
                        src:SEASCITY::varchar AS SEASCITY, 
                        src:SEASCNTRY::varchar AS SEASCNTRY, 
                        src:SEASDAYPHN::varchar AS SEASDAYPHN, 
                        src:SEASDPC::varchar AS SEASDPC, 
                        src:SEASEVEPHN::varchar AS SEASEVEPHN, 
                        src:SEASFAX::varchar AS SEASFAX, 
                        src:SEASFROMDT::datetime AS SEASFROMDT, 
                        src:SEASLOT::varchar AS SEASLOT, 
                        src:SEASSTATE::varchar AS SEASSTATE, 
                        src:SEASTODT::datetime AS SEASTODT, 
                        src:SEASZIP::varchar AS SEASZIP, 
                        src:STATE::varchar AS STATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WEBUSERKEY::integer AS WEBUSERKEY, 
                        src:ZIP::varchar AS ZIP, 
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
    
                        
                src:CNTCTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CNTCTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_RESOURCES_CONTACT as strm))