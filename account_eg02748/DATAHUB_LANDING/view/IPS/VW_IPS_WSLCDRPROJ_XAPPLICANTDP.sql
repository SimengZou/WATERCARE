CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XAPPLICANTDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJAPPLDTLKEY::integer AS APPROJAPPLDTLKEY, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:CITY::varchar AS CITY, 
                        src:COMPANYNAME::varchar AS COMPANYNAME, 
                        src:CPE::varchar AS CPE, 
                        src:CPENGNUMBER::varchar AS CPENGNUMBER, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EMAIL::varchar AS EMAIL, 
                        src:FIRSTNAME::varchar AS FIRSTNAME, 
                        src:LASTNAME::varchar AS LASTNAME, 
                        src:MOBILENUMBER::varchar AS MOBILENUMBER, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PHONENUMBER::varchar AS PHONENUMBER, 
                        src:POSTALCODE::varchar AS POSTALCODE, 
                        src:REFERENCENUMBER::varchar AS REFERENCENUMBER, 
                        src:SAMEASAPPLICANT::varchar AS SAMEASAPPLICANT, 
                        src:STREETNAME::varchar AS STREETNAME, 
                        src:STREETNUMBER::varchar AS STREETNUMBER, 
                        src:SUBURB::varchar AS SUBURB, 
                        src:TITLE::varchar AS TITLE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XAPPLICANTDPKEY::integer AS XAPPLICANTDPKEY, 
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
    
                        
                src:XAPPLICANTDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XAPPLICANTDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XAPPLICANTDP as strm))