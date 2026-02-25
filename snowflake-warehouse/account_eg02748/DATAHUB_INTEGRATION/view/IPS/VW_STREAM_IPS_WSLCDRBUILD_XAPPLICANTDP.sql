CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XAPPLICANTDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDR1::varchar AS ADDR1, 
                        src:APBLDGAPPLDTLKEY::integer AS APBLDGAPPLDTLKEY, 
                        src:CAPACITY::varchar AS CAPACITY, 
                        src:CITY::varchar AS CITY, 
                        src:CONAME::varchar AS CONAME, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:EMAIL::varchar AS EMAIL, 
                        src:FIRSTNAME::varchar AS FIRSTNAME, 
                        src:LASTNAME::varchar AS LASTNAME, 
                        src:MOBILE::varchar AS MOBILE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONSITEADDRESSLINE1::varchar AS ONSITEADDRESSLINE1, 
                        src:ONSITECITY::varchar AS ONSITECITY, 
                        src:ONSITECOMPANYNAME::varchar AS ONSITECOMPANYNAME, 
                        src:ONSITEEMAILADDRESS::varchar AS ONSITEEMAILADDRESS, 
                        src:ONSITEFIRSTNAME::varchar AS ONSITEFIRSTNAME, 
                        src:ONSITELASTNAME::varchar AS ONSITELASTNAME, 
                        src:ONSITEMOBILE::varchar AS ONSITEMOBILE, 
                        src:ONSITEPHONENUMBER::varchar AS ONSITEPHONENUMBER, 
                        src:ONSITEPOSTALCODE::varchar AS ONSITEPOSTALCODE, 
                        src:ONSITESUBURB::varchar AS ONSITESUBURB, 
                        src:ONSITETITLE::varchar AS ONSITETITLE, 
                        src:PHONENUMBER::varchar AS PHONENUMBER, 
                        src:SUBURB::varchar AS SUBURB, 
                        src:TITLE::varchar AS TITLE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XAPPLICANTDPKEY::integer AS XAPPLICANTDPKEY, 
                        src:ZIP::varchar AS ZIP, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XAPPLICANTDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XAPPLICANTDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGAPPLDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XAPPLICANTDPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null