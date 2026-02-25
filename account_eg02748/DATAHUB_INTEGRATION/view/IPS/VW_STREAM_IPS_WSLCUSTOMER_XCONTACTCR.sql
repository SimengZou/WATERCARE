CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCUSTOMER_XCONTACTCR AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CNTCTKEY::integer AS CNTCTKEY, 
                        src:COUNTRY::varchar AS COUNTRY, 
                        src:DELETED::boolean AS DELETED, 
                        src:ISRECORDUPDATED::varchar AS ISRECORDUPDATED, 
                        src:LANDLINE1::varchar AS LANDLINE1, 
                        src:LANDLINE1AREA::varchar AS LANDLINE1AREA, 
                        src:LANDLINE1NO::varchar AS LANDLINE1NO, 
                        src:LANDLINE1TYPE::varchar AS LANDLINE1TYPE, 
                        src:LANDLINE2::varchar AS LANDLINE2, 
                        src:LANDLINE2AREA::varchar AS LANDLINE2AREA, 
                        src:LANDLINE2NO::varchar AS LANDLINE2NO, 
                        src:LANDLINE2TYPE::varchar AS LANDLINE2TYPE, 
                        src:MOBILE::varchar AS MOBILE, 
                        src:MOBILE1AREA::varchar AS MOBILE1AREA, 
                        src:MOBILE1NO::varchar AS MOBILE1NO, 
                        src:MOBILE2::varchar AS MOBILE2, 
                        src:MOBILE2AREA::varchar AS MOBILE2AREA, 
                        src:MOBILE2NO::varchar AS MOBILE2NO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OVERSEA::varchar AS OVERSEA, 
                        src:OVERSEAAREA::varchar AS OVERSEAAREA, 
                        src:OVERSEANO::varchar AS OVERSEANO, 
                        src:POSTCODE::varchar AS POSTCODE, 
                        src:PRIMARYEMAILFLAG::varchar AS PRIMARYEMAILFLAG, 
                        src:PRIMARYMOBILEFLAG::varchar AS PRIMARYMOBILEFLAG, 
                        src:TOLLFREE::varchar AS TOLLFREE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XCONTACTCRKEY::integer AS XCONTACTCRKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XCONTACTCRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCUSTOMER_XCONTACTCR as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CNTCTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCONTACTCRKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null