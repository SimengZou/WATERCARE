CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCUSTOMER_XCONTACTCR AS SELECT
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
                        src:XCONTACTCRKEY::integer AS XCONTACTCRKEY, 
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
    
                        
                src:XCONTACTCRKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XCONTACTCRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCUSTOMER_XCONTACTCR as strm))