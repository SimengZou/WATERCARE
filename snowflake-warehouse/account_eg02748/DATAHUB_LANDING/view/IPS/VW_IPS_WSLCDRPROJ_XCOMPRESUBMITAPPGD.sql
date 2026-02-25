CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XCOMPRESUBMITAPPGD AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJPROBKEY::integer AS APPROJPROBKEY, 
                        src:APPROJREVIEWKEY::integer AS APPROJREVIEWKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ONHOLDCODE::varchar AS ONHOLDCODE, 
                        src:ONHOLDCOMMENTS::varchar AS ONHOLDCOMMENTS, 
                        src:ONHOLDDESCRIPTION::varchar AS ONHOLDDESCRIPTION, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XCOMPRESUBMITAPPDPKEY::integer AS XCOMPRESUBMITAPPDPKEY, 
                        src:XCOMPRESUBMITAPPGDKEY::integer AS XCOMPRESUBMITAPPGDKEY, 
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
    
                        
                src:XCOMPRESUBMITAPPGDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XCOMPRESUBMITAPPGDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XCOMPRESUBMITAPPGD as strm))