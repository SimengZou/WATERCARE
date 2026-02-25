CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CRM_DETAILPAGE AS SELECT
                        src:ACCESSIDADD::integer AS ACCESSIDADD, 
                        src:ACCESSIDUPDATE::integer AS ACCESSIDUPDATE, 
                        src:ACCESSIDVIEW::integer AS ACCESSIDVIEW, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CLASSDESC::varchar AS CLASSDESC, 
                        src:CLASSNAME::varchar AS CLASSNAME, 
                        src:DELETED::boolean AS DELETED, 
                        src:DETAILPAGEKEY::integer AS DETAILPAGEKEY, 
                        src:DISPLAY::integer AS DISPLAY, 
                        src:DISPLAYONLOAD::varchar AS DISPLAYONLOAD, 
                        src:DISPLAYORDER::integer AS DISPLAYORDER, 
                        src:EFFECTIVEDATE::datetime AS EFFECTIVEDATE, 
                        src:EVENTHANDLER::integer AS EVENTHANDLER, 
                        src:EXPIREDATE::datetime AS EXPIREDATE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PORTALCUSTOMERCONSTRAINT::integer AS PORTALCUSTOMERCONSTRAINT, 
                        src:PORTALDETAILPAGEAREA::integer AS PORTALDETAILPAGEAREA, 
                        src:PORTALDETAILPAGEPLACEMENT::integer AS PORTALDETAILPAGEPLACEMENT, 
                        src:PORTALPUBLICCONSTRAINT::integer AS PORTALPUBLICCONSTRAINT, 
                        src:TABPANEID::varchar AS TABPANEID, 
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
    
                        
                src:DETAILPAGEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DETAILPAGEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CRM_DETAILPAGE as strm))