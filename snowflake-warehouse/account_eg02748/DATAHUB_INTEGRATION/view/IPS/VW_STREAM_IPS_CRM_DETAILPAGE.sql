CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CRM_DETAILPAGE AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:DETAILPAGEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CRM_DETAILPAGE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCESSIDADD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCESSIDUPDATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCESSIDVIEW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DETAILPAGEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPLAY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISPLAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EVENTHANDLER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PORTALCUSTOMERCONSTRAINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PORTALDETAILPAGEAREA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PORTALDETAILPAGEPLACEMENT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PORTALPUBLICCONSTRAINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null