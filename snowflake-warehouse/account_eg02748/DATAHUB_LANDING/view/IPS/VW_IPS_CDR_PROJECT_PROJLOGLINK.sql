CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_PROJECT_PROJLOGLINK AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ALLOWCOMMENTS::varchar AS ALLOWCOMMENTS, 
                        src:APPLICATIONTYPE::integer AS APPLICATIONTYPE, 
                        src:DELETED::boolean AS DELETED, 
                        src:ENABLED::varchar AS ENABLED, 
                        src:ISINCUSTOMERDETAILS::varchar AS ISINCUSTOMERDETAILS, 
                        src:ISINMYPROJECTS::varchar AS ISINMYPROJECTS, 
                        src:ISINPUBLICDETAILS::varchar AS ISINPUBLICDETAILS, 
                        src:LINKTEXT::varchar AS LINKTEXT, 
                        src:LOGLINKKEY::integer AS LOGLINKKEY, 
                        src:LOGTYPECODE::varchar AS LOGTYPECODE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:STATUSCODE::varchar AS STATUSCODE, 
                        src:TABORDER::integer AS TABORDER, 
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
    
                        
                src:LOGLINKKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LOGLINKKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_PROJECT_PROJLOGLINK as strm))