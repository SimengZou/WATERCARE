CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_BUILDING_BLDGREVIEWPROB AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGPROBKEY::integer AS APBLDGPROBKEY, 
                        src:APBLDGREVIEWKEY::integer AS APBLDGREVIEWKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DOCUMENTID::varchar AS DOCUMENTID, 
                        src:DOCUMENTPROVIDERNAME::varchar AS DOCUMENTPROVIDERNAME, 
                        src:DOCUMENTTYPE::varchar AS DOCUMENTTYPE, 
                        src:EPLANREVIEWCOMMENTID::varchar AS EPLANREVIEWCOMMENTID, 
                        src:EPLANREVIEWMARKUPID::varchar AS EPLANREVIEWMARKUPID, 
                        src:EPLANREVIEWSOURCE::integer AS EPLANREVIEWSOURCE, 
                        src:FILENAME::varchar AS FILENAME, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAGEREFERENCE::varchar AS PAGEREFERENCE, 
                        src:PROBLEM::varchar AS PROBLEM, 
                        src:RECBY::varchar AS RECBY, 
                        src:RECDT::datetime AS RECDT, 
                        src:RECVER::varchar AS RECVER, 
                        src:RESBY::varchar AS RESBY, 
                        src:RESDT::datetime AS RESDT, 
                        src:RESOLVECODE::varchar AS RESOLVECODE, 
                        src:RESVER::varchar AS RESVER, 
                        src:REVIEWCYCLE::varchar AS REVIEWCYCLE, 
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
                                        
                src:APBLDGPROBKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGREVIEWPROB as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGPROBKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGREVIEWKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EPLANREVIEWSOURCE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RECDT), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESDT), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null