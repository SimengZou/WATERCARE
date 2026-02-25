CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_USE_USEREVIEW AS SELECT
                        src:ACTLTM::numeric(38, 10) AS ACTLTM, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APUSEKEY::integer AS APUSEKEY, 
                        src:APUSEREVIEWKEY::integer AS APUSEREVIEWKEY, 
                        src:APUSEREVIEWTYPEKEY::integer AS APUSEREVIEWTYPEKEY, 
                        src:ASSIGNED::varchar AS ASSIGNED, 
                        src:ASSIGNTO::varchar AS ASSIGNTO, 
                        src:ASSIGNTOPROVIDER::integer AS ASSIGNTOPROVIDER, 
                        src:CMPTRGEN::varchar AS CMPTRGEN, 
                        src:COMP::varchar AS COMP, 
                        src:COMPBY::varchar AS COMPBY, 
                        src:COMPBYPROVIDER::integer AS COMPBYPROVIDER, 
                        src:COMPDTTM::datetime AS COMPDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEPT::varchar AS DEPT, 
                        src:ISSBY::varchar AS ISSBY, 
                        src:ISSDTTM::datetime AS ISSDTTM, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:RELEVANTREVIEW::varchar AS RELEVANTREVIEW, 
                        src:RESULT::varchar AS RESULT, 
                        src:RESULTBY::varchar AS RESULTBY, 
                        src:RESULTBYPROVIDER::integer AS RESULTBYPROVIDER, 
                        src:RESULTDTTM::datetime AS RESULTDTTM, 
                        src:RESULTGEN::varchar AS RESULTGEN, 
                        src:SCHEDBY::varchar AS SCHEDBY, 
                        src:SCHEDDTTM::datetime AS SCHEDDTTM, 
                        src:STARTBY::varchar AS STARTBY, 
                        src:STARTBYPROVIDER::integer AS STARTBYPROVIDER, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STARTED::varchar AS STARTED, 
                        src:SUSPDT::datetime AS SUSPDT, 
                        src:TYPENO::integer AS TYPENO, 
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
                                        
                src:APUSEREVIEWKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_USE_USEREVIEW as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTLTM), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEREVIEWKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APUSEREVIEWTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSIGNTOPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPBYPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ISSDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESULTBYPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESULTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STARTBYPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SUSPDT), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TYPENO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null