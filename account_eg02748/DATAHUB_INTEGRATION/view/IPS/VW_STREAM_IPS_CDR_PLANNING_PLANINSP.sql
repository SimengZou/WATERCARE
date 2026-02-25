CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_PLANNING_PLANINSP AS SELECT
                        src:ACTVD::varchar AS ACTVD, 
                        src:ACTVDBY::varchar AS ACTVDBY, 
                        src:ACTVDDTTM::datetime AS ACTVDDTTM, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLANINSPKEY::integer AS APPLANINSPKEY, 
                        src:APPLANINSPTYPEKEY::integer AS APPLANINSPTYPEKEY, 
                        src:APPLANKEY::integer AS APPLANKEY, 
                        src:ASSIGNED::varchar AS ASSIGNED, 
                        src:ASSIGNTO::varchar AS ASSIGNTO, 
                        src:ASSIGNTOPROVIDER::integer AS ASSIGNTOPROVIDER, 
                        src:CALLBY::varchar AS CALLBY, 
                        src:CALLDTTM::datetime AS CALLDTTM, 
                        src:CMPTRGEN::varchar AS CMPTRGEN, 
                        src:CNTCTINFO::varchar AS CNTCTINFO, 
                        src:CNTCTPERSON::varchar AS CNTCTPERSON, 
                        src:COMPDTTM::datetime AS COMPDTTM, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:INSPBY::varchar AS INSPBY, 
                        src:INSPBYPROVIDER::integer AS INSPBYPROVIDER, 
                        src:INSPGEN::varchar AS INSPGEN, 
                        src:INSPHOURS::numeric(38, 10) AS INSPHOURS, 
                        src:INSTRIPKEY::integer AS INSTRIPKEY, 
                        src:ISPARTIAL::varchar AS ISPARTIAL, 
                        src:LOC::varchar AS LOC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ODOMSTART::integer AS ODOMSTART, 
                        src:ODOMSTOP::integer AS ODOMSTOP, 
                        src:ORD::integer AS ORD, 
                        src:PREFERENCE::varchar AS PREFERENCE, 
                        src:RELEVANTINSP::varchar AS RELEVANTINSP, 
                        src:REQUESTEDBY::integer AS REQUESTEDBY, 
                        src:RESULT::varchar AS RESULT, 
                        src:RESULTBY::varchar AS RESULTBY, 
                        src:RESULTBYPROVIDER::integer AS RESULTBYPROVIDER, 
                        src:RESULTDTTM::datetime AS RESULTDTTM, 
                        src:SCHED::varchar AS SCHED, 
                        src:SCHEDBY::varchar AS SCHEDBY, 
                        src:SCHEDDTTM::datetime AS SCHEDDTTM, 
                        src:SPECIALINSTR::varchar AS SPECIALINSTR, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STAT::integer AS STAT, 
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
                                        
                src:APPLANINSPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_PLANNING_PLANINSP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACTVDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLANINSPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLANINSPTYPEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPLANKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSIGNTOPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CALLDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPBYPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSPHOURS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INSTRIPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ODOMSTART), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ODOMSTOP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REQUESTEDBY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RESULTBYPROVIDER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESULTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STAT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TYPENO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null