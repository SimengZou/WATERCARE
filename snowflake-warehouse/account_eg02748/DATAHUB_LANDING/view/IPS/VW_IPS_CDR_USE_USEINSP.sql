CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_USE_USEINSP AS SELECT
                        src:ACTVD::varchar AS ACTVD, 
                        src:ACTVDBY::varchar AS ACTVDBY, 
                        src:ACTVDDTTM::datetime AS ACTVDDTTM, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APUSEINSPKEY::integer AS APUSEINSPKEY, 
                        src:APUSEINSPTYPEKEY::integer AS APUSEINSPTYPEKEY, 
                        src:APUSEKEY::integer AS APUSEKEY, 
                        src:ASSIGNED::varchar AS ASSIGNED, 
                        src:ASSIGNTO::varchar AS ASSIGNTO, 
                        src:ASSIGNTOPROVIDER::integer AS ASSIGNTOPROVIDER, 
                        src:CALLBY::varchar AS CALLBY, 
                        src:CALLDTTM::datetime AS CALLDTTM, 
                        src:CMPTRGEN::varchar AS CMPTRGEN, 
                        src:CNTCTINFO::varchar AS CNTCTINFO, 
                        src:CNTCTPERSON::varchar AS CNTCTPERSON, 
                        src:COMPDTTM::datetime AS COMPDTTM, 
                        src:DELETED::boolean AS DELETED, 
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
    
                        
                src:APUSEINSPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:APUSEINSPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_USE_USEINSP as strm))