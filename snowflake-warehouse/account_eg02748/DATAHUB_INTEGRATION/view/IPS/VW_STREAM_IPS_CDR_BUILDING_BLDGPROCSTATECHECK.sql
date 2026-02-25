CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CDR_BUILDING_BLDGPROCSTATECHECK AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APBLDGPROCESSSTATEKEY::integer AS APBLDGPROCESSSTATEKEY, 
                        src:APBLDGPROCSTATECHKKEY::integer AS APBLDGPROCSTATECHKKEY, 
                        src:CHECKFRMLA::integer AS CHECKFRMLA, 
                        src:CHECKTITLE::varchar AS CHECKTITLE, 
                        src:CHECKTYPE::integer AS CHECKTYPE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DPDESC::varchar AS DPDESC, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXCLUDEFROMPENDINGPORTAL::varchar AS EXCLUDEFROMPENDINGPORTAL, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:INTERNALONLYFLAG::varchar AS INTERNALONLYFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PORTALDESCRIPTION::varchar AS PORTALDESCRIPTION, 
                        src:SHOWINPORTAL::varchar AS SHOWINPORTAL, 
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
                                        
                src:APBLDGPROCSTATECHKKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CDR_BUILDING_BLDGPROCSTATECHECK as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGPROCESSSTATEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGPROCSTATECHKKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CHECKFRMLA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CHECKTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null