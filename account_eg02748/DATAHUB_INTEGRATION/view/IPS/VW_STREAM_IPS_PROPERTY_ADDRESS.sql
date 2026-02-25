CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_ADDRESS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESSSTATUS::varchar AS ADDRESSSTATUS, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ALLOWDUPLICATESERVICES::varchar AS ALLOWDUPLICATESERVICES, 
                        src:AREA::varchar AS AREA, 
                        src:BLOCK::varchar AS BLOCK, 
                        src:CASSISVALID::varchar AS CASSISVALID, 
                        src:CASSVALIDATIONDESC::varchar AS CASSVALIDATIONDESC, 
                        src:CASSVALIDATIONDT::datetime AS CASSVALIDATIONDT, 
                        src:CASSVALIDATIONSTATUS::varchar AS CASSVALIDATIONSTATUS, 
                        src:CITYLIMITS::varchar AS CITYLIMITS, 
                        src:CURRADDR::varchar AS CURRADDR, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:FULLSTNO::varchar AS FULLSTNO, 
                        src:GPSX::numeric(38, 10) AS GPSX, 
                        src:GPSY::numeric(38, 10) AS GPSY, 
                        src:GPSZ::numeric(38, 10) AS GPSZ, 
                        src:INOUTSERVICEAREA::varchar AS INOUTSERVICEAREA, 
                        src:LEGALOWNER::varchar AS LEGALOWNER, 
                        src:LOT::varchar AS LOT, 
                        src:MANAGEMENTGROUP::integer AS MANAGEMENTGROUP, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NZADDRTYPE::varchar AS NZADDRTYPE, 
                        src:NZCOMPDIR::varchar AS NZCOMPDIR, 
                        src:NZFLAT::varchar AS NZFLAT, 
                        src:NZHNOHI::varchar AS NZHNOHI, 
                        src:NZHNOSORTAS::integer AS NZHNOSORTAS, 
                        src:NZHNOSORTASHI::integer AS NZHNOSORTASHI, 
                        src:NZHOUSENO::varchar AS NZHOUSENO, 
                        src:NZPOSTCODE::varchar AS NZPOSTCODE, 
                        src:NZST2NAME::varchar AS NZST2NAME, 
                        src:NZST2TYPE::varchar AS NZST2TYPE, 
                        src:NZST3NAME::varchar AS NZST3NAME, 
                        src:NZST3TYPE::varchar AS NZST3TYPE, 
                        src:NZSTATE::varchar AS NZSTATE, 
                        src:NZSTNAME::varchar AS NZSTNAME, 
                        src:NZSTTYPE::varchar AS NZSTTYPE, 
                        src:NZSUBURB::varchar AS NZSUBURB, 
                        src:OPTA::varchar AS OPTA, 
                        src:OPTB::varchar AS OPTB, 
                        src:OPTC::varchar AS OPTC, 
                        src:OPTD::integer AS OPTD, 
                        src:PROPERTYUSE::varchar AS PROPERTYUSE, 
                        src:SUBDIVCODE::varchar AS SUBDIVCODE, 
                        src:SUBDIVDESC::varchar AS SUBDIVDESC, 
                        src:TOWNSHIP::varchar AS TOWNSHIP, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VERSION::integer AS VERSION, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:ADDRKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_PROPERTY_ADDRESS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CASSVALIDATIONDT), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GPSX), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GPSY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GPSZ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MANAGEMENTGROUP), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NZHNOSORTAS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NZHNOSORTASHI), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPTD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VERSION), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null