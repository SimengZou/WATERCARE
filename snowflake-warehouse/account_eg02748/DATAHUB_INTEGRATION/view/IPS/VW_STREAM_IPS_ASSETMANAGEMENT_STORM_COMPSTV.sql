CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_STORM_COMPSTV AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIR::varchar AS DIR, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:EXPECTLIFE::integer AS EXPECTLIFE, 
                        src:EXPECTLIFEUOM::varchar AS EXPECTLIFEUOM, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:INTKEY::integer AS INTKEY, 
                        src:INVERTELEV::numeric(38, 10) AS INVERTELEV, 
                        src:INVERTELEVUOM::varchar AS INVERTELEVUOM, 
                        src:LOC::varchar AS LOC, 
                        src:MAINKEY::integer AS MAINKEY, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MFGDATE::datetime AS MFGDATE, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:MTBF::integer AS MTBF, 
                        src:MTBFUOM::varchar AS MTBFUOM, 
                        src:NOTURNS::varchar AS NOTURNS, 
                        src:OBST::varchar AS OBST, 
                        src:OPERDPTH::numeric(38, 10) AS OPERDPTH, 
                        src:OPERDPTHUOM::varchar AS OPERDPTHUOM, 
                        src:OWN::varchar AS OWN, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PURCDATE::datetime AS PURCDATE, 
                        src:RIMELEV::numeric(38, 10) AS RIMELEV, 
                        src:RIMELEVUOM::varchar AS RIMELEVUOM, 
                        src:SEGKEY::integer AS SEGKEY, 
                        src:SERNO::varchar AS SERNO, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:SLKEY::integer AS SLKEY, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:USGAREAKEY::integer AS USGAREAKEY, 
                        src:USGTOT::numeric(38, 10) AS USGTOT, 
                        src:USGTOTUOM::varchar AS USGTOTUOM, 
                        src:VALVESIZE::numeric(38, 10) AS VALVESIZE, 
                        src:VALVESIZEUOM::varchar AS VALVESIZEUOM, 
                        src:VALVESTAT::varchar AS VALVESTAT, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XCOORD::numeric(38, 10) AS XCOORD, 
                        src:YCOORD::numeric(38, 10) AS YCOORD, 
                        src:ZCOORD::numeric(38, 10) AS ZCOORD, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:COMPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_STORM_COMPSTV as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPLEXKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPECTLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INVERTELEV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAINKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MTBF), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OPERDPTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:POSITION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RIMELEV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VALVESIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null