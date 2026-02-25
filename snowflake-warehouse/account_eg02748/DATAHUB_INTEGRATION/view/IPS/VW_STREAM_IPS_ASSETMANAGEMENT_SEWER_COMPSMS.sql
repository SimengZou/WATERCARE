CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPSMS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:AVGMONUSG::numeric(38, 10) AS AVGMONUSG, 
                        src:AVGMONUSGUOM::varchar AS AVGMONUSGUOM, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COLOR::varchar AS COLOR, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:COND::varchar AS COND, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIAM::numeric(38, 10) AS DIAM, 
                        src:DIAMUOM::varchar AS DIAMUOM, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:DPTH::numeric(38, 10) AS DPTH, 
                        src:DPTHUOM::varchar AS DPTHUOM, 
                        src:ELEV::numeric(38, 10) AS ELEV, 
                        src:ELEVUOM::varchar AS ELEVUOM, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:HT::numeric(38, 10) AS HT, 
                        src:HTUOM::varchar AS HTUOM, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:INTKEY::integer AS INTKEY, 
                        src:LEN::numeric(38, 10) AS LEN, 
                        src:LENUOM::varchar AS LENUOM, 
                        src:LOC::varchar AS LOC, 
                        src:MAINKEY::integer AS MAINKEY, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MATL::varchar AS MATL, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MISCDESC::varchar AS MISCDESC, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:OWN::varchar AS OWN, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:SEGKEY::integer AS SEGKEY, 
                        src:SERNO::varchar AS SERNO, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:SLKEY::integer AS SLKEY, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:SURF::varchar AS SURF, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:USGAREAKEY::integer AS USGAREAKEY, 
                        src:USGDATE::datetime AS USGDATE, 
                        src:USGTOT::numeric(38, 10) AS USGTOT, 
                        src:USGTOTUOM::varchar AS USGTOTUOM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WID::numeric(38, 10) AS WID, 
                        src:WIDUOM::varchar AS WIDUOM, 
                        src:WT::numeric(38, 10) AS WT, 
                        src:WTUOM::varchar AS WTUOM, 
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
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPSMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVGMONUSG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPLEXKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DPTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ELEV), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LEN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAINKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:POSITION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null