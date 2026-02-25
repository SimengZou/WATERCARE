CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_AGENCYDEFINED_COMPAGENCYSIMPLE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AGENCYCHARACTER1::varchar AS AGENCYCHARACTER1, 
                        src:AGENCYCHARACTER2::varchar AS AGENCYCHARACTER2, 
                        src:AGENCYCODE1::varchar AS AGENCYCODE1, 
                        src:AGENCYCODE2::varchar AS AGENCYCODE2, 
                        src:AGENCYDATE1::datetime AS AGENCYDATE1, 
                        src:AGENCYDATE2::datetime AS AGENCYDATE2, 
                        src:AGENCYFLAG1::varchar AS AGENCYFLAG1, 
                        src:AGENCYFLAG2::varchar AS AGENCYFLAG2, 
                        src:AGENCYINTEGER1::integer AS AGENCYINTEGER1, 
                        src:AGENCYINTEGER2::integer AS AGENCYINTEGER2, 
                        src:AREA::varchar AS AREA, 
                        src:ASBLT::varchar AS ASBLT, 
                        src:AVGMONUSG::numeric(38, 10) AS AVGMONUSG, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COLOR::varchar AS COLOR, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPTYPE::integer AS COMPTYPE, 
                        src:COND::varchar AS COND, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIAM::numeric(38, 10) AS DIAM, 
                        src:DIAMUOM::varchar AS DIAMUOM, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:DPTH::numeric(38, 10) AS DPTH, 
                        src:DPTHUOM::varchar AS DPTHUOM, 
                        src:ELEV::numeric(38, 10) AS ELEV, 
                        src:ELEVUOM::varchar AS ELEVUOM, 
                        src:EXPECTEDLIFE::integer AS EXPECTEDLIFE, 
                        src:EXPECTEDLIFEUOM::varchar AS EXPECTEDLIFEUOM, 
                        src:GISSTATIC::integer AS GISSTATIC, 
                        src:HT::numeric(38, 10) AS HT, 
                        src:HTUOM::varchar AS HTUOM, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:LEN::numeric(38, 10) AS LEN, 
                        src:LENUOM::varchar AS LENUOM, 
                        src:LOC::varchar AS LOC, 
                        src:MATL::varchar AS MATL, 
                        src:MFGDATE::datetime AS MFGDATE, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:OWN::varchar AS OWN, 
                        src:POSITION::integer AS POSITION, 
                        src:PURCCOST::numeric(38, 10) AS PURCCOST, 
                        src:PURCDATE::datetime AS PURCDATE, 
                        src:SERNO::varchar AS SERNO, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:SUBUNITOF::integer AS SUBUNITOF, 
                        src:SURF::varchar AS SURF, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
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
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_AGENCYDEFINED_COMPAGENCYSIMPLE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGENCYDATE1), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGENCYDATE2), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGENCYINTEGER1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGENCYINTEGER2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AVGMONUSG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DPTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ELEV), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPECTEDLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GISSTATIC), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LEN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:POSITION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PURCCOST), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SUBUNITOF), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null