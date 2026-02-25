CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_FLEET_COMPVEH AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ADDRQUAL::varchar AS ADDRQUAL, 
                        src:AREA::varchar AS AREA, 
                        src:AVGMONUSG::numeric(38, 10) AS AVGMONUSG, 
                        src:AVGMONUSGUOM::varchar AS AVGMONUSGUOM, 
                        src:AXLE::varchar AS AXLE, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPLEXKEY::integer AS COMPLEXKEY, 
                        src:CYLINDERS::integer AS CYLINDERS, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISTRICT::varchar AS DISTRICT, 
                        src:DRIVERCLAS::varchar AS DRIVERCLAS, 
                        src:ENGINENO::varchar AS ENGINENO, 
                        src:ENGINETYPE::varchar AS ENGINETYPE, 
                        src:EXPBY::varchar AS EXPBY, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:EXPECTLIFE::integer AS EXPECTLIFE, 
                        src:EXPECTLIFEUOM::varchar AS EXPECTLIFEUOM, 
                        src:GVW::varchar AS GVW, 
                        src:GVWUOM::varchar AS GVWUOM, 
                        src:INSTDATE::datetime AS INSTDATE, 
                        src:INTKEY::integer AS INTKEY, 
                        src:LICENSE::varchar AS LICENSE, 
                        src:LOC::varchar AS LOC, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MFGDATE::datetime AS MFGDATE, 
                        src:MFGKEY::integer AS MFGKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:MODELYR::varchar AS MODELYR, 
                        src:MTBF::integer AS MTBF, 
                        src:NOTIRES::integer AS NOTIRES, 
                        src:OWN::varchar AS OWN, 
                        src:POSITION::integer AS POSITION, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PURCCOST::numeric(38, 10) AS PURCCOST, 
                        src:PURCDATE::datetime AS PURCDATE, 
                        src:SERNO::varchar AS SERNO, 
                        src:SERVSTAT::varchar AS SERVSTAT, 
                        src:SITEKEY::integer AS SITEKEY, 
                        src:SUBAREA::varchar AS SUBAREA, 
                        src:TIRESIZE::varchar AS TIRESIZE, 
                        src:TIRETYPE::varchar AS TIRETYPE, 
                        src:TRANSMISSN::varchar AS TRANSMISSN, 
                        src:UNITDESC::varchar AS UNITDESC, 
                        src:UNITID::varchar AS UNITID, 
                        src:UNITTYPE::varchar AS UNITTYPE, 
                        src:USGAREAKEY::integer AS USGAREAKEY, 
                        src:USGDATE::datetime AS USGDATE, 
                        src:USGTOT::numeric(38, 10) AS USGTOT, 
                        src:USGTOTUOM::varchar AS USGTOTUOM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VEHSZ::varchar AS VEHSZ, 
                        src:VEHSZUOM::varchar AS VEHSZUOM, 
                        src:VIN::varchar AS VIN, 
                        src:WARREXP::datetime AS WARREXP, 
                        src:WARRUSG::numeric(38, 10) AS WARRUSG, 
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
                FROM DATAHUB_INTEGRATION.STREAM_IPS_ASSETMANAGEMENT_FLEET_COMPVEH as strm)
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
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CYLINDERS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXPECTLIFE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INSTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MFGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MFGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MTBF), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NOTIRES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:POSITION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PURCCOST), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PURCDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SITEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGAREAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:USGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USGTOT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARREXP), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WARRUSG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORD), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null