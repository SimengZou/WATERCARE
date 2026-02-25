CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSCONFIGURED_XMECHROTATE AS SELECT
                        src:ACCELDEPREC::varchar AS ACCELDEPREC, 
                        src:ACCELDEPRECDESC::varchar AS ACCELDEPRECDESC, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BEARINGTYPE::varchar AS BEARINGTYPE, 
                        src:BODYTYPE::varchar AS BODYTYPE, 
                        src:BUSINESSAREA::varchar AS BUSINESSAREA, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CONFINEDSPACE::varchar AS CONFINEDSPACE, 
                        src:COOLING::varchar AS COOLING, 
                        src:COSTCENTRE::varchar AS COSTCENTRE, 
                        src:CRITICALITY::varchar AS CRITICALITY, 
                        src:CURRENTIN::numeric(38, 10) AS CURRENTIN, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DESIGNSPEED::numeric(38, 10) AS DESIGNSPEED, 
                        src:DIAMINLET::numeric(38, 10) AS DIAMINLET, 
                        src:DIAMOUTLET::numeric(38, 10) AS DIAMOUTLET, 
                        src:DISPOSALDESC::varchar AS DISPOSALDESC, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:DRAWINGNOREF::varchar AS DRAWINGNOREF, 
                        src:ENERGYRATING::integer AS ENERGYRATING, 
                        src:EXTCOAT::varchar AS EXTCOAT, 
                        src:FORDISPOSAL::varchar AS FORDISPOSAL, 
                        src:FRAMESIZE::varchar AS FRAMESIZE, 
                        src:FUELTYPE::varchar AS FUELTYPE, 
                        src:FUNCAREA::varchar AS FUNCAREA, 
                        src:FUNCOUPUT::varchar AS FUNCOUPUT, 
                        src:HAZARDRATING::varchar AS HAZARDRATING, 
                        src:IMPELLORDIA::numeric(38, 10) AS IMPELLORDIA, 
                        src:IMPELLORTYPE::varchar AS IMPELLORTYPE, 
                        src:INSTALLMOUNT::varchar AS INSTALLMOUNT, 
                        src:INSTALLORIENT::varchar AS INSTALLORIENT, 
                        src:INSULATIONCLASS::varchar AS INSULATIONCLASS, 
                        src:INTLINING::varchar AS INTLINING, 
                        src:IPRATING::varchar AS IPRATING, 
                        src:LINKEDASSET::varchar AS LINKEDASSET, 
                        src:LOCALITY::varchar AS LOCALITY, 
                        src:MAKE::varchar AS MAKE, 
                        src:MAXFLOW::numeric(38, 10) AS MAXFLOW, 
                        src:MEDIATYPE::varchar AS MEDIATYPE, 
                        src:MINFLOW::numeric(38, 10) AS MINFLOW, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:PHASES::varchar AS PHASES, 
                        src:PHOTO3D::varchar AS PHOTO3D, 
                        src:PRESSURERATING::numeric(38, 10) AS PRESSURERATING, 
                        src:PROJECTNO::varchar AS PROJECTNO, 
                        src:PROJECTWISE::varchar AS PROJECTWISE, 
                        src:RELDATE::varchar AS RELDATE, 
                        src:RELDEPTH::varchar AS RELDEPTH, 
                        src:RELDIA::varchar AS RELDIA, 
                        src:RELGEOSP::varchar AS RELGEOSP, 
                        src:RELMATL::varchar AS RELMATL, 
                        src:RELOWN::varchar AS RELOWN, 
                        src:RELSTATUS::varchar AS RELSTATUS, 
                        src:SAFETYCRITICAL::varchar AS SAFETYCRITICAL, 
                        src:STAGES::integer AS STAGES, 
                        src:SUBTYPEFEATURES::varchar AS SUBTYPEFEATURES, 
                        src:TORQUE::numeric(38, 10) AS TORQUE, 
                        src:TORQUEOUT::numeric(38, 10) AS TORQUEOUT, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOLTAGEIN::numeric(38, 10) AS VOLTAGEIN, 
                        src:VOLTAGEINTYPE::varchar AS VOLTAGEINTYPE, 
                        src:VOLTAGEOUT::numeric(38, 10) AS VOLTAGEOUT, 
                        src:VOLTAGEOUTTYPE::varchar AS VOLTAGEOUTTYPE, 
                        src:WARRANTYEND::datetime AS WARRANTYEND, 
                        src:WARRANTYSTART::datetime AS WARRANTYSTART, 
                        src:XMECHROTATEKEY::integer AS XMECHROTATEKEY, 
                        src:ZONECATCHMENT::varchar AS ZONECATCHMENT, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XMECHROTATEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETSCONFIGURED_XMECHROTATE as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DESIGNSPEED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMINLET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMOUTLET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENERGYRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPELLORDIA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRESSURERATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STAGES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TORQUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TORQUEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XMECHROTATEKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null