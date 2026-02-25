CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSSEWER_XSEWERMISCELLANEOUS AS SELECT
                        src:ACCELDEPREC::varchar AS ACCELDEPREC, 
                        src:ACCELDEPRECDESC::varchar AS ACCELDEPRECDESC, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AREA::numeric(38, 10) AS AREA, 
                        src:BEARINGTYPE::varchar AS BEARINGTYPE, 
                        src:BODYTYPE::varchar AS BODYTYPE, 
                        src:BOLTDOWN::varchar AS BOLTDOWN, 
                        src:BUSINESSAREA::varchar AS BUSINESSAREA, 
                        src:CAPACITY::numeric(38, 10) AS CAPACITY, 
                        src:COMMSPROTOCOL::varchar AS COMMSPROTOCOL, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CONFINEDSPACE::varchar AS CONFINEDSPACE, 
                        src:CONSTMETHOD::varchar AS CONSTMETHOD, 
                        src:COOLING::varchar AS COOLING, 
                        src:COSTCENTRE::varchar AS COSTCENTRE, 
                        src:COVERTYPE::varchar AS COVERTYPE, 
                        src:CRITICALITY::varchar AS CRITICALITY, 
                        src:CURRENTIN::numeric(38, 10) AS CURRENTIN, 
                        src:CURRENTOUT::numeric(38, 10) AS CURRENTOUT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DESIGNRESILIENCE::varchar AS DESIGNRESILIENCE, 
                        src:DESIGNSPEED::numeric(38, 10) AS DESIGNSPEED, 
                        src:DIAMEXT::numeric(38, 10) AS DIAMEXT, 
                        src:DIAMINLET::numeric(38, 10) AS DIAMINLET, 
                        src:DIAMINT::numeric(38, 10) AS DIAMINT, 
                        src:DIAMOUTLET::numeric(38, 10) AS DIAMOUTLET, 
                        src:DISCHARGECAP::numeric(38, 10) AS DISCHARGECAP, 
                        src:DISPOSALDESC::varchar AS DISPOSALDESC, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:DRAWINGNOREF::varchar AS DRAWINGNOREF, 
                        src:DROPMH::varchar AS DROPMH, 
                        src:ENERGYRATING::integer AS ENERGYRATING, 
                        src:EXTCOAT::varchar AS EXTCOAT, 
                        src:FALLPROTECT::varchar AS FALLPROTECT, 
                        src:FORDISPOSAL::varchar AS FORDISPOSAL, 
                        src:FRAMESIZE::varchar AS FRAMESIZE, 
                        src:FUELTYPE::varchar AS FUELTYPE, 
                        src:FUNCAREA::varchar AS FUNCAREA, 
                        src:FUNCOUPUT::varchar AS FUNCOUPUT, 
                        src:GRDLEVEL::numeric(38, 10) AS GRDLEVEL, 
                        src:HAZARDRATING::varchar AS HAZARDRATING, 
                        src:HINGED::varchar AS HINGED, 
                        src:IMPELLORDIA::numeric(38, 10) AS IMPELLORDIA, 
                        src:IMPELLORTYPE::varchar AS IMPELLORTYPE, 
                        src:INHIBITLEVEL::numeric(38, 10) AS INHIBITLEVEL, 
                        src:INSTALLMOUNT::varchar AS INSTALLMOUNT, 
                        src:INSTALLORIENT::varchar AS INSTALLORIENT, 
                        src:INSTRUMENTRANGE::varchar AS INSTRUMENTRANGE, 
                        src:INSULATIONCLASS::varchar AS INSULATIONCLASS, 
                        src:INTLINING::varchar AS INTLINING, 
                        src:INVLEVEL::numeric(38, 10) AS INVLEVEL, 
                        src:IPRATING::varchar AS IPRATING, 
                        src:JTTYPE::varchar AS JTTYPE, 
                        src:LIDLEVEL::numeric(38, 10) AS LIDLEVEL, 
                        src:LINKEDASSET::varchar AS LINKEDASSET, 
                        src:LOADRATING::numeric(38, 10) AS LOADRATING, 
                        src:LOCALITY::varchar AS LOCALITY, 
                        src:LOCKABLE::varchar AS LOCKABLE, 
                        src:MAKE::varchar AS MAKE, 
                        src:MAXFLOW::numeric(38, 10) AS MAXFLOW, 
                        src:MEDIATYPE::varchar AS MEDIATYPE, 
                        src:MINFLOW::numeric(38, 10) AS MINFLOW, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:OVERFLOWLEVEL::numeric(38, 10) AS OVERFLOWLEVEL, 
                        src:OVERFLOWTYPE::varchar AS OVERFLOWTYPE, 
                        src:PHASES::varchar AS PHASES, 
                        src:PHOTO3D::varchar AS PHOTO3D, 
                        src:PRESSURERATING::numeric(38, 10) AS PRESSURERATING, 
                        src:PROJECTNO::varchar AS PROJECTNO, 
                        src:PROJECTWISE::varchar AS PROJECTWISE, 
                        src:QUAKEDESIGN::varchar AS QUAKEDESIGN, 
                        src:RELDATE::varchar AS RELDATE, 
                        src:RELDEPTH::varchar AS RELDEPTH, 
                        src:RELDIA::varchar AS RELDIA, 
                        src:RELGEOSP::varchar AS RELGEOSP, 
                        src:RELMATL::varchar AS RELMATL, 
                        src:RELOWN::varchar AS RELOWN, 
                        src:RELSTATUS::varchar AS RELSTATUS, 
                        src:REMOVABLE::varchar AS REMOVABLE, 
                        src:SAFETYCRITICAL::varchar AS SAFETYCRITICAL, 
                        src:SPILLWAYTYPE::varchar AS SPILLWAYTYPE, 
                        src:STAGES::integer AS STAGES, 
                        src:STFCABINET::varchar AS STFCABINET, 
                        src:STFFAN::varchar AS STFFAN, 
                        src:STFFILTER::varchar AS STFFILTER, 
                        src:STFHEATER::varchar AS STFHEATER, 
                        src:STFLIGHTARRESTOR::varchar AS STFLIGHTARRESTOR, 
                        src:STFREDUNDANCY::varchar AS STFREDUNDANCY, 
                        src:STFSTANDBYUNIT::varchar AS STFSTANDBYUNIT, 
                        src:STFTEMP::varchar AS STFTEMP, 
                        src:STFTRANSCEIVER::varchar AS STFTRANSCEIVER, 
                        src:STFVENT::varchar AS STFVENT, 
                        src:STIFFRATING::numeric(38, 10) AS STIFFRATING, 
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
                        src:XSEWERMISCELLANEOUSKEY::integer AS XSEWERMISCELLANEOUSKEY, 
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
                                        
                src:XSEWERMISCELLANEOUSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETSSEWER_XSEWERMISCELLANEOUS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AREA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CAPACITY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTOUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DESIGNSPEED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMEXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMINLET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMOUTLET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DISCHARGECAP), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENERGYRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRDLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:IMPELLORDIA), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INHIBITLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INVLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LIDLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LOADRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OVERFLOWLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRESSURERATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STAGES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STIFFRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TORQUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TORQUEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XSEWERMISCELLANEOUSKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null