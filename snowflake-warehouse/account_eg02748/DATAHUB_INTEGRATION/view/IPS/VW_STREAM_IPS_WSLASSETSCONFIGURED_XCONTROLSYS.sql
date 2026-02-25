CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSCONFIGURED_XCONTROLSYS AS SELECT
                        src:ACCELDEPREC::varchar AS ACCELDEPREC, 
                        src:ACCELDEPRECDESC::varchar AS ACCELDEPRECDESC, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BUSINESSAREA::varchar AS BUSINESSAREA, 
                        src:COMMSPROTOCOL::varchar AS COMMSPROTOCOL, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CONFINEDSPACE::varchar AS CONFINEDSPACE, 
                        src:COSTCENTRE::varchar AS COSTCENTRE, 
                        src:CRITICALITY::varchar AS CRITICALITY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPOSALDESC::varchar AS DISPOSALDESC, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:DRAWINGNOREF::varchar AS DRAWINGNOREF, 
                        src:ENERGYRATING::integer AS ENERGYRATING, 
                        src:FORDISPOSAL::varchar AS FORDISPOSAL, 
                        src:FUNCAREA::varchar AS FUNCAREA, 
                        src:GRDLEVEL::numeric(38, 10) AS GRDLEVEL, 
                        src:HAZARDRATING::varchar AS HAZARDRATING, 
                        src:INSTRUMENTRANGE::varchar AS INSTRUMENTRANGE, 
                        src:INSULATIONCLASS::varchar AS INSULATIONCLASS, 
                        src:IPRATING::varchar AS IPRATING, 
                        src:LINKEDASSET::varchar AS LINKEDASSET, 
                        src:LOCALITY::varchar AS LOCALITY, 
                        src:MAKE::varchar AS MAKE, 
                        src:MEDIATYPE::varchar AS MEDIATYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:PHOTO3D::varchar AS PHOTO3D, 
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
                        src:SUBTYPEFEATURES::varchar AS SUBTYPEFEATURES, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOLTAGEIN::numeric(38, 10) AS VOLTAGEIN, 
                        src:VOLTAGEINTYPE::varchar AS VOLTAGEINTYPE, 
                        src:VOLTAGEOUT::numeric(38, 10) AS VOLTAGEOUT, 
                        src:VOLTAGEOUTTYPE::varchar AS VOLTAGEOUTTYPE, 
                        src:WARRANTYEND::datetime AS WARRANTYEND, 
                        src:WARRANTYSTART::datetime AS WARRANTYSTART, 
                        src:XCONTROLSYSKEY::integer AS XCONTROLSYSKEY, 
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
                                        
                src:XCONTROLSYSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETSCONFIGURED_XCONTROLSYS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENERGYRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRDLEVEL), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEIN), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCONTROLSYSKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null