CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETSWATER_XWATERVALVE AS SELECT
                        src:ACCELDEPREC::varchar AS ACCELDEPREC, 
                        src:ACCELDEPRECDESC::varchar AS ACCELDEPRECDESC, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BUSINESSAREA::varchar AS BUSINESSAREA, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CONFINEDSPACE::varchar AS CONFINEDSPACE, 
                        src:COSTCENTRE::varchar AS COSTCENTRE, 
                        src:CRITICALITY::varchar AS CRITICALITY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISPOSALDESC::varchar AS DISPOSALDESC, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:DRAWINGNOREF::varchar AS DRAWINGNOREF, 
                        src:EXTCOAT::varchar AS EXTCOAT, 
                        src:FOBFP::varchar AS FOBFP, 
                        src:FOBURSTCONTROL::varchar AS FOBURSTCONTROL, 
                        src:FOBYPASS::varchar AS FOBYPASS, 
                        src:FODISCHARGE::varchar AS FODISCHARGE, 
                        src:FOFIREFIGHTING::varchar AS FOFIREFIGHTING, 
                        src:FOFLOWCONTROL::varchar AS FOFLOWCONTROL, 
                        src:FOFLOWDIRECTION::varchar AS FOFLOWDIRECTION, 
                        src:FOISOLATION::varchar AS FOISOLATION, 
                        src:FOLEVELCONTROL::varchar AS FOLEVELCONTROL, 
                        src:FONONRETURN::varchar AS FONONRETURN, 
                        src:FOPRESSRED::varchar AS FOPRESSRED, 
                        src:FOPRESSRLF::varchar AS FOPRESSRLF, 
                        src:FOPRESSSSTN::varchar AS FOPRESSSSTN, 
                        src:FORDISPOSAL::varchar AS FORDISPOSAL, 
                        src:FOREGULATING::varchar AS FOREGULATING, 
                        src:FOSCOUR::varchar AS FOSCOUR, 
                        src:FOSURGE::varchar AS FOSURGE, 
                        src:FOVACB::varchar AS FOVACB, 
                        src:FUNCAREA::varchar AS FUNCAREA, 
                        src:HAZARDRATING::varchar AS HAZARDRATING, 
                        src:INSTALLMOUNT::varchar AS INSTALLMOUNT, 
                        src:INTLINING::varchar AS INTLINING, 
                        src:IPRATING::varchar AS IPRATING, 
                        src:JTTYPE::varchar AS JTTYPE, 
                        src:LINKEDASSET::varchar AS LINKEDASSET, 
                        src:LOCALITY::varchar AS LOCALITY, 
                        src:MAKE::varchar AS MAKE, 
                        src:MAXFLOW::numeric(38, 10) AS MAXFLOW, 
                        src:MEDIATYPE::varchar AS MEDIATYPE, 
                        src:MINFLOW::numeric(38, 10) AS MINFLOW, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
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
                        src:SUBTYPEFEATURES::varchar AS SUBTYPEFEATURES, 
                        src:TORQUE::numeric(38, 10) AS TORQUE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WARRANTYEND::datetime AS WARRANTYEND, 
                        src:WARRANTYSTART::datetime AS WARRANTYSTART, 
                        src:XWATERVALVEKEY::integer AS XWATERVALVEKEY, 
                        src:ZONECATCHMENT::varchar AS ZONECATCHMENT, 
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
    
                        
                src:XWATERVALVEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XWATERVALVEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETSWATER_XWATERVALVE as strm))