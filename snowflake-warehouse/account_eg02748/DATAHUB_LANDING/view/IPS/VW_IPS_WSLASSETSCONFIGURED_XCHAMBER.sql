CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETSCONFIGURED_XCHAMBER AS SELECT
                        src:ACCELDEPREC::varchar AS ACCELDEPREC, 
                        src:ACCELDEPRECDESC::varchar AS ACCELDEPRECDESC, 
                        src:ACQTYPE::varchar AS ACQTYPE, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BOLTDOWN::varchar AS BOLTDOWN, 
                        src:BUSINESSAREA::varchar AS BUSINESSAREA, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:CONFINEDSPACE::varchar AS CONFINEDSPACE, 
                        src:COSTCENTRE::varchar AS COSTCENTRE, 
                        src:COVERTYPE::varchar AS COVERTYPE, 
                        src:CRITICALITY::varchar AS CRITICALITY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DESIGNRESILIENCE::varchar AS DESIGNRESILIENCE, 
                        src:DIAMEXT::numeric(38, 10) AS DIAMEXT, 
                        src:DIAMINT::numeric(38, 10) AS DIAMINT, 
                        src:DISPOSALDESC::varchar AS DISPOSALDESC, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:DRAWINGNOREF::varchar AS DRAWINGNOREF, 
                        src:DROPMH::varchar AS DROPMH, 
                        src:FALLPROTECT::varchar AS FALLPROTECT, 
                        src:FORDISPOSAL::varchar AS FORDISPOSAL, 
                        src:FUNCAREA::varchar AS FUNCAREA, 
                        src:GRDLEVEL::numeric(38, 10) AS GRDLEVEL, 
                        src:HINGED::varchar AS HINGED, 
                        src:INTLINING::varchar AS INTLINING, 
                        src:INVLEVEL::numeric(38, 10) AS INVLEVEL, 
                        src:LIDLEVEL::numeric(38, 10) AS LIDLEVEL, 
                        src:LINKEDASSET::varchar AS LINKEDASSET, 
                        src:LOADRATING::numeric(38, 10) AS LOADRATING, 
                        src:LOCALITY::varchar AS LOCALITY, 
                        src:LOCKABLE::varchar AS LOCKABLE, 
                        src:MAKE::varchar AS MAKE, 
                        src:MEDIATYPE::varchar AS MEDIATYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MODELNO::varchar AS MODELNO, 
                        src:OVERFLOWTYPE::varchar AS OVERFLOWTYPE, 
                        src:PHOTO3D::varchar AS PHOTO3D, 
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
                        src:SUBTYPEFEATURES::varchar AS SUBTYPEFEATURES, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WARRANTYEND::datetime AS WARRANTYEND, 
                        src:WARRANTYSTART::datetime AS WARRANTYSTART, 
                        src:XCHAMBERKEY::integer AS XCHAMBERKEY, 
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
    
                        
                src:XCHAMBERKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XCHAMBERKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETSCONFIGURED_XCHAMBER as strm))