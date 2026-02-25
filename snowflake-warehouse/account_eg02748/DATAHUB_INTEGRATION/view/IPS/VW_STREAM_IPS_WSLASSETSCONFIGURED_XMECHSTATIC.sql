CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSCONFIGURED_XMECHSTATIC AS SELECT
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
                        src:DIAMEXT::numeric(38, 10) AS DIAMEXT, 
                        src:DIAMINLET::numeric(38, 10) AS DIAMINLET, 
                        src:DIAMINT::numeric(38, 10) AS DIAMINT, 
                        src:DIAMOUTLET::numeric(38, 10) AS DIAMOUTLET, 
                        src:DISPOSALDESC::varchar AS DISPOSALDESC, 
                        src:DRAWINGNO::varchar AS DRAWINGNO, 
                        src:DRAWINGNOREF::varchar AS DRAWINGNOREF, 
                        src:ENERGYRATING::integer AS ENERGYRATING, 
                        src:EXTCOAT::varchar AS EXTCOAT, 
                        src:FORDISPOSAL::varchar AS FORDISPOSAL, 
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
                        src:STIFFRATING::numeric(38, 10) AS STIFFRATING, 
                        src:SUBTYPEFEATURES::varchar AS SUBTYPEFEATURES, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOLTAGEIN::numeric(38, 10) AS VOLTAGEIN, 
                        src:VOLTAGEINTYPE::varchar AS VOLTAGEINTYPE, 
                        src:WARRANTYEND::datetime AS WARRANTYEND, 
                        src:WARRANTYSTART::datetime AS WARRANTYSTART, 
                        src:XMECHSTATICKEY::integer AS XMECHSTATICKEY, 
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
                                        
                src:XMECHSTATICKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLASSETSCONFIGURED_XMECHSTATIC as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMEXT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMINLET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMINT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIAMOUTLET), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ENERGYRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAXFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MINFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRESSURERATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STAGES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:STIFFRATING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOLTAGEIN), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYEND), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:WARRANTYSTART), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XMECHSTATICKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null