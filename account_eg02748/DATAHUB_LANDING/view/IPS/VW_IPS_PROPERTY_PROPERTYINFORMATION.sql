CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PROPERTY_PROPERTYINFORMATION AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AREA::varchar AS AREA, 
                        src:BLOCK::varchar AS BLOCK, 
                        src:BOROUGH::varchar AS BOROUGH, 
                        src:CENSUSTRACT::integer AS CENSUSTRACT, 
                        src:CITYOWNED::varchar AS CITYOWNED, 
                        src:COUNTY::varchar AS COUNTY, 
                        src:CURRPROP::varchar AS CURRPROP, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:HEALTHAREA::integer AS HEALTHAREA, 
                        src:LANDMARK::varchar AS LANDMARK, 
                        src:LEGALOWNER::varchar AS LEGALOWNER, 
                        src:LOFT::varchar AS LOFT, 
                        src:LOT::varchar AS LOT, 
                        src:MAPNUMBER::varchar AS MAPNUMBER, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOTES::varchar AS NOTES, 
                        src:PLANMEDIA::varchar AS PLANMEDIA, 
                        src:PROPID::varchar AS PROPID, 
                        src:PROPKEY::integer AS PROPKEY, 
                        src:PROPNAME::varchar AS PROPNAME, 
                        src:PROPNOTE::varchar AS PROPNOTE, 
                        src:PROPTYPE::varchar AS PROPTYPE, 
                        src:SPECIALPLACENAME::varchar AS SPECIALPLACENAME, 
                        src:STATUS::varchar AS STATUS, 
                        src:SUBDIVDESC::varchar AS SUBDIVDESC, 
                        src:SUBDIVISION::varchar AS SUBDIVISION, 
                        src:TAXBLOCK::varchar AS TAXBLOCK, 
                        src:TAXEXEMPT::varchar AS TAXEXEMPT, 
                        src:TOWNSHIP::varchar AS TOWNSHIP, 
                        src:VACANT::varchar AS VACANT, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VERSION::integer AS VERSION, 
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
    
                        
                src:PROPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PROPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PROPERTY_PROPERTYINFORMATION as strm))