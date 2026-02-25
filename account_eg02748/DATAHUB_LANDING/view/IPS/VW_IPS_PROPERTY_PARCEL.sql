CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_PROPERTY_PARCEL AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AREA::varchar AS AREA, 
                        src:ATLASPAGE::varchar AS ATLASPAGE, 
                        src:BLOCK::varchar AS BLOCK, 
                        src:COMMELEM::numeric(38, 10) AS COMMELEM, 
                        src:COUNTY::varchar AS COUNTY, 
                        src:CURRPRCL::varchar AS CURRPRCL, 
                        src:DELETED::boolean AS DELETED, 
                        src:EFFDATE::datetime AS EFFDATE, 
                        src:EXPDATE::datetime AS EXPDATE, 
                        src:LEGALOWNER::varchar AS LEGALOWNER, 
                        src:LNDPRICETY::varchar AS LNDPRICETY, 
                        src:LOT::varchar AS LOT, 
                        src:LOTDPTH::numeric(38, 10) AS LOTDPTH, 
                        src:LOTFRONTAG::numeric(38, 10) AS LOTFRONTAG, 
                        src:LOTSHP::varchar AS LOTSHP, 
                        src:LOTSZ::numeric(38, 10) AS LOTSZ, 
                        src:LOTUM::varchar AS LOTUM, 
                        src:MAPNO::varchar AS MAPNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PLANMEDIA::varchar AS PLANMEDIA, 
                        src:PRCLID::varchar AS PRCLID, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRCLNAME::varchar AS PRCLNAME, 
                        src:PRCLSTAT::varchar AS PRCLSTAT, 
                        src:PRCLTYPE::varchar AS PRCLTYPE, 
                        src:PROPNOTE::varchar AS PROPNOTE, 
                        src:RANGE::varchar AS RANGE, 
                        src:SECTION::varchar AS SECTION, 
                        src:SUBDIV::varchar AS SUBDIV, 
                        src:SUBDIVCODE::varchar AS SUBDIVCODE, 
                        src:SUBOFKEY::integer AS SUBOFKEY, 
                        src:TOWNSHIP::varchar AS TOWNSHIP, 
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
    
                        
                src:PRCLKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:PRCLKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_PROPERTY_PARCEL as strm))