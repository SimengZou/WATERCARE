CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCDRPROJ_XPROJCOMPSTAKEHOLDERSDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROJINSPDTLKEY::integer AS APPROJINSPDTLKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DEVELOPERCOMPANYNAME::varchar AS DEVELOPERCOMPANYNAME, 
                        src:DEVELOPEREMAIL::varchar AS DEVELOPEREMAIL, 
                        src:DEVELOPERFIRSTNAME::varchar AS DEVELOPERFIRSTNAME, 
                        src:DEVELOPERLASTNAME::varchar AS DEVELOPERLASTNAME, 
                        src:DEVELOPERMOBILE::varchar AS DEVELOPERMOBILE, 
                        src:DEVELOPERPHONE::varchar AS DEVELOPERPHONE, 
                        src:DEVELOPERPOSTCODE::varchar AS DEVELOPERPOSTCODE, 
                        src:DEVELOPERREFERENCE::varchar AS DEVELOPERREFERENCE, 
                        src:DEVELOPERSTREETNAME::varchar AS DEVELOPERSTREETNAME, 
                        src:DEVELOPERSTREETNO::varchar AS DEVELOPERSTREETNO, 
                        src:DEVELOPERSUBURB::varchar AS DEVELOPERSUBURB, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTECOMPANYNAME::varchar AS WASTECOMPANYNAME, 
                        src:WASTEEMAIL::varchar AS WASTEEMAIL, 
                        src:WASTEFIRSTNAME::varchar AS WASTEFIRSTNAME, 
                        src:WASTELASTNAME::varchar AS WASTELASTNAME, 
                        src:WASTEMOBILE::varchar AS WASTEMOBILE, 
                        src:WASTEPHONE::varchar AS WASTEPHONE, 
                        src:WASTEPOSTCODE::varchar AS WASTEPOSTCODE, 
                        src:WASTEREFERENCE::varchar AS WASTEREFERENCE, 
                        src:WASTESTREETNAME::varchar AS WASTESTREETNAME, 
                        src:WASTESTREETNO::varchar AS WASTESTREETNO, 
                        src:WASTESUBURB::varchar AS WASTESUBURB, 
                        src:WATERCOMPANYNAME::varchar AS WATERCOMPANYNAME, 
                        src:WATEREMAIL::varchar AS WATEREMAIL, 
                        src:WATERFIRSTNAME::varchar AS WATERFIRSTNAME, 
                        src:WATERLASTNAME::varchar AS WATERLASTNAME, 
                        src:WATERMOBILE::varchar AS WATERMOBILE, 
                        src:WATERPHONE::varchar AS WATERPHONE, 
                        src:WATERPOSTCODE::varchar AS WATERPOSTCODE, 
                        src:WATERREFERENCE::varchar AS WATERREFERENCE, 
                        src:WATERSTREETNAME::varchar AS WATERSTREETNAME, 
                        src:WATERSTREETNO::varchar AS WATERSTREETNO, 
                        src:WATERSUBURB::varchar AS WATERSUBURB, 
                        src:XPROJCOMPSTAKEHOLDERSDPKEY::integer AS XPROJCOMPSTAKEHOLDERSDPKEY, 
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
    
                        
                src:XPROJCOMPSTAKEHOLDERSDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XPROJCOMPSTAKEHOLDERSDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCDRPROJ_XPROJCOMPSTAKEHOLDERSDP as strm))