CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WORKMANAGEMENT_HISTORY AS SELECT
                        src:ACTKEY::integer AS ACTKEY, 
                        src:ACTUALQTY::numeric(38, 10) AS ACTUALQTY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:ASGNFLAG::varchar AS ASGNFLAG, 
                        src:ASSETINSPECTIONKEY::integer AS ASSETINSPECTIONKEY, 
                        src:ASSIGNED::varchar AS ASSIGNED, 
                        src:ASSVALTYPE::varchar AS ASSVALTYPE, 
                        src:AUTH::varchar AS AUTH, 
                        src:BGTNO::varchar AS BGTNO, 
                        src:BLDGFLOOR::integer AS BLDGFLOOR, 
                        src:BLDGROOM::integer AS BLDGROOM, 
                        src:BUDGETKEY::integer AS BUDGETKEY, 
                        src:CANCELWORKORDER::varchar AS CANCELWORKORDER, 
                        src:CMPLKEY::integer AS CMPLKEY, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:COMPBY::varchar AS COMPBY, 
                        src:COMPDTTM::datetime AS COMPDTTM, 
                        src:COMPFLAG::varchar AS COMPFLAG, 
                        src:COMPKEY::integer AS COMPKEY, 
                        src:COMPTYPE::integer AS COMPTYPE, 
                        src:COND::varchar AS COND, 
                        src:DATAGRP::varchar AS DATAGRP, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DIST::numeric(38, 10) AS DIST, 
                        src:DOWNTIME::numeric(38, 10) AS DOWNTIME, 
                        src:ESTIMATEDCOST::numeric(38, 10) AS ESTIMATEDCOST, 
                        src:FAILFLAG::varchar AS FAILFLAG, 
                        src:FLOWDPTH::numeric(38, 10) AS FLOWDPTH, 
                        src:FRREF::numeric(38, 10) AS FRREF, 
                        src:GRPMAINTSCHD::integer AS GRPMAINTSCHD, 
                        src:GRPPROJKEY::integer AS GRPPROJKEY, 
                        src:HISTKEY::integer AS HISTKEY, 
                        src:HRS::numeric(38, 10) AS HRS, 
                        src:INCIDENT::integer AS INCIDENT, 
                        src:INITBY::varchar AS INITBY, 
                        src:INITDTTM::datetime AS INITDTTM, 
                        src:LINEARFROMFT::numeric(38, 10) AS LINEARFROMFT, 
                        src:LINEARFROMM::numeric(38, 10) AS LINEARFROMM, 
                        src:LINEARTOFT::numeric(38, 10) AS LINEARTOFT, 
                        src:LINEARTOM::numeric(38, 10) AS LINEARTOM, 
                        src:LOCATION::varchar AS LOCATION, 
                        src:MAINTSCHED::integer AS MAINTSCHED, 
                        src:MAINTTYPE::varchar AS MAINTTYPE, 
                        src:MEASFLOW::numeric(38, 10) AS MEASFLOW, 
                        src:MILESTONE::integer AS MILESTONE, 
                        src:MOBILE::varchar AS MOBILE, 
                        src:MOBILEDTTM::datetime AS MOBILEDTTM, 
                        src:MOBILEEMP::varchar AS MOBILEEMP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTSCHEDFLAG::varchar AS NEXTSCHEDFLAG, 
                        src:OUTOFSERV::varchar AS OUTOFSERV, 
                        src:OWNKEY::integer AS OWNKEY, 
                        src:PLANKEY::integer AS PLANKEY, 
                        src:PLANNEDQTY::numeric(38, 10) AS PLANNEDQTY, 
                        src:POTSERVREQ::varchar AS POTSERVREQ, 
                        src:PRCLKEY::integer AS PRCLKEY, 
                        src:PRES::numeric(38, 10) AS PRES, 
                        src:PRI::varchar AS PRI, 
                        src:PROB::varchar AS PROB, 
                        src:PROJ::varchar AS PROJ, 
                        src:REFNO::varchar AS REFNO, 
                        src:RES::varchar AS RES, 
                        src:RESP::varchar AS RESP, 
                        src:RESPONSIBILITY::varchar AS RESPONSIBILITY, 
                        src:RWATTRKEY::integer AS RWATTRKEY, 
                        src:RWATTRTYPE::varchar AS RWATTRTYPE, 
                        src:RWXSP::varchar AS RWXSP, 
                        src:SCHEDDTTM::datetime AS SCHEDDTTM, 
                        src:SCHEDFINISH::datetime AS SCHEDFINISH, 
                        src:SCHEDFINISHFLAG::varchar AS SCHEDFINISHFLAG, 
                        src:SCHEDFLAG::varchar AS SCHEDFLAG, 
                        src:SRC::varchar AS SRC, 
                        src:STARTDTTM::datetime AS STARTDTTM, 
                        src:STARTFLAG::varchar AS STARTFLAG, 
                        src:STDACTFLAG::varchar AS STDACTFLAG, 
                        src:STOPPAGE::varchar AS STOPPAGE, 
                        src:SUSPDTTM::datetime AS SUSPDTTM, 
                        src:TOREF::numeric(38, 10) AS TOREF, 
                        src:UM::varchar AS UM, 
                        src:USG::numeric(38, 10) AS USG, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WOLINKKEY::integer AS WOLINKKEY, 
                        src:WOTYPE::varchar AS WOTYPE, 
                        src:XCOORDINATE::numeric(38, 10) AS XCOORDINATE, 
                        src:YCOORDINATE::numeric(38, 10) AS YCOORDINATE, 
                        src:ZCOORDINATE::numeric(38, 10) AS ZCOORDINATE, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:HISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WORKMANAGEMENT_HISTORY as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTUALQTY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ASSETINSPECTIONKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGFLOOR), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BLDGROOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BUDGETKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CMPLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COMPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DIST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:DOWNTIME), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESTIMATEDCOST), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FLOWDPTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FRREF), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRPMAINTSCHD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GRPPROJKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HISTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HRS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INCIDENT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:INITDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARFROMM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOFT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEARTOM), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MAINTSCHED), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MEASFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MILESTONE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MOBILEDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OWNKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PLANKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PLANNEDQTY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRCLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRES), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RWATTRKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCHEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SCHEDFINISH), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:STARTDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:SUSPDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TOREF), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USG), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WOLINKKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:YCOORDINATE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ZCOORDINATE), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null