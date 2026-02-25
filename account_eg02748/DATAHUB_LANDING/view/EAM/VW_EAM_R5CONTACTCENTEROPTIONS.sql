CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5CONTACTCENTEROPTIONS AS SELECT
                        src:COP_CHKDUPLICATEOPENREQ::varchar AS COP_CHKDUPLICATEOPENREQ, 
                        src:COP_CHKRECURRINGCLOSEDREQ::varchar AS COP_CHKRECURRINGCLOSEDREQ, 
                        src:COP_CLOSEABLE::varchar AS COP_CLOSEABLE, 
                        src:COP_COPYCUSTOMERINFO::varchar AS COP_COPYCUSTOMERINFO, 
                        src:COP_DEFCONTACTSOURCE::varchar AS COP_DEFCONTACTSOURCE, 
                        src:COP_DEFFINDBY::varchar AS COP_DEFFINDBY, 
                        src:COP_DEFFOLLOWUPSTATUS::varchar AS COP_DEFFOLLOWUPSTATUS, 
                        src:COP_DEFOPENSTATUS::varchar AS COP_DEFOPENSTATUS, 
                        src:COP_DEFORGUSAGE::varchar AS COP_DEFORGUSAGE, 
                        src:COP_DEFPORTALSOURCE::varchar AS COP_DEFPORTALSOURCE, 
                        src:COP_DEFPORTALSTATUS::varchar AS COP_DEFPORTALSTATUS, 
                        src:COP_DEFPORTALTYPE::varchar AS COP_DEFPORTALTYPE, 
                        src:COP_DEFTYPE::varchar AS COP_DEFTYPE, 
                        src:COP_DEFWOORG::varchar AS COP_DEFWOORG, 
                        src:COP_DEFWOSTATUS::varchar AS COP_DEFWOSTATUS, 
                        src:COP_DEPTSTRUCTUREUSAGE::varchar AS COP_DEPTSTRUCTUREUSAGE, 
                        src:COP_EVENTTYPEFILTER::varchar AS COP_EVENTTYPEFILTER, 
                        src:COP_HIGHLIGHTMAP::varchar AS COP_HIGHLIGHTMAP, 
                        src:COP_IDEFEATURE::varchar AS COP_IDEFEATURE, 
                        src:COP_INCLUDEPMWORKORDERS::varchar AS COP_INCLUDEPMWORKORDERS, 
                        src:COP_KBRESULTSPERPAGE::numeric(38, 10) AS COP_KBRESULTSPERPAGE, 
                        src:COP_LASTSAVED::datetime AS COP_LASTSAVED, 
                        src:COP_MATCHSC::varchar AS COP_MATCHSC, 
                        src:COP_MATCHSPC::varchar AS COP_MATCHSPC, 
                        src:COP_MINPENALTY::numeric(38, 10) AS COP_MINPENALTY, 
                        src:COP_NEARADDRESS::varchar AS COP_NEARADDRESS, 
                        src:COP_NEWCUSTOMERALLOWED::varchar AS COP_NEWCUSTOMERALLOWED, 
                        src:COP_ORG::varchar AS COP_ORG, 
                        src:COP_RECURRINGREQLOOKBACKDAYS::numeric(38, 10) AS COP_RECURRINGREQLOOKBACKDAYS, 
                        src:COP_SCHEDULEWO::varchar AS COP_SCHEDULEWO, 
                        src:COP_SHOWCHILDREN::varchar AS COP_SHOWCHILDREN, 
                        src:COP_SHOWPROVIDER::varchar AS COP_SHOWPROVIDER, 
                        src:COP_SHOWSERVICECATEGORY::varchar AS COP_SHOWSERVICECATEGORY, 
                        src:COP_SHOWSPC::varchar AS COP_SHOWSPC, 
                        src:COP_SHOWSUPPLIER::varchar AS COP_SHOWSUPPLIER, 
                        src:COP_SPCVALIDATION::varchar AS COP_SPCVALIDATION, 
                        src:COP_TOPTENLOOKBACK::numeric(38, 10) AS COP_TOPTENLOOKBACK, 
                        src:COP_UPDATECOUNT::numeric(38, 10) AS COP_UPDATECOUNT, 
                        src:COP_UPDATED::datetime AS COP_UPDATED, 
                        src:COP_UPDATEDBY::varchar AS COP_UPDATEDBY, 
                        src:COP_WOCLOSEDAYS::numeric(38, 10) AS COP_WOCLOSEDAYS, 
                        src:COP_WODUPLICATECHECK::varchar AS COP_WODUPLICATECHECK, 
                        src:COP_WOHEADEROBJONLY::varchar AS COP_WOHEADEROBJONLY, 
                        src:COP_WOOPENDAYS::numeric(38, 10) AS COP_WOOPENDAYS, 
                        src:COP_WOSTATUSES::varchar AS COP_WOSTATUSES, 
                        src:COP_WOTYPES::varchar AS COP_WOTYPES, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:COP_ORG,
            src:COP_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:COP_ORG  ORDER BY 
            src:COP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5CONTACTCENTEROPTIONS as strm))