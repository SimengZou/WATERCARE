
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5CLAIMCONFIG
                   as                       
                    SELECT
                        t.CLC_ORG,
                        t.CLC_CONTRACTOR,
                        t.CLC_COSTITEM,
                        t.CLC_LNCOMPANY,
                        t.CLC_STGCOSTCODE,
                        t.CLC_PROJECTNUMBER,
                        t.CLC_PROJECTACTIVITY,
                        t.CLC_SUPPLIER,
                        t.CLC_STORE,
                        t.CLC_CREATOR,
                        t.CLC_REQUESTOR,
                        t.CLC_REQUESTOR2,
                        t.CLC_PURCHASEOFFICE,
                        t.CLC_CONTORDERTYPE,
                        t.CLC_NONCONTORDERTYPE,
                        t.CLC_ORDERSERIES,
                        t.CLC_EXCLUDEWOFROMLN,
                        t.CLC_EXCLUDEPOFROMLN,
                        t.CLC_SPLITINTERCOMPANYCOSTS,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.CLC_PMWORATEAPP,
                        t.CLC_CHILDWORATEAPP,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5CLAIMCONFIG as  t
					 union
					        SELECT
                        th.CLC_ORG,
                        th.CLC_CONTRACTOR,
                        th.CLC_COSTITEM,
                        th.CLC_LNCOMPANY,
                        th.CLC_STGCOSTCODE,
                        th.CLC_PROJECTNUMBER,
                        th.CLC_PROJECTACTIVITY,
                        th.CLC_SUPPLIER,
                        th.CLC_STORE,
                        th.CLC_CREATOR,
                        th.CLC_REQUESTOR,
                        th.CLC_REQUESTOR2,
                        th.CLC_PURCHASEOFFICE,
                        th.CLC_CONTORDERTYPE,
                        th.CLC_NONCONTORDERTYPE,
                        th.CLC_ORDERSERIES,
                        th.CLC_EXCLUDEWOFROMLN,
                        th.CLC_EXCLUDEPOFROMLN,
                        th.CLC_SPLITINTERCOMPANYCOSTS,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.CLC_PMWORATEAPP,
                        th.CLC_CHILDWORATEAPP,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5CLAIMCONFIG as  th ;
                     