
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_U5WOCLAIMS
                   as                       
                    SELECT
                        t.WCO_EVENT,
                        t.WCO_PK,
                        t.WCO_ORG,
                        t.WCO_SCHEDULE_ITEM,
                        t.WCO_TRANSID,
                        t.WCO_CONTRACTOR_QTY,
                        t.WCO_CONTRACTOR_RATE,
                        t.WCO_CHARGEDATE,
                        t.WCO_COMMENTS,
                        t.WCO_SCHEDITEM_DESC,
                        t.WCO_SCHEDITEM_RATE,
                        t.WCO_LINETOTAL,
                        t.WCO_WOSCHEDITEM,
                        t.WCO_WOTYPE,
                        t.WCO_WOPARENT,
                        t.WCO_CONTRACT_CODE,
                        t.WCO_CONTRACTOR,
                        t.WCO_ACTIVITY,
                        t.WCO_ACTIVITY_DESC,
                        t.CREATEDBY,
                        t.CREATED,
                        t.UPDATEDBY,
                        t.UPDATED,
                        t.LASTSAVED,
                        t.UPDATECOUNT,
                        t.WCO_LINE_STATUS,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_U5WOCLAIMS as  t
					 union
					        SELECT
                        th.WCO_EVENT,
                        th.WCO_PK,
                        th.WCO_ORG,
                        th.WCO_SCHEDULE_ITEM,
                        th.WCO_TRANSID,
                        th.WCO_CONTRACTOR_QTY,
                        th.WCO_CONTRACTOR_RATE,
                        th.WCO_CHARGEDATE,
                        th.WCO_COMMENTS,
                        th.WCO_SCHEDITEM_DESC,
                        th.WCO_SCHEDITEM_RATE,
                        th.WCO_LINETOTAL,
                        th.WCO_WOSCHEDITEM,
                        th.WCO_WOTYPE,
                        th.WCO_WOPARENT,
                        th.WCO_CONTRACT_CODE,
                        th.WCO_CONTRACTOR,
                        th.WCO_ACTIVITY,
                        th.WCO_ACTIVITY_DESC,
                        th.CREATEDBY,
                        th.CREATED,
                        th.UPDATEDBY,
                        th.UPDATED,
                        th.LASTSAVED,
                        th.UPDATECOUNT,
                        th.WCO_LINE_STATUS,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_U5WOCLAIMS as  th ;
                     