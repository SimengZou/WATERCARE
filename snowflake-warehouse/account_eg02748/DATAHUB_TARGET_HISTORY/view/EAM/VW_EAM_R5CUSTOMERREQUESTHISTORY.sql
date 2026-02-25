
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5CUSTOMERREQUESTHISTORY
                   as                       
                    SELECT
                        t.CRH_PK,
                        t.CRH_CALLCENTERCODE,
                        t.CRH_CALLCENTER_ORG,
                        t.CRH_REQDATE,
                        t.CRH_EVENTTYPE,
                        t.CRH_USERCODE,
                        t.CRH_FIELD,
                        t.CRH_OLDVALUE,
                        t.CRH_NEWVALUE,
                        t.CRH_UPDATECOUNT,
                        t.CRH_COMMENT,
                        t.CRH_EVENT,
                        t.CRH_RENTITY,
                        t.CRH_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5CUSTOMERREQUESTHISTORY as  t
					 union
					        SELECT
                        th.CRH_PK,
                        th.CRH_CALLCENTERCODE,
                        th.CRH_CALLCENTER_ORG,
                        th.CRH_REQDATE,
                        th.CRH_EVENTTYPE,
                        th.CRH_USERCODE,
                        th.CRH_FIELD,
                        th.CRH_OLDVALUE,
                        th.CRH_NEWVALUE,
                        th.CRH_UPDATECOUNT,
                        th.CRH_COMMENT,
                        th.CRH_EVENT,
                        th.CRH_RENTITY,
                        th.CRH_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5CUSTOMERREQUESTHISTORY as  th ;
                     