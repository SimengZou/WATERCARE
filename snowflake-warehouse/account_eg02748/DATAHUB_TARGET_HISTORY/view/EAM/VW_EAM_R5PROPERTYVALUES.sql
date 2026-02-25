
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5PROPERTYVALUES
                   as                       
                    SELECT
                        t.PRV_PROPERTY,
                        t.PRV_RENTITY,
                        t.PRV_CLASS,
                        t.PRV_CODE,
                        t.PRV_SEQNO,
                        t.PRV_VALUE,
                        t.PRV_NVALUE,
                        t.PRV_DVALUE,
                        t.PRV_CLASS_ORG,
                        t.PRV_UPDATECOUNT,
                        t.PRV_CREATED,
                        t.PRV_UPDATED,
                        t.PRV_NOTUSED,
                        t.PRV_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5PROPERTYVALUES as  t
					 union
					        SELECT
                        th.PRV_PROPERTY,
                        th.PRV_RENTITY,
                        th.PRV_CLASS,
                        th.PRV_CODE,
                        th.PRV_SEQNO,
                        th.PRV_VALUE,
                        th.PRV_NVALUE,
                        th.PRV_DVALUE,
                        th.PRV_CLASS_ORG,
                        th.PRV_UPDATECOUNT,
                        th.PRV_CREATED,
                        th.PRV_UPDATED,
                        th.PRV_NOTUSED,
                        th.PRV_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5PROPERTYVALUES as  th ;
                     