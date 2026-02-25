
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5ROUTOBJECTS
                   as                       
                    SELECT
                        t.ROB_ROUTE,
                        t.ROB_LINE,
                        t.ROB_OBTYPE,
                        t.ROB_OBRTYPE,
                        t.ROB_OBJECT,
                        t.ROB_REVISION,
                        t.ROB_OBJECT_ORG,
                        t.ROB_UPDATECOUNT,
                        t.ROB_FROMPOINT,
                        t.ROB_FROMREFDESC,
                        t.ROB_FROMGEOREF,
                        t.ROB_TOPOINT,
                        t.ROB_TOREFDESC,
                        t.ROB_TOGEOREF,
                        t.ROB_FROM_REFERENCE,
                        t.ROB_FROM_OFFSET,
                        t.ROB_FROM_OFFSET_DIRECTION,
                        t.ROB_TO_REFERENCE,
                        t.ROB_TO_OFFSET,
                        t.ROB_TO_OFFSET_DIRECTION,
                        t.ROB_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5ROUTOBJECTS as  t
					 union
					        SELECT
                        th.ROB_ROUTE,
                        th.ROB_LINE,
                        th.ROB_OBTYPE,
                        th.ROB_OBRTYPE,
                        th.ROB_OBJECT,
                        th.ROB_REVISION,
                        th.ROB_OBJECT_ORG,
                        th.ROB_UPDATECOUNT,
                        th.ROB_FROMPOINT,
                        th.ROB_FROMREFDESC,
                        th.ROB_FROMGEOREF,
                        th.ROB_TOPOINT,
                        th.ROB_TOREFDESC,
                        th.ROB_TOGEOREF,
                        th.ROB_FROM_REFERENCE,
                        th.ROB_FROM_OFFSET,
                        th.ROB_FROM_OFFSET_DIRECTION,
                        th.ROB_TO_REFERENCE,
                        th.ROB_TO_OFFSET,
                        th.ROB_TO_OFFSET_DIRECTION,
                        th.ROB_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5ROUTOBJECTS as  th ;
                     