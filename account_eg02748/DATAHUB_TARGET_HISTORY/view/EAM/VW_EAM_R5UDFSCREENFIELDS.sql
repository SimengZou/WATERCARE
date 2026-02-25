
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5UDFSCREENFIELDS
                   as                       
                    SELECT
                        t.USF_SCREENNAME,
                        t.USF_FIELDNAME,
                        t.USF_DESC,
                        t.USF_SEQUENCE,
                        t.USF_MAXLENGTH,
                        t.USF_PRECISION,
                        t.USF_SCALE,
                        t.USF_FIELDLABEL,
                        t.USF_FIELDTYPE,
                        t.USF_PRIMARY,
                        t.USF_NOTUSED,
                        t.USF_NULLABLE,
                        t.USF_PARENTFIELD,
                        t.USF_LOOKUPSOURCE,
                        t.USF_LOOKUPQUERY,
                        t.USF_COMPUTEDDATA,
                        t.USF_UPPERCASE,
                        t.USF_CREATED,
                        t.USF_UPDATED,
                        t.USF_CREATEDBY,
                        t.USF_UPDATEDBY,
                        t.USF_UPDATECOUNT,
                        t.USF_GENERATED,
                        t.USF_RETRIEVEDVALUELOOKUP,
                        t.USF_DROPPDOWN,
                        t.USF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5UDFSCREENFIELDS as  t
					 union
					        SELECT
                        th.USF_SCREENNAME,
                        th.USF_FIELDNAME,
                        th.USF_DESC,
                        th.USF_SEQUENCE,
                        th.USF_MAXLENGTH,
                        th.USF_PRECISION,
                        th.USF_SCALE,
                        th.USF_FIELDLABEL,
                        th.USF_FIELDTYPE,
                        th.USF_PRIMARY,
                        th.USF_NOTUSED,
                        th.USF_NULLABLE,
                        th.USF_PARENTFIELD,
                        th.USF_LOOKUPSOURCE,
                        th.USF_LOOKUPQUERY,
                        th.USF_COMPUTEDDATA,
                        th.USF_UPPERCASE,
                        th.USF_CREATED,
                        th.USF_UPDATED,
                        th.USF_CREATEDBY,
                        th.USF_UPDATEDBY,
                        th.USF_UPDATECOUNT,
                        th.USF_GENERATED,
                        th.USF_RETRIEVEDVALUELOOKUP,
                        th.USF_DROPPDOWN,
                        th.USF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5UDFSCREENFIELDS as  th ;
                     