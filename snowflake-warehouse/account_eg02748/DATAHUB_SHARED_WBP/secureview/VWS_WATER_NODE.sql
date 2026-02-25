CREATE OR REPLACE SECURE VIEW ${buildvar.env}_WBP_SHARE.DATAHUB_SHARED_WBP.VWS_WATER_NODE
AS
SELECT 
  A1."ACQTYPE" AS "Acquistion Type"
, A2."ADDRKEY" AS "Address"
, A2."UNITID" AS "Asset ID"
, A2."COMPKEY" AS "CompKey"
, A1."COSTCENTRE" AS "Cost Centre Code"
, 'TBD' AS "Cost Centre Description"
, A1."CRITICALITY" AS "Criticality Code"
, 'TBD' AS "Criticality Description"
, A2."UNITDESC" AS "Description"
, A1."FORDISPOSAL" AS "Disposal Reason"
, A1."DRAWINGNO" AS "Drawing No"
, A1."DRAWINGNOREF" AS "Drawing No Ref"
, A2."BGTNO" AS "Equipment No"
, A2."EXPDATE" AS "Expire Date"
, A2."AREA" AS "Facility Code"
, 'TBD' AS "Facility Description"
, A2."INSTDATE" AS "Installed"
, A2."ASBLT" AS "Legacy Key"
, A1."MODELNO" AS "Model #"
, A2."OWN" AS "Ownership Code"
, 'TBD' AS "Ownership Description"
, A2."MAPNO" AS "Position"
, A2."SUBAREA" AS "Process Code"
, 'TBD' AS "Process Description"
, A1."SAFETYCRITICAL" AS "Safety Critical Element"
, A2."SERVSTAT" AS "Service Status"
, A2."LOC" AS "Set Code"
, 'TBD' AS "Set Description"
, A2."DISTRICT" AS "System Code"
, 'TBD' AS "System Description"
, A2."UNITTYPE" AS "Unit Type Code"
, 'TBD' AS "Unit Type Description"

, ad."NZHOUSENO" AS "Street Number"
, CONCAT(ad."NZSTNAME",' ',ad."NZSTTYPE") AS "Street Name"
, ad."NZSUBURB" AS "City"
, ad."NZPOSTCODE" AS "Postal Code"

, A1.MAKE                   AS "Make"
, A2.SERNO                  AS "Serial Number"
, A1.ZONECATCHMENT          AS "Water Zone/Sewer Catchment"
, A1.WARRANTYEND            AS "Warranty End Date"
, A1.WARRANTYSTART          AS "Warranty Start Date"

FROM
  "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET".IPS_ASSETMANAGEMENT_WATER_COMPWND as A2
  INNER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET".IPS_WSLASSETS_XALLASSETSDP02 as A1 ON A2.COMPKEY = A1.COMPKEY AND A1.BUSINESSAREA IN ('WBC','WBE','WBW')
  LEFT OUTER JOIN "${buildvar.env}_WCC_DATAWAREHOUSE"."DATAHUB_TARGET".IPS_PROPERTY_ADDRESS as ad ON ad.ADDRKEY = a2.ADDRKEY AND ad.OPTA = 'WBP'
  WHERE A2.BLDGFLOOR = 1003;