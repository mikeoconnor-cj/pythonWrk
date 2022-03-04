USE Warehouse prod_adaugeopi;                                       
USE DATABASE prod_adaugeopi;                                       
                                       
WITH dupCTE
    AS
    (
    SELECT 'patient_x_chronic_condition_month' AS tableName
        , sum(rowsDuped) AS rowsEffected
        , count(*) AS pkRowCount
    FROM
        (
        SELECT chronic_condition_source_cd,
               fk_patient_id,
               measure_id,
               month_cd
            , count(*) AS rowsDuped 
        FROM INSIGHTS.patient_x_chronic_condition_month
        --WHERE CHRONIC_CONDITION_SOURCE_CD <> 'assgn_hcc' --all related to these
        GROUP BY chronic_condition_source_cd,
               fk_patient_id,
               measure_id,
               month_cd
        HAVING count(*) > 1
        ) a 
    ) 
    SELECT tableName 
        , 'prod_adaugeopi' AS orgDBName
        , rowsEffected
        , pkRowCount
    FROM dupCTE           
    

--error log
--A2575_m-2022-01_2022-02-25T20:52:03.220458_40980  load_stage_table -> cclf_7_v26        | failed 
--A2575_m-2022-01_2022-02-25T20:52:05.533755_40981  load_stage_table -> cclf_7_v26        | failed

SELECT date(load_ts) AS load_date
  , SSF_FILE_ID
  , count(*) AS rwCnt
FROM prod_a2575.STG.SSF_CCLF_7_V26 
WHERE date(load_ts) > '2022-02-01'
GROUP BY date(load_ts), SSF_FILE_ID
ORDER BY date(load_ts) DESC

LOAD_DATE SSF_FILE_ID RWCNT
2022-02-24  40,980    120,082
2022-02-24  40,981    816,649



--error log
--A2841_m-2022-01_2022-02-28T18:17:37.473197_41089_0 load_ods_table -> cclf_7_v26          | failed
--A2841_m-2022-01_2022-02-28T18:42:43.825841_41090  load_stage_table -> cclf_7_v26        | success 
--A2841_m-2022-01_2022-02-28T18:42:43.825841_41090_0 load_ods_table -> cclf_7_v26          | failed 

SELECT date(load_ts) AS load_date
  , SSF_FILE_ID
  , count(*) AS rwCnt
FROM prod_a2841.STG.SSF_CCLF_7_V26 
WHERE date(load_ts) > '2022-02-01'
GROUP BY date(load_ts), SSF_FILE_ID
ORDER BY date(load_ts) DESC

LOAD_DATE SSF_FILE_ID RWCNT
2022-02-28  41,089    725,830
2022-02-28  41,090    34,837
2022-02-24  40,917    34,837
2022-02-24  40,918    725,830



--error log
--A3327_m-2022-01_2022-02-25T18:12:00.621451_40859  load_stage_table -> cclf_5_v27 

SELECT date(load_ts) AS load_date
  , SSF_FILE_ID
  , count(*) AS rwCnt
FROM prod_a3327.STG.SSF_CCLF_5_V27 
WHERE date(load_ts) > '2022-02-01'
GROUP BY date(load_ts), SSF_FILE_ID
ORDER BY date(load_ts) DESC

LOAD_DATE SSF_FILE_ID RWCNT
2022-02-25  40,860    5,135,179
2022-02-17  40,859    330,184

--the usual member month report... only shows 2022 data for Adaugeo and ilumed
USE DATABASE prod_adaugeopi;    
USE DATABASE prod_ilumedpi;

Select org_id   
    , period_id 
    , measure_value_decimal AS totlForGroup 
from insights.metric_value_operational_dashboard  
where measure_cd = 'total_member_years_current_month' 
    and patient_medicare_group_cd = '#NA' 
    and org_level_category_cd = 'aco'
    and attribution_type = 'as_was'
    and substr(period_id,3,7) >= '2019-01' 
order by ORG_GROUP_ID 
    ,  period_id     
    
    
USE DATABASE prod_cityblockdce;    --no data either attrib type
USE DATABASE prod_canodce;

Select org_id   
    , period_id 
    , measure_value_decimal AS totlForGroup 
from insights.metric_value_operational_dashboard  
where measure_cd = 'total_member_years_current_month' 
    and patient_medicare_group_cd = '#NA' 
    and org_level_category_cd = 'aco'
    --and attribution_type = 'as_was'
    and substr(period_id,3,7) >= '2019-01' 
order by ORG_GROUP_ID 
    ,  period_id       
 