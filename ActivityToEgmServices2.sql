WITH body_mod_data
AS
(
--EGM Services
SELECT SPLIT_PART(PK_ACTIVITY_ID,'|', 3) AS clm_id 
	, SPLIT_PART(PK_ACTIVITY_ID,'|', 4) AS line_num
	, FK_PATIENT_ID AS bene_key ---for Change, there's a hash for a2024, there's an org_src_id prefix
	, CASE 
		WHEN regexp_like(PROCEDURE_HCPCS_MOD_CD_LIST[0],'E[1-4]|FA|F[1-9]|L[CDT]|R[CT]|TA|T[1-9]') THEN trim(PROCEDURE_HCPCS_MOD_CD_LIST[0])
		WHEN regexp_like(PROCEDURE_HCPCS_MOD_CD_LIST[1],'E[1-4]|FA|F[1-9]|L[CDT]|R[CT]|TA|T[1-9]') THEN trim(PROCEDURE_HCPCS_MOD_CD_LIST[1])
		WHEN regexp_like(PROCEDURE_HCPCS_MOD_CD_LIST[2],'E[1-4]|FA|F[1-9]|L[CDT]|R[CT]|TA|T[1-9]') THEN trim(PROCEDURE_HCPCS_MOD_CD_LIST[2])
	  END AS body_mod
	, trim(PROCEDURE_HCPCS_MOD_CD_LIST[0]) AS mod_1
	, trim(PROCEDURE_HCPCS_MOD_CD_LIST[1]) AS mod_2
	, claim_line_allowed_amt AS pay
	, FACILITY_PLACE_OF_SERVICE_CD AS pos_cd
	, FACILITY_NPI_NUM AS prf_at_grp_npi
	, SPLIT_PART(fk_provider_primary_id,'|',2) AS prf_op_physn_npi  --etls show primary ID is best as it already considers claim type
	, split_part(fk_diagnosis_id_list[0],'|',2 )AS primary_dx
	, PROVIDER_RENDERING_SPECIALTY_CD AS prov_spec
	, CASE
		WHEN claim_type_cd IN ('40', '60',  '61') THEN facility_ccn_num
		WHEN claim_type_cd = '10' THEN facility_ccn_num
		ELSE SPLIT_PART(fk_provider_primary_id,'|',2)
	  END AS providers
	, FACILITY_REVENUE_CENTER_CD AS rev_cd

	, CASE 
		WHEN claim_type_cd = '40' THEN 'op'
		WHEN claim_type_cd IN ('70','71') THEN 'pb'
		WHEN claim_type_cd IN ('60', '61') THEN 'ip'
		WHEN claim_type_cd = '10' THEN 'hh'
		-- other cases '01' '02' is med
	  END AS src_file	
	, CASE 
		WHEN ACTIVITY_TYPE_CD = 'fac_proc'
			THEN coalesce(NULLIF(PROCEDURE_ICD_9_CD,'#NA'), NULLIF(PROCEDURE_ICD_9_CD,'#NA'))
		WHEN ACTIVITY_TYPE_CD = 'med'
			THEN MEDICATION_NDC_SPL_CD	--is MEDICATION_HCPCS_CD useful? is it used in Services?
		WHEN ACTIVITY_TYPE_CD = 'dme'	
			THEN DME_HCPCS_CD
		ELSE PROCEDURE_HCPCS_CD 			
	  END AS src_code	
	, activity_from_dt AS svc_from_dt
	, activity_thru_dt AS svc_thru_dt
	
--supporting data	
	, FK_PROCEDURE_ID 
	, PROCEDURE_HCPCS_CD 
	, fk_diagnosis_id_list
	, PROCEDURE_HCPCS_MOD_CD_LIST[2] AS mod3
	, PROCEDURE_HCPCS_MOD_CD_LIST 	
	, PROCEDURE_ICD_9_CD 
	, PROCEDURE_ICD_10_CD 
	, MEDICATION_NDC_SPL_CD	
	, MEDICATION_HCPCS_CD
	, DME_HCPCS_CD
	, claim_type_cd
  	, ACTIVITY_TYPE_CD 
  	, PK_ACTIVITY_ID 
FROM insights.ACTIVITY 
WHERE 
ACTIVITY_TYPE_CD <> 'med'
)  
SELECT *  FROM body_mod_data 
WHERE claim_type_cd = '40'
fk_diagnosis_id_list[0] <> '#NA'
body_mod IS NOT NULL


-- do we need 
-- CPT modifiers
-- The two code sets are so similar, in fact, that you can regularly use modifiers from one codeset to the other. The HCPCS modifier –LT, for example, is regularly used in CPT codes when you need to describe a bilateral procedure that was only performed on one side of the body.
-- HCPCS modifiers, like CPT modifiers, are always two characters, and are added to the end of a HCPCS or CPT code with a hyphen. When differentiating between a CPT modifier and a HCPCS modifier, all there’s one simple rule: if the modifier has a letter in it, it’s a HCPCS modifier. If that modifier is entirely numeric, it’s a CPT modifier.




--HCPCS modifiers

-- E1: upper left eyelid
-- E2: lower left eyelid
-- E3: upper right eyelid
-- E4: lower right eyelid
-- FA: left hand, thumb
-- F1: left hand, second digit 
-- F2: left hand, third digit 
-- F3: left hand, fourth digit 
-- F4: left hand, fifth digit 
-- F5: right hand, thumb
-- F6: right hand, second digit 
-- F7: right hand, third digit 
-- F8: right hand, fourth digit 
-- F9: right hand, fifth digit

-- LC: left circumflex coronary artery
-- LD: left anterior descending coronary artery
-- LT: left side (used to identify procedures performed on the left side of the body)

-- RC: right coronary artery
-- RT: right side
-- (used to identify procedures performed on the right side of the body)
-- TA: left foot, great toe
-- T1: left foot, second digit 
-- T2: left foot, third digit 
-- T3: left foot, fourth digit 
-- T4: left foot, fifth digit
-- T5: right foot, great toe
-- T6: right foot, second digit 
-- T7: right foot, third digit 
-- T8: right foot, fourth digit 
-- T9: right foot, fifth digit


-- GG: performance and payment of a screening mammogram
-- and diagnostic mammogram on the same patient, same day
-- GH: diagnostic mammogram converted from screening mammogram on same day
-- QM: ambulance service provided under arrangement by a provider of services
-- QN: ambulance service furnished directly by a provider of services