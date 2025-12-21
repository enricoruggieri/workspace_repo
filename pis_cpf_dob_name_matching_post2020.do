*** select new 2021 workers
*2021 has DOB missing, only year of birth; will deal with it later
global PATH "~/Documents/rais_after_2017_combined"

*put all years together
use "$PATH/combined_2021.dta", clear
keep nometrabalhador pis cpf 
g name = nometrabalhador
*g dob = datadenascimento
keep name pis cpf  
duplicates drop

*get rid of missing pis for the same cpf
g pis2 = pis
replace pis2 = "1" if pis == ""

egen tag = tag(pis2 cpf)
egen ndistinct = total(tag), by(cpf) //number of PIS for the same CPF
drop if pis == "" & ndistinct > 1
drop pis2 

gen cpf2 = string(real(cpf), "%011.0f")
drop cpf 
rename cpf2 cpf
*gen dob2 = string(real(dob), "%08.0f")
*drop dob 
*rename dob2 dob 

g k = pis + cpf
bys k: keep if _n==1
drop k

save all_2021_temp.dta, replace

merge n:1 pis using pis_cpf_dob_name_matching_final
keep if _merge == 1
drop _merge
merge n:1 pis using pis_cpf_dob_name_matching_2018_2020
keep if _merge == 1

keep if pis == ""
keep pis name cpf 
duplicates drop

save missing_pis.dta, replace 

use "$PATH/all_2021_temp.dta", clear

merge n:1 pis using pis_cpf_dob_name_matching_final
keep if _merge == 1
drop _merge
merge n:1 pis using pis_cpf_dob_name_matching_2018_2020
keep if _merge == 1

drop if pis == ""
keep pis 

save new_workers_post_2020.dta, replace
***************************************************************
use "$PATH/combined_2021.dta", clear
keep nometrabalhador pis cpf 
g name = nometrabalhador
*g dob = datadenascimento
keep name pis cpf  
drop if pis == ""
duplicates drop

merge n:n pis using new_workers_post_2020
keep if _merge == 3
drop _merge

save new_workers_post_2020_full.dta, replace
**************************************************************
use new_workers_post_2020_full.dta, clear

gen cpf2 = string(real(cpf), "%011.0f")
drop cpf 
rename cpf2 cpf
*gen dob2 = string(real(dob), "%08.0f")
*drop dob 
*rename dob2 dob 

g k = pis + cpf  
bys k: keep if _n==1
drop k

*match based on pis
sort pis
drop if pis == ""

egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis) //number of CPFs for the same PIS
drop if cpf == "00000000000" & ndistinct > 1 //drop null cpf if there's more than one per pis
drop tag ndistinct

*g k = pis + cpf
*egen n_k = count(k), by(k)
*drop if dob == "." & n_k > 1 //drop null dob if appear more than once (by pis + cpf)
*drop k n_k

egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000099" & ndistinct > 1
drop tag ndistinct

*delete entries with same cpf and pis with different DOBs
g k = pis + cpf
bys k: keep if _n==1
drop k

save "$PATH/all_temp_2021.dta", replace

g id = name + cpf
egen tag = tag(id pis)
egen ndistinct = total(tag), by(pis)

g unique_cpf_match = 1 if ndistinct == 1
replace unique_cpf_match = 0 if unique_cpf_match == .
g k = 1
drop tag ndistinct

egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)

g unique_cpf = 1 if ndistinct == 1
replace unique_cpf = 0 if unique_cpf == .

egen tag2 = tag(pis cpf)
egen ndistinct2 = total(tag2), by(cpf)

g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (ndistinct2 == 1 | cpf == "00000000000")
replace one_to_one_cpf_pis = 0 if one_to_one_cpf_pis == .

rename ndistinct2 n_pis_for_cpf
rename ndistinct n_cpf_for_pis
*
egen min_cpf_for_pis = min(n_cpf_for_pis), by(cpf) 
g w = 1 if n_cpf_for_pis == 2 & n_pis_for_cpf == 2 & min_cpf_for_pis == 1 
drop if w == 1
drop w
*
drop one_to_one_cpf_pis
g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (n_pis_for_cpf == 1 | n_pis_for_cpf >= 300)
replace one_to_one_cpf_pis = 0 if one_to_one_cpf_pis == .
replace cpf = "00000000000" if n_pis_for_cpf >= 300

drop tag
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000000" & ndistinct > 1
drop tag ndistinct

keep if one_to_one_cpf_pis == 1

save one_to_one_cpf_pis_temp.dta, replace

************
use "$PATH/all_temp_2021.dta", clear

g id = name + cpf 
egen tag = tag(id pis)
egen ndistinct = total(tag), by(pis)
g unique_cpf_match = 1 if ndistinct == 1
replace unique_cpf_match = 0 if unique_cpf_match == .
drop tag ndistinct
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
g unique_cpf = 1 if ndistinct == 1
replace unique_cpf = 0 if unique_cpf == .
egen tag2 = tag(pis cpf)
egen ndistinct2 = total(tag2), by(cpf)
rename ndistinct2 n_pis_for_cpf
rename ndistinct n_cpf_for_pis

g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (n_pis_for_cpf == 1 | n_pis_for_cpf >= 300)
replace one_to_one_cpf_pis = 0 if one_to_one_cpf_pis == .
replace cpf = "00000000000" if n_pis_for_cpf >= 300
*egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000000" & ndistinct > 1
drop tag ndistinct

keep if one_to_one_cpf_pis != 1
merge n:1 pis using pis_cpf_matching
drop if _merge == 2
keep if cpf == cpf_imputed

drop _merge n_pis_for_cpf n_cpf_for_pis unique_cpf
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)

g unique_cpf = 1 if ndistinct == 1
replace unique_cpf = 0 if unique_cpf == .

g pis_imputed = pis
gen first_name = substr(name, 1, strpos(name, " ") - 1) 
replace first_name = trim(first_name)

drop tag
egen tag = tag(first_name cpf)
egen n_names_for_cpf = total(tag), by(cpf)

drop pis_imputed
bys cpf_imputed: generate pis_imputed=pis[1]
gen n_zeros = length(cpf) - length(subinstr(cpf, "0", "", .))
drop if n_zeros > 5

keep if n_names_for_cpf == 1
bys cpf_imputed: generate dob_imputed=dob[1]
gen l_name = length(name)
egen max_l_name = max(l_name), by(cpf_imputed) 
keep if l_name == max_l_name
*bys cpf_imputed: keep if _n==1

keep pis name cpf one_to_one_cpf_pis cpf_imputed frequency pis_imputed
keep cpf_imputed pis_imputed name one_to_one_cpf_pis frequency pis

save not_one_to_one_cpf_pis_temp.dta, replace
append using one_to_one_cpf_pis_temp
replace cpf_imputed = cpf if one_to_one_cpf_pis == 1
replace pis_imputed = pis if one_to_one_cpf_pis == 1
*replace dob_imputed = dob if one_to_one_cpf_pis == 1
replace frequency = 1 if one_to_one_cpf_pis == 1

keep cpf_imputed pis pis_imputed name one_to_one_cpf_pis frequency
order pis pis_imputed cpf_imputed name one_to_one_cpf_pis frequency
rename name name_imputed
*************
*drop if pis == ""
*keep if dob_imputed == "."
*g cpf = cpf_imputed
*merge n:n cpf using new_workers_post_2017_full
*keep if _merge == 3
*g dob2 = dob
*gen byte nz = dob != "."
*bys cpf (nz): gen strL _fill = dob if nz          // keep only non-"." rows
*bys cpf: replace _fill = _fill[_N]                 // broadcast group's non-"." value
*bys cpf: replace dob2 = _fill if _fill != ""       // assign to all rows in the group
*replace dob_imputed = dob2 if dob_imputed == "."
*drop cpf name dob _merge dob2 nz 
*g dob2 = dob_imputed
*gen byte nz = dob_imputed != "."
*bys cpf_imputed (nz): gen strL _fill = dob_imputed if nz          // keep only non-"." rows
*bys cpf_imputed: replace _fill = _fill[_N]                 // broadcast group's non-"." value
*bys cpf_imputed: replace dob2 = _fill if _fill != ""       // assign to all rows in the group
*replace dob_imputed = dob2 if dob_imputed == ""
*duplicates drop
*drop dob2 nz _fill

save pis_cpf_dob_name_matching_2021.dta, replace

g cpf = cpf_imputed
merge n:1 cpf using missing_pis.dta

drop if _merge == 2
drop cpf name _merge

save pis_cpf_dob_name_matching_2021.dta, replace

use pis_cpf_dob_name_matching_2018_2020, clear
g cpf = cpf_imputed
merge n:1 cpf using missing_pis.dta

keep if _merge == 3
drop cpf name _merge
append using pis_cpf_dob_name_matching_2021.dta
duplicates drop

save pis_cpf_dob_name_matching_2021.dta, replace

use pis_cpf_dob_name_matching_final, clear
g cpf = cpf_imputed
merge n:1 cpf using missing_pis.dta

keep if _merge == 3
drop cpf name _merge
append using pis_cpf_dob_name_matching_2021.dta
duplicates drop

save pis_cpf_dob_name_matching_2021.dta, replace

***check
use missing_pis.dta, clear
g cpf_imputed = cpf
merge 1:n cpf_imputed using pis_cpf_dob_name_matching_2021.dta

keep if _merge == 1
*only 200k not found!






