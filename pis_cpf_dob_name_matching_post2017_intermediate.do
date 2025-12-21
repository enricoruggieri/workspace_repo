*2021 has DOB missing, only year of birth; will deal with it later

global PATH "~/Documents/rais_after_2017_combined"

use "$PATH/all_temp.dta", clear

* ---------- Normalize identifiers ----------
gen cpf2 = string(real(cpf), "%011.0f")
drop cpf 
rename cpf2 cpf
gen dob2 = string(real(dob), "%08.0f")
drop dob 
rename dob2 dob 
* ---------- Collapse to unique (pis,cpf) pairs with counts ----------
egen tag2 = tag(pis cpf)
egen ndistinct2 = total(tag2), by(cpf)
drop if ndistinct2 >= 300

g id = cpf + pis
egen n_match = count(pis), by (id)
egen n_pis = count(pis), by (pis)

* ---------- Choose modal CPF per PIS with strict majority ----------
bys id: keep if _n==1
g frequency = n_match/n_pis
egen max_frequency = max(frequency), by(pis) 
g keep = 1 if frequency == max_frequency
replace keep = 0 if keep == .

* strict majority: ties at 0.5 are dropped
replace keep = 0 if max_frequency < .501
keep if keep == 1
drop keep n_match n_pis max_frequency
g cpf_imputed = cpf

g dob2 = dob
gen byte nz = dob != "."
bys pis (nz): gen strL _fill = dob if nz          // keep only non-"." rows
bys pis: replace _fill = _fill[_N]                 // broadcast group's non-"." value
bys pis: replace dob2 = _fill if _fill != ""       // assign to all rows in the group
drop nz _fill dob tag ndistinct
rename dob2 dob

save pis_cpf_matching.dta, replace
