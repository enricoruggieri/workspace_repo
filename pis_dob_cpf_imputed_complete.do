
global PATH "~/Library/CloudStorage/Box-Box/rais_pia/data/rais/intermediate/original files"

use "$PATH/2002_temp.dta", clear
append using "$PATH/2003_temp.dta"
append using "$PATH/2004_temp.dta"
append using "$PATH/2005_temp.dta"
append using "$PATH/2006_temp.dta"
append using "$PATH/2007_temp.dta"
append using "$PATH/2008_temp.dta"
append using "$PATH/2009_temp.dta"
append using "$PATH/2010_temp.dta"

gen cpf2 = string(real(cpf), "%011.0f")
drop cpf 
rename cpf2 cpf
gen dob2 = string(real(dob), "%08.0f")
drop dob 
rename dob2 dob 

g k = pis + cpf + dob
bys k: keep if _n==1
drop k

append using "$PATH/2011_temp.dta"
append using "$PATH/2012_temp.dta"
append using "$PATH/2013_temp.dta"
append using "$PATH/2014_temp.dta"
append using "$PATH/2015_temp.dta"
append using "$PATH/2016_temp.dta"
append using "$PATH/2017_temp.dta"


gen cpf2 = string(real(cpf), "%011.0f")
drop cpf 
rename cpf2 cpf
gen dob2 = string(real(dob), "%08.0f")
drop dob 
rename dob2 dob 

g k = pis + cpf + dob
bys k: keep if _n==1
drop k

sort pis
drop if pis == "0"

egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis) //number of CPFs for the same PIS
drop if cpf == "00000000000" & ndistinct > 1 //drop null cpf if there's more than one per pis
drop tag ndistinct

g k = pis + cpf
egen n_k = count(k), by(k)
drop if dob == "." & n_k > 1 //drop null dob if appear more than once (by pis + cpf)
drop k n_k

egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000099" & ndistinct > 1
drop tag ndistinct

*delete entries with same cpf and pis with different DOBs
g k = pis + cpf
bys k: keep if _n==1
drop k

save "$PATH/all_temp.dta", replace


* fix above still leaves ~1% of pis with associated null cpf
g k = 1
g m = 1 if cpf == "00000000000"
collapse (sum) m k, by(pis) fast
g g = 1
collapse (sum) m k g, fast

*breakdown of multiple cpfs by pis: 91% are unique
use "$PATH/all_temp.dta", clear
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
g k = 1
collapse (sum) k, by(ndistinct) fast

* 
use "$PATH/all_temp.dta", clear
egen tag = tag(pis cpf)
egen ndistinct = total(tag), by(cpf)
g k = 1
collapse (sum) k, by(ndistinct) fast
*way more multiple cpfs per pis

use "$PATH/all_temp.dta", clear
g id = name + cpf + dob
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
*drop tag
*g id = name + cpf + dob
*egen tag = tag(id pis)
*egen ndistinct = total(tag), by(pis)
*g unique_cpf_match = 1 if ndistinct == 1
*replace unique_cpf_match = 0 if unique_cpf_match == .
*drop tag ndistinct
*egen tag = tag(cpf pis)
*egen ndistinct = total(tag), by(pis)
*g unique_cpf = 1 if ndistinct == 1
*replace unique_cpf = 0 if unique_cpf == .
*egen tag2 = tag(pis cpf)
*egen ndistinct2 = total(tag2), by(cpf)
*rename ndistinct2 n_pis_for_cpf
*rename ndistinct n_cpf_for_pis
*g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (n_pis_for_cpf == 1 | cpf == "00000000000" | cpf == "00000000099" | cpf == "12345678909" | cpf == "98765432100" | cpf == "00000000191" | cpf == "11111111111")
drop one_to_one_cpf_pis
g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (n_pis_for_cpf == 1 | n_pis_for_cpf >= 300)
replace one_to_one_cpf_pis = 0 if one_to_one_cpf_pis == .
replace cpf = "00000000000" if n_pis_for_cpf >= 300

*egen tag = tag(cpf pis)
*egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000000" & ndistinct > 1
drop tag ndistinct

keep if one_to_one_cpf_pis == 1

save one_to_one_cpf_pis_temp.dta, replace

************
use "$PATH/all_temp.dta", clear

g id = name + cpf + dob
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

keep pis name cpf dob one_to_one_cpf_pis cpf_imputed frequency pis_imputed dob_imputed 
keep cpf_imputed pis_imputed dob_imputed name one_to_one_cpf_pis frequency pis

save not_one_to_one_cpf_pis_temp.dta, replace
append using one_to_one_cpf_pis_temp
replace cpf_imputed = cpf if one_to_one_cpf_pis == 1
replace pis_imputed = pis if one_to_one_cpf_pis == 1
replace dob_imputed = dob if one_to_one_cpf_pis == 1
replace frequency = 1 if one_to_one_cpf_pis == 1

keep cpf_imputed pis pis_imputed dob_imputed name one_to_one_cpf_pis frequency
order pis pis_imputed cpf_imputed dob_imputed name one_to_one_cpf_pis frequency
rename name name_imputed

save pis_cpf_dob_name_matching_final.dta, replace
