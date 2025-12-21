




clear all
set varabbrev off

*-------------------------------------------------------------------------------
* 1. Settings and helper function
*-------------------------------------------------------------------------------

global CLEAN_FREQ 5        // For efficiency reasons, run dedupe function every x years

* ------ Define Cleaning Function

capture program drop run_cleanup
program define run_cleanup
    di as result "Cleaning and Deduping..."
    
    * Standardize CPF
    capture confirm string variable cpf
	if !_rc di "cpf is string"
	gen cpf2 = string(real(cpf), "%011.0f")
	drop cpf 
	rename cpf2 cpf
    
    * Standardize DOB
    capture confirm string variable dob
	if !_rc di "dob is string"
	gen dob2 = string(real(dob), "%08.0f")
	drop dob 
	rename dob2 dob 

    * Dedupe
    capture confirm string variable pis
	if !_rc di "pis is string"
	
    g k = pis + cpf + dob
    bys k: keep if _n==1
    drop k
end

*-------------------------------------------------------------------------------
* 2. Loading
*-------------------------------------------------------------------------------

tempfile snippet main_data

* --- Load 2002 ---
if $LOW_RAM_MODE == 1 use "$PATH/2002_temp.dta" in 1/50000, clear // If Low RAM mode, just 50,000 rows
else use "$PATH/2002_temp.dta", clear


* --- Remaining Years
forvalues y = 2003/$LAST_YEAR {
    
    di as text "Processing `y'..."

    if $LOW_RAM_MODE == 1 {
        save `main_data', replace
        use "$PATH/`y'_temp.dta" in 1/50000, clear
        save `snippet', replace
        use `main_data', clear
        append using `snippet'
    }
    else {
        append using "$PATH/`y'_temp.dta"
    }

    * Cleanup logic every CLEAN_FREQ years
    if mod(`y', $CLEAN_FREQ) == 0 | `y' == $LAST_YEAR {
        run_cleanup
    }
}

di as result "Done loading."


*-------------------------------------------------------------------------------
* 3. Rest of code as is
*-------------------------------------------------------------------------------



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

ren n_cpf_for_pis ndistinct

drop if cpf == "00000000000" & ndistinct > 1
drop tag ndistinct

keep if one_to_one_cpf_pis == 1

save "$PATH/one_to_one_cpf_pis_temp_CHECK.dta", replace

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
merge n:1 pis using "$PATH/pis_cpf_dob_name_matching_CHECK.dta"
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

keep pis name cpf dob one_to_one_cpf_pis cpf_imputed freq pis_imputed dob_imputed 
keep cpf_imputed pis_imputed dob_imputed name one_to_one_cpf_pis freq pis

save "$PATH/not_one_to_one_cpf_pis_temp_CHECK.dta", replace
append using "$PATH/one_to_one_cpf_pis_temp_CHECK.dta"
replace cpf_imputed = cpf if one_to_one_cpf_pis == 1
replace pis_imputed = pis if one_to_one_cpf_pis == 1
replace dob_imputed = dob if one_to_one_cpf_pis == 1
replace freq = 1 if one_to_one_cpf_pis == 1

keep cpf_imputed pis pis_imputed dob_imputed name one_to_one_cpf_pis freq
order pis pis_imputed cpf_imputed dob_imputed name one_to_one_cpf_pis freq
rename name name_imputed

save "$PATH/pis_cpf_dob_name_matching_final_CHECK.dta", replace
