



clear all
set varabbrev off
set more off

*-------------------------------------------------------------------------------
* 1. Settings and defining helper functions
*-------------------------------------------------------------------------------

global CLEAN_FREQ 5        // For efficiency reasons, run dedupe function every x years

* --- Func Standardize & Dedupe ---
capture program drop run_cleanup
program define run_cleanup
    di as result ">>> [Cleanup] Standardizing Types and Deduping..."
    
    * Standardize CPF to 11 digits string
    capture confirm string variable cpf
    if !_rc di "    cpf is already string"
    gen cpf2 = string(real(cpf), "%011.0f")
    drop cpf 
    rename cpf2 cpf
    
    * Standardize DOB to 8 digits string
    capture confirm string variable dob
    if !_rc di "    dob is already string"
    gen dob2 = string(real(dob), "%08.0f")
    drop dob 
    rename dob2 dob 

    * Dedupe based on full key (PIS + CPF + DOB)
    capture confirm string variable pis
    if !_rc di "    pis is already string"
    g k = pis + cpf + dob
    bys k: keep if _n==1
    drop k
end

* --- Function CPF Official Mathematical Validation ---
capture program drop run_cpf_validation
program define run_cpf_validation
    di as result ">>> [Validation] Running Mathematical CPF Check..."
    
    * Generate temporary digits
    gen cpf_str = string(real(cpf), "%011.0f")
    forvalues i = 1/11 {
        gen d`i' = real(substr(cpf_str, `i', 1))
    }
    
    * First Digit Check
    gen sum1 = 0
    forvalues i = 1/9 {
        replace sum1 = sum1 + d`i' * (11 - `i')
    }
    gen r1 = mod(sum1, 11)
    gen digit10 = cond(r1 < 2, 0, 11 - r1)
    
    * Second Digit Check
    gen sum2 = 0
    forvalues i = 1/9 {
        replace sum2 = sum2 + d`i' * (12 - `i')
    }
    replace sum2 = sum2 + digit10 * 2
    gen r2 = mod(sum2, 11)
    gen digit11 = cond(r2 < 2, 0, 11 - r2)
    
    * Validation Logic
    gen cpf_is_valid = (d10 == digit10 & d11 == digit11)
    
    * Check for "all same" digits
    gen all_same = 1 if cpf_is_valid == 1
    forvalues i = 2/11 {
        replace all_same = 0 if d`i' != d1 & all_same == 1
    }
    replace cpf_is_valid = 0 if all_same == 1
    
	tab cpf_is_valid
	
    * Apply Filter
    *keep if cpf_is_valid == 1
    
    * Clean up calculation variables
    drop d1-d11 sum1 sum2 r1 r2 digit10 digit11 all_same cpf_str 
    *cpf_is_valid
    
    di as result "Validation Complete. Invalid CPFs dropped."
end

*-------------------------------------------------------------------------------
* 2. Loading Data (Refactored for 2020/2021 Support)
*-------------------------------------------------------------------------------
tempfile main_data snippet

* Build File list
* ------------------------------------------------------------------------------
* We construct a list of filenames first
* to deal with the irregularity of the 2018-2020 block file.
local file_list ""

* Standard Years (2002 - 2017)
local end_std = min($LAST_YEAR, 2017)
forvalues y = 2002/`end_std' {
    local file_list "`file_list' `y'_temp.dta"
}

* 2018-2020 Block
if $LAST_YEAR >= 2018 {
    local file_list "`file_list' all_temp_2018_2020.dta"
}

* 2021
if $LAST_YEAR >= 2021 {
    local file_list "`file_list' all_temp_2021.dta"
}


* Execute load and clean loop
* ------------------------------------------------------------------------------
local i = 0
local total_files : word count `file_list'

foreach f in `file_list' {
    local i = `i' + 1
    di as text "Processing file `i' of `total_files': `f'..."

    * --- STEP A: LOAD THE FILE (Snippet or Full) ---
    if $LOW_RAM_MODE == 1 {
        use "$PATH/`f'" in 1/50000, clear
    }
    else {
        use "$PATH/`f'", clear
    }

    * Standardize 
    capture rename nometrabalhador name
    capture rename datadenascimento dob
    
    * Sanity Check
    capture confirm variable dob
    if _rc != 0 {
        di as error "File `f' is missing 'dob' variable."
    }

    * --- Append to main ---
    if `i' == 1 {
        save `main_data', replace
    }
    else {
        save `snippet', replace
        
        use `main_data', clear
        append using `snippet'
        
        save `main_data', replace
    }

    * Run cleanup if: 
    * 1. Every X files)
    * 2. It is the very last file

    
    if mod(`i', $CLEAN_FREQ) == 0 | `i' == `total_files' {
        run_cleanup
    }
}

di as result "Data Loading Complete."

*-------------------------------------------------------------------------------
* 3. Primary Cleaning & Filtering
*-------------------------------------------------------------------------------

* Remove invalid PIS entries
sort pis
drop if pis == "0"

run_cpf_validation
* --- Optional CPF Validation ---
if $KEEP_ONLY_VALID_CPF == 1 {
    *run_cpf_validation
    keep if cpf_is_valid == 1
    
}

* --- CLEANING BAD LINKS ---

* Remove null CPFs (000...) if the PIS has other valid CPFs
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis) 
drop if cpf == "00000000000" & ndistinct > 1 
drop tag ndistinct

* Remove null DOBs if PIS+CPF combination appears elsewhere with valid DOB
g k = pis + cpf

** IMPROVEMENT SUGGESTION
* egen k = group (pis cpf) //THIS WOULD BE better

egen n_k = count(k), by(k)
drop if dob == "." & n_k > 1 
drop k n_k


* Remove specific placeholder CPF (ending in 99) if duplicates exist
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000099" & ndistinct > 1
drop tag ndistinct

** IMPROVEMENT SUGGESTION

* Remove invalid CPFs if there is a valid CPF
*by pis: egen has_valid_cpf = max(cpf_is_valid == 1)
*drop if cpf_is_valid == 0 & ndistinct_cpf > 1 & has_valid_cpf == 1

* Consolidate: Keep first occurrence of PIS+CPF (dropping inconsistent DOBs)
g k = pis + cpf

** IMPROVEMENT SUGGESTION
* egen k = group (pis cpf) //THIS WOULD BE better
* OR gen k = pis + "_" + cpf
* gsort k - (dob != ".")   // put non-missing DOB first

bys k: keep if _n==1
drop k

save "$PATH/all_temp.dta", replace

*-------------------------------------------------------------------------------
* 4. Relationship Analysis
*-------------------------------------------------------------------------------
* FUTURE: IMPROVE re-loading the data. 

* Diagnostic: Fix above leaves ~1% of pis with associated null cpf
use "$PATH/all_temp.dta", clear
g k = 1
g m = 1 if cpf == "00000000000"
collapse (sum) m k, by(pis) fast
g g = 1
collapse (sum) m k g, fast

* Diagnostic: Breakdown of multiple CPFs by PIS
use "$PATH/all_temp.dta", clear
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
g k = 1
collapse (sum) k, by(ndistinct) fast

* Diagnostic: Breakdown of multiple PIS by CPF
use "$PATH/all_temp.dta", clear
egen tag = tag(pis cpf)
egen ndistinct = total(tag), by(cpf)
g k = 1
collapse (sum) k, by(ndistinct) fast


*-------------------------------------------------------------------------------
* 5. Splitting: One-to-One Matches vs. Problematic Matches
*-------------------------------------------------------------------------------
use "$PATH/all_temp.dta", clear

* Calculate ID and Uniqueness Flags
g id = name + cpf + dob

** IMPROVEMENT SUGGESTION
*egen id = group(name cpf dob)


egen tag = tag(id pis)
egen ndistinct = total(tag), by(pis)

g unique_cpf_match = 1 if ndistinct == 1
replace unique_cpf_match = 0 if unique_cpf_match == .
g k = 1
drop tag ndistinct

* Tag unique CPFs per PIS
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)
g unique_cpf = 1 if ndistinct == 1
replace unique_cpf = 0 if unique_cpf == .

* Tag unique PIS per CPF
egen tag2 = tag(pis cpf)
egen ndistinct2 = total(tag2), by(cpf)

rename ndistinct2 n_pis_for_cpf
rename ndistinct n_cpf_for_pis

* Define One-to-One Logic
g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (n_pis_for_cpf == 1 | n_pis_for_cpf >= 300)
replace one_to_one_cpf_pis = 0 if one_to_one_cpf_pis == .

* Neutralize High Frequency CPFs
replace cpf = "00000000000" if n_pis_for_cpf >= 300

* Recalculate distinct CPFs per PIS after neutralization
ren n_cpf_for_pis ndistinct
drop if cpf == "00000000000" & ndistinct > 1
drop tag ndistinct

* --- BRANCH A: SAVE ONE-TO-ONE MATCHES ---
keep if one_to_one_cpf_pis == 1
save "$PATH/one_to_one_cpf_pis_refactored.dta", replace

*-------------------------------------------------------------------------------
* 6. Processing the "Not One-to-One"
*-------------------------------------------------------------------------------
use "$PATH/all_temp.dta", clear

* Re-calculate tags (necessary because we reloaded the data)
g id = name + cpf + dob

** IMPROVEMENT SUGGESTION
*egen id = group(name cpf dob)

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

* Identify One-to-One again to exclude them
g one_to_one_cpf_pis = 1 if unique_cpf == 1 & (n_pis_for_cpf == 1 | n_pis_for_cpf >= 300)
replace one_to_one_cpf_pis = 0 if one_to_one_cpf_pis == .
replace cpf = "00000000000" if n_pis_for_cpf >= 300

egen ndistinct = total(tag), by(pis)
drop if cpf == "00000000000" & ndistinct > 1
drop tag ndistinct

* KEEP ONLY NON-MATCHES
keep if one_to_one_cpf_pis != 1

* Merge with External Mapping File (Recovery)
merge n:1 pis using "$PATH/pis_cpf_dob_name_matching_refactored.dta"
drop if _merge == 2
keep if cpf == cpf_imputed

drop _merge n_pis_for_cpf n_cpf_for_pis unique_cpf
egen tag = tag(cpf pis)
egen ndistinct = total(tag), by(pis)

g unique_cpf = 1 if ndistinct == 1
replace unique_cpf = 0 if unique_cpf == .

* Setup Imputed Variables
g pis_imputed = pis
gen first_name = substr(name, 1, strpos(name, " ") - 1) 
replace first_name = trim(first_name)

drop tag
egen tag = tag(first_name cpf)
egen n_names_for_cpf = total(tag), by(cpf)

* Imputation Logic
drop pis_imputed
bys cpf_imputed: generate pis_imputed=pis[1]
gen n_zeros = length(cpf) - length(subinstr(cpf, "0", "", .))
drop if n_zeros > 5

* Keep only if name mapping is unique
keep if n_names_for_cpf == 1
bys cpf_imputed: generate dob_imputed=dob[1]

* Resolve Longest Name logic
gen l_name = length(name)
egen max_l_name = max(l_name), by(cpf_imputed) 
keep if l_name == max_l_name

* Final variable selection for this branch
keep pis name cpf dob one_to_one_cpf_pis cpf_imputed freq pis_imputed dob_imputed 
keep cpf_imputed pis_imputed dob_imputed name one_to_one_cpf_pis freq pis

save "$PATH/not_one_to_one_cpf_pis_refactored.dta", replace

*-------------------------------------------------------------------------------
* 7. Final Merge and Output
*-------------------------------------------------------------------------------

* Append the One-to-One matches back
append using "$PATH/one_to_one_cpf_pis_refactored"

* Standardize columns for the "Clean" matches
replace cpf_imputed = cpf if one_to_one_cpf_pis == 1
replace pis_imputed = pis if one_to_one_cpf_pis == 1
replace dob_imputed = dob if one_to_one_cpf_pis == 1
replace freq = 1 if one_to_one_cpf_pis == 1

keep cpf_imputed pis pis_imputed dob_imputed name one_to_one_cpf_pis freq
order pis pis_imputed cpf_imputed dob_imputed name one_to_one_cpf_pis freq
rename name name_imputed

save "$PATH/pis_cpf_dob_name_matching_final_refactored.dta", replace

di as result "Process Complete successfully."
