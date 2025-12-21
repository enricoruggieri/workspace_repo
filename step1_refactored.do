
**********************************
* December 2025
*
*
* Code based on pis_cpf_step1 and pis_cpf_dob_name_intermediate
* With changes in Section 1 (Load Data) and 2 (DOB normalization)
*
* For doubts/support: eruggieri@gmail.com
*
**********************************


clear
set varabbrev off

*-------------------------------------------------------------------------------
* 1. Load Data (2002 - LAST_YEAR)
*-------------------------------------------------------------------------------
local files ""
local end_loop = $LAST_YEAR 

** 2002 to 2017 regular filenames
forvalues y = 2002/`end_loop' {
    if `y' <= 2017 {
        local files "`files' `y'_temp.dta"
    }
}

* After 2017, different filenames
if $LAST_YEAR >= 2020 local files "`files' all_temp_2018_2020.dta"
if $LAST_YEAR >= 2021 local files "`files' all_temp_2021.dta"

* Low RAM and Full Dataset Modes
if $LOW_RAM_MODE == 1 {
    tempfile accum
    local first = 1
    foreach f in `files' {
        capture confirm file "$PATH/`f'"
        if _rc == 0 {
            use "$PATH/`f'" in 1/50000, clear
            capture rename nometrabalhador name
            capture rename datadenascimento dob
			
			capture confirm variable dob
			if _rc == 0 {
				di "`f' has dob"
			}
			else di "`f' has no dob"
            
			if `first' == 0 append using `accum'
            save `accum', replace
            local first = 0
        }
    }
    use `accum', clear
}
else {
    * Full load
    local first_file : word 1 of `files'
    use "$PATH/`first_file'", clear
    capture rename nometrabalhador name
    capture rename datadenascimento dob
    
    local n_files : word count `files'
    forvalues i = 2/`n_files' {
        local f : word `i' of `files'
        append using "$PATH/`f'"
        capture rename nometrabalhador name
        capture rename datadenascimento dob
    }
}



*-------------------------------------------------------------------------------
* 2. Normalize CPF and DOB
*-------------------------------------------------------------------------------

keep pis cpf dob name
drop if pis == "" 

* Normalize Identifiers
gen cpf2 = string(real(cpf), "%011.0f")
drop cpf 
rename cpf2 cpf

* Normalize DOB
replace dob = subinstr(dob, "/", "", .)
replace dob = subinstr(dob, "-", "", .)
replace dob = subinstr(dob, ".", "", .)
replace dob = strtrim(dob)
gen dob2 = string(real(dob), "%08.0f")
drop dob 
rename dob2 dob

* Fill missing DOB within PIS using an existing non-missing (arbitrary) DOB for the same PIS
gen dob_clean = dob
gen byte nz = (dob_clean != "" & dob_clean != ".")
bysort pis (nz): gen strL _fill = dob_clean if nz 
bysort pis: replace _fill = _fill[_N] 
replace dob = _fill if (dob == "" | dob == ".") & _fill != ""
drop dob_clean nz _fill


** IMPROVEMENT SUGGESTION

* Fill missing DOB within PIS using the modal (most common) non-missing DOB
*bys pis dob: gen long _freq = _N if dob != "" & dob != "."
*bys pis: egen long _maxf = max(_freq)
*gen byte _is_mode = (_freq == _maxf) if _freq < .
*bys pis: egen byte _nmode = total(_is_mode)
*bys pis: gen strL _mode_dob = dob if _is_mode == 1
*bys pis: replace _mode_dob = _mode_dob[_N]

*replace dob = _mode_dob if (dob == "" | dob == ".") & _nmode == 1 & _mode_dob != ""
*drop _freq _maxf _is_mode _nmode _mode_dob


*-------------------------------------------------------------------------------
* 3. Choose modal CPF per PIS
*-------------------------------------------------------------------------------

* for each CPF, count distinct PIS and drop CPFs with more 300 PIS associated

egen tag2 = tag(pis cpf)
egen ndistinct = total(tag2), by(cpf) //number of unique PIS by CPF
drop if ndistinct >= 300
drop tag2 ndistinct


* first line should be replaced by second
gen pair_id = cpf + pis
* egen pair_id = group(cpf pis) // better

egen n_match = count(pis), by (pair_id)
egen n_total = count(pis), by (pis)

* ---------- Choose modal CPF per PIS with strict majority ----------
bys pair_id: keep if _n==1

gen freq = n_match / n_total
egen max_freq = max(freq), by(pis)

g keep = 1 if freq == max_freq
replace keep = 0 if keep == .

* strict majority: ties at 0.5 are dropped
replace keep = 0 if max_freq < .501
keep if keep == 1
drop keep n_match n_total max_freq
gen cpf_imputed = cpf





save "$PATH/pis_cpf_dob_name_matching_refactored.dta", replace
