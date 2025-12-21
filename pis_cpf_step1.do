



global PATH "~/Library/CloudStorage/Box-Box/rais_pia/data/rais/intermediate/original files"

* ---------- Load 2002–2017 ----------
use "$PATH/2002_temp.dta", clear
forvalues y = 2003/2017 {
    append using "$PATH/`y'_temp.dta"
}

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

save pis_cpf_matching.dta, replace
