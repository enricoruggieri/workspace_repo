
**********************************
* December 2025
*
*
* Master code to run both CHECK and REFACTORED pipelines
*
* PLEASE CHANGE PATH AND CHOOSE THE CORRECT PARAMETERS
*
* For doubts/support: eruggieri@gmail.com
*
**********************************



clear all
set more off
macro drop _all

*-------------------------------------------------------------------------------
* CONFIGURATION
*-------------------------------------------------------------------------------
* 1 = Run fast (load 50k rows only), 0 = Run full dataset
global LOW_RAM_MODE 1

* LAST YEAR for the Base Index 
global LAST_YEAR 2017

* Calculate Validity of CPF using official algorithm. Set to 1 to DROP invalid CPFs.
global KEEP_ONLY_VALID_CPF 0 


* Your Path
global PATH "/Users/er3317/Library/CloudStorage/Box-Box/projects_quotas (Pedro Tremacoldi Rossi)/data/input/rais/enrico/enrico_replicating"

*-------------------------------------------------------------------------------
* EXECUTION CHECK PIPELINE
*-------------------------------------------------------------------------------

display "STEP 1: Building Base Index (2002 - $LAST_YEAR)..."
do "$PATH/step1_check.do"

display "STEP 2: Recovering Missing PIS"
do "$PATH/step2_check.do"

display "ALL DONE (CHECK VERSION)"



*-------------------------------------------------------------------------------
* EXECUTION REFACTORED PIPELINE
*-------------------------------------------------------------------------------


* INCREASE LAST_YEAR
global LAST_YEAR 2021

display "STEP 1: Building Base Index (2002 - $LAST_YEAR)..."
do "$PATH/step1_refactored.do"

display  "STEP 2: Recovering Missing PIS"
do "$PATH/step2_refactored.do"

display "ALL DONE (REFACTORED VERSION)"
