# `Chemprop` on Databricks
[Chemprop](https://pubs.acs.org/doi/10.1021/acs.jcim.3c01250) is a chemical property prediction package popular among pharma cheminformaticians for designing drugs with desirable molecular properties.<br>
This repo provides example notebooks for running Chemprop on Databricks. See accompanying [blog](https://docs.google.com/document/d/1_iZPss2I3KNUgfWXPlicO_xtSB1Bjq1C3qZfiRTjKCo/edit?tab=t.0).

## Setup
`pip install chemprop rdkit-pypi`
Setup is straightforward as Chemprop is available as a python package on [PyPI](https://pypi.org/project/chemprop/) or on [github](https://github.com/chemprop/chemprop). You will also need to install its dependency `rdkit-pypi`

## Example NBs/workflows:
#### [Example 0: Load data](0_load_csv_to_deltatable.ipynb)
Load example dataset (CSV), e.g. Drugbank (download from https://doi.org/10.5281/zenodo.10372418), into Delta tables on Unity Catalog
###
#### [Example 1: Load existing models for solubility inferencing](1_load_model_inference_solubility.ipynb)
Use existing model (*.ckpt) from [Chemprop](https://github.com/chemprop/chemprop/tree/f8774bd92174f97030e5ba25eb971e33f45cb96b) to predict solubility of compound (component 1) in solvent (component 2)
###
#### [Example 2: Train single-task classifier](2_singletask_classifier_clintox.ipynb) 
Sometimes, existing models are inadequate as they may be nonexistent for a particular molecular property or may not be applicable to the chemical space of interest. Thus, it may be necessary to train a new custom model.
###
#### [Example 3: Load single-task model for inferencing](3_singletask_inference_clintox.ipynb)
Load ClinTox classifier from [Example 2](2_singletask_classifier_clintox.ipynb) to predict ClinTox properties of DrugBank
###
#### [Example 4: Train multi-task ADMET regressor](4_multitask_regressor_admet.ipynb)
Build a multi-task regression model simultaneously trained on 10 continuous properties from [ADMET-AI](https://academic.oup.com/bioinformatics/article/40/7/btae416/7698030#469619671)
###
#### [Example 5: Load multi-task model for inferencing](5_multitask_inference_admet)
Load multi-task ADMET regressor from [Example 4](4_multitask_regressor_admet.ipynb) to predict ADMET properties of DrugBank
