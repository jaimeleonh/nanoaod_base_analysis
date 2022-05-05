# Installation (on lxplus)

```
git clone https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis.git -b dummy
cd nanoaod_base_analysis
source setup.sh
law index --verbose
```

# Command to run
```
voms-proxy-init -voms cms -valid 192:0
law run PreprocessRDF --version prod_2804  --category-name base_selection --config-name ul_2018 --dataset-name wjets --PreprocessRDF-max-runtime 20h  --branch 14
```
