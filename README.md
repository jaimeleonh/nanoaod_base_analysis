[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8187177.svg)](https://doi.org/10.5281/zenodo.8187177)

This is the main code of the NanoAOD-base-analysis, which aims to process NanoAOD datasets, allowing to generate different root files, histograms and plots with the desired selection of events, variables and branches.

If you want to make some suggestions to improve this code, feel free to make a pull request!

# User guide:

Information about the code, how to install it, setting a configuration to use it and more useful information about this framework cand be found [here](https://nanoaod-base-analysis.readthedocs.io).

In case you want to update it after including some changes:
```
cd docs
make html
cp -r build/* ../_readthedocs
cd ..
git add _readthedocs
```

And `commit` back whenever you are ready.

# Running

## htcondor

To run in htcondor at CERN, simply add ```--worflow htcondor```. To run at CIEMAT, add also ```--htcondor-scheduler condorsc1.ciemat.es```.

