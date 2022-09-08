# Installation (on lxplus)

```
git clone https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis.git -b py3 nanoaod_base_analysis
cd nanoaod_base_analysis
source setup.sh
law index --verbose
```

After starting a new session, you always need to do ``` source setup.sh ```


# Running

## htcondor

To run in htcondor at CERN, simply add ```--worflow htcondor```. To run at CIEMAT, add also ```--htcondor-scheduler condorsc1.ciemat.es```.

Remember to obtain your proxy before (`voms-proxy-init -voms cms -valid 192:0`) and copy it to the `nanoaod_base_analysis` folder (`cp /tmp/x509up_uXXXXXX ./x509up`)


# Documentation

Some WIP documentation can be found [here](https://nanoaod-base-analysis.readthedocs.io). In case you want to update it after including some changes:
```
cd docs
make html
cp -r build/* ../_readthedocs
cd ..
git add _readthedocs
```
 
And `commit` back whenever you are ready.
