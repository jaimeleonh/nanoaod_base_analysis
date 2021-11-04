# Installation (on lxplus)

```
git clone https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis.git -b HHbbtt
cd nanoaod_base_analysis
source setup.sh
law index --verbose
```

After starting a new session, you always need to do ``` source setup.sh ```


# Running

To run in htcondor at CERN, simply add ```--worflow htcondor```. To run at CIEMAT, add also ```--htcondor-scheduler condorsc1.ciemat.es```.
