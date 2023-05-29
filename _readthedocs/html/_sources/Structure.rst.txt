
.. _structure:

====================================
Structure of the framework
====================================

This framework is structured in two separate repositories, summarized in the next sections.

User code
---------

It should contain the user's configuration and custom tasks. As an example, `this repository  <https://github.com/jaimeleonh/hhbbtt-analysis>`_
includes the code used for the HH->bbtt analysis, and it can be used as an example to add the desired configuration.

It contains two folders:

- *Config*: it contains different files in yaml and python format to define the configuration of the analysis.
- *Tasks*: files in python format. Custom tasks (i.e. specific from your analysis or modifying a common task) can be stored here taking advantage of inheritance. 

`Main code <https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/>`_
-----------------------------------------------------------------------------
Base code of the framework that shouldn't be modified unless there is any improvement suggestion. In this repository there are several folders, some of them just to document information about the code, and inside the main one (*/cmt/*) are a few relevant files on our understanding of the code:

- `preprocessing.py <https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/-/blob/py3/cmt/base_tasks/preprocessing.py>`_: main tasks of the code to process data and to count events are defined here.

- `plotting.py <https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/-/blob/py3/cmt/base_tasks/plotting.py>`_: definitions of the tasks used to make plots of the processed data.

- `base_config.py <https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/-/blob/py3/cmt/config/base_config.py>`_: useful datasets and definitions of the mandatory variables for the analysis.

All this code gives us the opportunity to execute multiple tasks depending on what we want to do, and can be summarized with this diagram:

.. image:: lawStructure.png

Where there are two branches of tasks before plotting depending on what the goal is: event counting or processing data.
In the section :ref:`tasks` you can see step by step how it works and which are the specific tasks the code can make.

Environment variables
~~~~~~~~~~~~~~~~~~~~~

Inside the user and main code there is a *setup* file, where multiple directories are defined with a variable name. The source file of the main code define:

.. _directories layout:

- **Directories layout**: Important directories and what you can file inside of them: 

    - **$CMT_STORE**: output files of every task.
    - **$CMT_JOB_DIR**: scripts of the jobs that are being executed.
    - **$CMT_TMPDIR**: temporal files that are produced during the execution of all tasks.
    - **$CMT_STORE_LOCAL**: condor status, submission information and logs.
    - **$CMT_GFAL_DIR**: Grid File Access Library.
    - **$CMT_REMOTE_JOB**: number of jobs (not a directory, but still may be useful).

- Useful **Git repositories**:
    - `CMS-nanoAOD-tools ($NANOTOOLS_PATH) <https://github.com/cms-nanoAOD/nanoAOD-tools>`_
    - `Base modules cms-phys-ciemat ($BASEMODULES_PATH) <https://gitlab.cern.ch/cms-phys-ciemat/cmt-base-modules>`_
    - `HHKinFit2 ($HHKINFIT_PATH) <https://github.com/bvormwald/HHKinFit2/tree/CMSSWversion>`_
    -  SVfit (*$SVFIT_PATH*) with two repositories:
        - `Classic SVfit <https://github.com/LLRCMS/ClassicSVfit/tree/bbtautau_LegacyRun2>`_
        - `SVfitTF <https://github.com/svfit/SVfitTF>`_
    - `GEM Modules ($GEM_PATH) <https://gitlab.cern.ch/diegof/gem-modules>`_
    - HTT-utilities (*$HTT_PATH*) with several repositories: 
        - `LeptonEff-interface <https://github.com/CMS-HTT/LeptonEff-interface>`_
        - `Lepton Efficiencies <https://github.com/CMS-HTT/LeptonEfficiencies>`_
        - `TauAnalysisTools <https://github.com/cms-tau-pog/TauTriggerSFs/tree/run2_SFs>`_
        - TauAnalysisTools data: `data1 <https://github.com/camendola/VBFTriggerSFs/raw/master/data/2017_VBFHTauTauTrigger_JetLegs.root>`_ , `data2 <https://github.com/camendola/VBFTriggerSFs/raw/master/data/2018_VBFHTauTauTrigger_JetLegs.root>`_
    - `HHbtag ($HHBTAG_PATH): <https://github.com/hh-italian-group/HHbtag>`_
        - `hh/bbtautau <https://gitlab.cern.ch/hh/bbtautau/MulticlassInference>`_
        - `InterferenceTools <https://github.com/jaimeleonh/InferenceTools>`_
        - `cms_hh_proc_interface <https://github.com/GilesStrong/cms_hh_proc_interface>`_
        - `cms_hh_tf_inference <https://github.com/GilesStrong/cms_hh_tf_inference>`_
        - `cms_runII_dnn_models <https://github.com/GilesStrong/cms_runII_dnn_models>`_
    - Corrections (*$CORRECTIONS_PATH*):
        - `correctionlib-wrapper <https://github.com/jaimeleonh/correctionlib-wrapper/tree/cmssw_version>`_
        - `tau-corrections <https://gitlab.cern.ch/cms-phys-ciemat/tau-corrections>`_
        - `jme-corrections <https://gitlab.cern.ch/cms-phys-ciemat/jme-corrections>`_
        - jme-corrections data: `1 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL18_V5_MC.tar.gz>`_, `2 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL17_V5_MC.tar.gz>`_, `3 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL16_V7_MC.tar.gz>`_, `4 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL16APV_V7_MC.tar.gz>`_.
        - `lum-corrections <https://gitlab.cern.ch/cms-phys-ciemat/lum-corrections>`_
        - `muo-corrections <https://gitlab.cern.ch/cms-phys-ciemat/muo-corrections>`_
        - `egm-corrections <https://gitlab.cern.ch/cms-phys-ciemat/egm-corrections>`_
        - `btv-corrections <https://gitlab.cern.ch/cms-phys-ciemat/btv-corrections>`_
    - Software (*$CMT_SOFTWARE*):
        - `analysis_tools <https://gitlab.cern.ch/cms-phys-ciemat/analysis_tools>`_
        - `plotting_tools <https://gitlab.cern.ch/cms-phys-ciemat/plotting_tools>`_
        - `law <https://github.com/riga/law>`_
        - `plotlib <https://github.com/riga/plotlib>`_

- **CMS Software**: you cand find the CMS software used for this framework inside *Reponame/nanoaod_base_analysis/data/cmssw/*, also associated to the variable *$CMT_CMSSW_BASE*. Also, you can see the version with *$CMT_CMSSW_VERSION*.
- **$PYTHONPATH** and **$CMT_PYTHON_VERSION**.