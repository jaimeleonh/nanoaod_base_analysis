
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

.. _directories layout:

Directories layout
------------------

Once installed the framework in your user area, the directories structure and some important folders and files located inside will be the following:

    - ``reponame/``: user code of the framework. This path would be ``/nfs/cms/<username>/<installdir>/<reponame>`` in CIEMAT by default, and ``/afs/cern.ch/<installdir>/<user>/<reponame>`` in CERN.
 
        - ``config/``: different files in yaml and python format to define the configuration of the analysis are inside.
        - ``modules/``: specific modules for some tasks are added here.
        - ``tasks/``: for custom tasks in python format.
        - ``setup.sh``: required to define environment variables and install important software code. The environment variables can be modified in this setup.sh script.
        - ``nanoaod_base_analysis/``: or main code. Defined with the variable ``$CMT_BASE``.
        
            - ``_readthedocs/`` and ``docs/``: folders where this docummentation you are looking at is defined.
            - ``bin/``: some useful scripts to use.
            - ``cmt/``: python modules from the framework's code.
            - ``data/``:
            
                - ``cmssw/``: CMS software used for this framework, also associated to the variable ``$CMT_CMSSW_BASE`` or ``$CMT_BASE/data/cmssw``.
                - ``jobs/``: submit files and the actual scripts submitted. Defined with the variable ``$CMT_JOB_DIR`` or ``$CMT_BASE/data/jobs``.
                - ``law/``: index of law tasks.
                - ``software/``: software of law, python and sphinx. Defined with ``$CMT_SOFTWARE``. 
                - ``store/``: condor control files (json files) and htcondor logs (.txt files). Defined with the variable ``$CMT_STORE_LOCAL``. (Very) general structure of the folders inside the path is ``<TaskName>/<config_name>/<dataset_name>/[<category_name> (for PreprocessRDF, Categorization, PrePlot)]/<version_name>``.
                - ``tmp/``: it contains sorted .txt lists of LFNs (Logical File Names) for the required input files needed for preprocessing or precounting. Defined with ``$CMT_TMP_DIR``.
                - ``x509up``: CMS VOMS proxy. It's created once you make a ``voms-proxy-init``.

    - Output area (``cmt/``): output files produced by every task. Defined with the variable ``$CMT_STORE`` and located by default in ``/nfs/cms/<username>/cmt/`` in CIEMAT and ``/eos/<username>`` in CERN. (Very) general structure of the folders inside the path is ``<username>/cmt/<TaskName>/<config_name>/[<dataset_name> (all except FeaturePlot)]/[<category_name> (for PreprocessRDF, Categorization, MergeCategorization, PrePlot, FeaturePlot)]/<version_name>``.

    - Temporal files area (``tmp/``): or ``$TMPDIR`` (do an ``echo $TMPDIR`` to see specific path), it contains temporal files created during the execution of tasks and within the HTCondor nodes.


Git repositories
-------------------------

Useful git repositories used in the framework.

    - Software: defined with ``$CMT_SOFTWARE`` and located in ``<installdir>/<reponame>/nanoaod_base_analysis/data/software/``.

        - CIEMAT tools:
        
          - `analysis_tools <https://gitlab.cern.ch/cms-phys-ciemat/analysis_tools>`_
          - `plotting_tools <https://gitlab.cern.ch/cms-phys-ciemat/plotting_tools>`_
        
        - `law <https://github.com/riga/law>`_
        - `plotlib <https://github.com/riga/plotlib>`_

    - Other software, located in different areas (do an ``echo $variable`` to see specific path):

        - `CMS-nanoAOD-tools ($NANOTOOLS_PATH) <https://github.com/cms-nanoAOD/nanoAOD-tools>`_
        - `Base modules cms-phys-ciemat ($BASEMODULES_PATH) <https://gitlab.cern.ch/cms-phys-ciemat/cmt-base-modules>`_

    - Other git repositories that can be useful for your analysis:

        - `HHKinFit2 ($HHKINFIT_PATH) <https://github.com/bvormwald/HHKinFit2/tree/CMSSWversion>`_
        -  SVfit (``$SVFIT_PATH``) with two repositories:

            - `Classic SVfit <https://github.com/LLRCMS/ClassicSVfit/tree/bbtautau_LegacyRun2>`_
            - `SVfitTF <https://github.com/svfit/SVfitTF>`_

        - `GEM Modules ($GEM_PATH) <https://gitlab.cern.ch/diegof/gem-modules>`_
        - HTT-utilities (``$HTT_PATH``) with several repositories:

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

        - Corrections (``$CORRECTIONS_PATH``):

            - `correctionlib-wrapper <https://github.com/jaimeleonh/correctionlib-wrapper/tree/cmssw_version>`_
            - `tau-corrections <https://gitlab.cern.ch/cms-phys-ciemat/tau-corrections>`_
            - `jme-corrections <https://gitlab.cern.ch/cms-phys-ciemat/jme-corrections>`_
            - jme-corrections data: `1 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL18_V5_MC.tar.gz>`_, `2 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL17_V5_MC.tar.gz>`_, `3 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL16_V7_MC.tar.gz>`_, `4 <https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL16APV_V7_MC.tar.gz>`_.
            - `lum-corrections <https://gitlab.cern.ch/cms-phys-ciemat/lum-corrections>`_
            - `muo-corrections <https://gitlab.cern.ch/cms-phys-ciemat/muo-corrections>`_
            - `egm-corrections <https://gitlab.cern.ch/cms-phys-ciemat/egm-corrections>`_
            - `btv-corrections <https://gitlab.cern.ch/cms-phys-ciemat/btv-corrections>`_   
            