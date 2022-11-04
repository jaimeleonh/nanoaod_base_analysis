.. _configuration:

==========================
Setting the configuration
==========================

NanoAOD framework allows you to configure multiple settings for your analysis to be done. 
Some of them are mandatory to create your configuration files, but you can create others in order to improve your analysis. The mandatory ones are defined `here <https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/-/tree/py3/cmt/config>`_, where the parent classes can be found in `this <https://gitlab.cern.ch/cms-phys-ciemat/analysis_tools/-/tree/master/analysis_tools>`_ repository.

As we said in the :ref:`structure` section, `hhbbtt repository <https://github.com/jaimeleonh/hhbbtt-analysis/tree/main/config>`_ serves us as an example on how to configure our analysis. So if we navigate through it we can see there are several python and yaml files.  Python ones define the main configuration  settings for an analysis (and there may be several of them taking advantage of inheritance), and yaml files are used to indicate modules to use, features to compute/plot, or weights to apply.  

Therefore, to set your configuration, you'll have to create a config file (with a python format) and yaml files in your own *reponame/config/* folder and define the settings you need inside of them.

Let's see then what are the main settings you can configure and how to do it: 

- `Datasets <https://gitlab.cern.ch/cms-phys-ciemat/analysis_tools/-/blob/master/analysis_tools/dataset.py>`_: Datasets consist of ntuples root files and can be data or Monte Carlo. Several datasets are already defined in `the base framework code <https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis/-/blob/py3/cmt/config/base_config.py>`_, but you will probably need to define some more in your configuration file (`e.g. <https://github.com/jaimeleonh/hhbbtt-analysis/blob/main/config/ul_2018_v10.py>`__). Every dataset is assigned to a process and a specific cross section. 
 
- **Redirector**: XRootD redirectors may be used to read datasets that aren't local. There's the option to specify one for an individual dataset (e.g. ``prefix="xrootd-cms.infn.it//``), or just one for all non-localdatasets as a parameter in the ``Config`` object constructor e.g.: ``config = Config_ul_2018("ul_2018_v10", year=2018, ecm=13, lumi_pb=59741, xrd_redir='gaeds004.ciemat.es:1094')`` . This setting is not compulsory, but it's recommended in the case of CIEMAT, since it allows you to take advantage of local data sources or caches (please, ask computing support for recommended settings).

- **Categories**: selections to be applied.

- **Processes**: classify and merge datasets. This parameter facilitates the selection of datasets when plotting.

- **Features**: histograms of the variables of the process and particles of interest to be plotted. 

- **Weights**: used to make a proper normalization, both for the variables and the systematics. They are only applied to Monte Carlo data.

- **Systematics**: used in the plotting task. There's the possibility to not use them for your features just leaving the values systematics as ``systematics=[]`` (`e.g. <https://github.com/jaimeleonh/hhbbtt-analysis/blob/5298be05d19a3a561cbf814a595545815cedd566/config/base_config.py#L345>`__).

- **Modules**: RDataFrame (RDF) based code used to set new variables to the analysis from the ntuple branches, other files and/or make selections over specific variables. You can use `existing ones <https://github.com/jaimeleonh/InferenceTools/tree/main/python>`_, or just make yours. More info about modules can be found in the sections :ref:`info` and :ref:`modules`.

	- **How to make modules**:

	You'll have to create two files: a .yaml file and a .py one. The first one is used to sort modules to be applied and has to be inside your config folder (as `this one <https://github.com/jaimeleonh/hhbbtt-analysis/blob/5298be05d19a3a561cbf814a595545815cedd566/config/modulesrdf.yaml>`_), and the .py file to define the code of your modules. Instructions to create your modules are the following:

	1. Create a new (empty or with a README) repository.

	2. Clone it inside *$CMSSW_BASE/src/* + *Reponame/Modules/* (or *WHATEVER/YOUWANT/*) ``git clone https://github.com/username/reponame.git Reponame/Modules``.

	3. Inside Reponame/Modules create a BuildFile.xml (you can adapt it from `this file <https://github.com/jaimeleonh/InferenceTools/blob/main/BuildFile.xml>`_).

	4. Create a python folder. If precompiled code wants to be added, create a src and an interface folders.

	5. Inside python/modulename.py, create a moduleProducer class (with the init and run methods) and a module function (which returns a moduleProducer object and is the one that's going to be included in the .yaml file).


All of these parameters are mandatory for the configuration of any analysis with this framework, but you can add other settings like regions or channels that can be very useful to classify categories.
