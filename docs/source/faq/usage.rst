Usage
=====

.. dropdown:: I've created a new task but, when trying to run it, it says it doesn't exist.

  You need to rerun ``law index --verbose``


.. dropdown:: I want to reinstall all the software that is downloaded when running the ``setup.sh``
  script for the first time.
  
  You can run ``CMT_FORCE_SOFTWARE=1 source setup.sh``
  
.. dropdown:: Someone has updated a CMSSW module I'm using and I want to obtain the latest version.

  The easiest (and slowest) way is to reinstall the whole software stack (see a previous FAQ).
  This will download again all CMSSW modules and recompile, but keep in mind it will delete the whole 
  data folder, so make a backup of your changes. It can also be done manually going into the module's
  folder, running ``git pull origin branchname`` (usually ``branchname`` = ``main``), going back to
  the ``${CMSSW_BASE}/src`` folder and recompiling (``scram b``).

.. dropdown:: I launched some jobs to htcondor and after one day or so they still appear as idle.

  If you are running at CIEMAT, remember you should store your analysis setup under /nfs,
  as /afs is not mounted in the htcondor nodes.

.. dropdown:: A colleague has already run some PreprocessRDF, Categorization or
   MergeCategorization tasks. Can I use his/her ntuples?

  Yes, just set ``CMT_STORE_EOS_PREPROCESSING``, ``CMT_STORE_EOS_CATEGORIZATION`` or
  ``CMT_STORE_EOS_MERGECATEGORIZATION`` to the new path (e.g.
  ``/eos/user/${CMT_CERN_USER:0:1}/$CMT_CERN_USER/cmt`` or ``/nfs/cms/$CMT_CIEMAT_USER/cmt``)

.. dropdown:: It looks like I'm missing events after running PreprocessRDF for some datasets, or I've 
   different number of events after running PreprocessRDF with the same selection more than once.

  Firstly, you need to check the PreCounter for those datasets. If the number of events is different
  compared to what appears in DAS, surely the problem is that you're taking a corrupted file within the dataset.
  A possible solution would be to fix the prefix for the dataset in your config file, so that you force the 
  program to take it from a server where you know it is not corrupted. From DAS, you can check the sites where
  a dataset replica is stored by typing ``site dataset=/your_DAS_dataset/``. You can then look for the prefixes of 
  the different sites by typing on your terminal (here for the european site) 
  ``xrdfs xrootd-cms.infn.it locate -h /one_of_the_files_from_DAS_dataset.root``. The ones where it says ``Server`` are valid.
  TIP: If you're running on lxplus, use ``prefix=eoscms-ns-ip563.cern.ch:1098//`` when possible. It also runs much faster.
