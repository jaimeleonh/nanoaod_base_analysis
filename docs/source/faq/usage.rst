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
