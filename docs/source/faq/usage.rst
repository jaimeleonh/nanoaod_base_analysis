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