.. _installation:

=======================
Installation
=======================

The code is designed to work both in CIEMAT User Interfaces (you'll need to access to a gaecmsui (``ssh gaecmsui``) and then go to your ``/nfs/cms/username/`` path) and *lxplus*, so once you're in one of them you can follow these steps:

Make a fork of the user code <https://github.com/jaimeleonh/hhbbtt-analysis> in your github area with the *reponame* you want.

.. code-block:: console

   git clone https://github.com/username/reponame #where username is your own github user name.
   cd <reponame>
   git clone https://gitlab.cern.ch/cms-phys-ciemat/nanoaod_base_analysis.git --branch py3 nanoaod_base_analysis/
   source setup.sh
   law index --verbose #to do only after installation or including a new task

To reinstall the software you would need to do:

``CMT_FORCE_SOFTWARE=1 source setup.sh``

As improvements of the main code are frequently made, it is very recommended to update your own as well with the following commands every time you log into your repository:

.. code-block:: console

   cd nanoaod_base_analysis
   git pull origin py3
