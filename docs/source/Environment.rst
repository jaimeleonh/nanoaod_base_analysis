.. _environment:

==========================
Setting the environment
==========================

After configuring the analysis, we can define the different directories mentioned in :ref:`Directories layout<directories layout>` as we prefer in our user code's setup.sh, or use the ones defined by default.

Therefore, once we have configure our analysis and environment, you'll need to do the following steps in your local repository every time we want to run the code:

.. code-block:: console

   cd nanoaod_base_analysis #To update the main code
   git pull origin py3 #To update the main code
   cd ..
   source setup.sh
   law index --verbose #If we have added some tasks in our user code.
   voms-proxy-init -voms cms #To identify yourself with a valid proxy in case you don't have it.
