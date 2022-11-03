.. _environment:

==========================
Setting the environment
==========================

Every time we want to run the code we need to do the following steps in your local repository:

.. code-block:: console

   cd nanoaod_base_analysis #To update the main code
   git pull origin py3 #To update the main code
   cd ..
   source setup.sh
   law index --verbose #If we have added some tasks in our user code.
   voms-proxy-init -voms cms #To identify yourself with a valid proxy in case you don't have it.
