JME
===

Installation
------------

.. code:: bash
    
    git clone https://gitlab.cern.ch/cms-phys-ciemat/JME-corrections.git Corrections/JME
    cd Corrections/JME/data
    wget https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL18_V5_MC.tar.gz
    wget https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL17_V5_MC.tar.gz
    wget https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL16_V7_MC.tar.gz
    wget https://github.com/cms-jet/JECDatabase/raw/master/tarballs/Summer19UL16APV_V7_MC.tar.gz
    cd -

RDFModules
----------

.. toctree::
    :maxdepth: 2
   
    smearing
    jec
    pujetid_sf 
