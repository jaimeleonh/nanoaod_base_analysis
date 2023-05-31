Plotting
========

.. dropdown:: How do I normalize histograms if my input dataset is skimmed (i.e. pre-filtered)?

    You can assign to the dataset a ``secondary_dataset`` parameter, which should point to
    a valid dataset that can be used to do the event counting. If the
    ``secondary_dataset`` parameter has been defined, the plotting will automatically
    require the counting from this dataset instead of the one is being plotted.

.. dropdown:: I'm launching the ``FeaturePlot`` task without having previously run the 
    ``MergeCategorizationStats`` and ``PreCounter`` tasks, so they are run automatically
    before running the ``FeaturePlot``. Some error appears regarding the ``PUweight``.
    
    As the ``PUweight`` does not appear in basic NanoAOD, you need to add it to the RDF using
    its dedicated RDFModule. To do so, you should add to your ``FeaturePlot`` command
    ``--PreCounter-weights-file weights``.