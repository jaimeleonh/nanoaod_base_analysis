Plotting
========

.. dropdown:: How do I normalize histograms if my input dataset is skimmed (i.e. pre-filtered)?

    You can assign to the dataset a ``secondary_dataset`` parameter, which should point to
    a valid dataset that can be used to do the event counting. If the
    ``secondary_dataset`` parameter has been defined, the plotting will automatically
    require the counting from this dataset instead of the one is being plotted.
