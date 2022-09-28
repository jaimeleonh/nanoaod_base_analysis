Plotting
========

.. dropdown:: How do I normalize histograms if my input dataset is skimmed (i.e. pre-filtered)?

    You can assign to the dataset a ``secondary_dataset`` parameter, which will be used to
    extract the number of events from DAS using ``dasgoclient``. This option won't be useful if
    weights have to be applied to the sum of events. In this case, a solution will come (hopefully)
    soon.