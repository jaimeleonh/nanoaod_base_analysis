# coding: utf-8

"""
Cutflow tasks.
"""

__all__ = []


import abc
import contextlib
import itertools
from collections import OrderedDict, defaultdict
import os
import sys
import json
import tabulate

import law
import luigi

from analysis_tools.utils import join_root_selection as jrs
from analysis_tools.utils import import_root, create_file_dir

from cmt.base_tasks.base import ConfigTaskWithCategory, DatasetTaskWithCategory, DatasetWrapperTask
from cmt.base_tasks.preprocessing import DatasetCategoryWrapperTask


class MergeCutFlow(DatasetTaskWithCategory, law.tasks.ForestMerge):
    """
    Merges the output from the PreProcessRDF task if the option
    compute-filter-efficiency is used.
    """ 

    # regions not supported
    region_name = None

    merge_factor = -1

    def merge_workflow_requires(self):
        return PreprocessRDF.vreq(self, compute_filter_efficiency=True, 
                _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        return PreprocessRDF.vreq(self, compute_filter_efficiency=True, 
                workflow="local", branches=((start_leaf, end_leaf),), _exclude={"branch"})

    def trace_merge_inputs(self, inputs):
        return [inp for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        return self.local_target(f"cut_flow.json")

    def merge(self, inputs, output):
        # output content
        stats = defaultdict(lambda: {"pass": 0, "all": 0})

        # merge
        for inp in inputs:
            try:
                if "json" in inp["cut_flow"].path:
                    _stats = inp["cut_flow"].load(formatter="json")
                else:
                    continue
            except:
                print("error leading input target {}".format(inp["cut_flow"]))
                raise

            # add pass and all
            if "json" in inp["cut_flow"].path:
                for cutName, cutEffs in _stats.items():
                    for keyPassAll, val in cutEffs.items():
                        stats[cutName][keyPassAll] += val
            else:
                continue

        output.parent.touch()
        output.dump(stats, indent=4, formatter="json")


class MergeCutFlowWrapper(DatasetCategoryWrapperTask, law.WrapperTask):
    """
    Wrapper task to run the MergeCutFlow task over several datasets in parallel.

    Example command:

    ``law run MergeCutFlowWrapper --version test --category-names etau \
--config-name base_config --dataset-names tt_dl,tt_sl --workers 10``
    """

    def atomic_requires(self, dataset, category):
        return MergeCutFlow.vreq(self, dataset_name=dataset.name, category_name=category.name)


# class CutFlowTable(ConfigTaskWithCategory, DatasetWrapperTask):
