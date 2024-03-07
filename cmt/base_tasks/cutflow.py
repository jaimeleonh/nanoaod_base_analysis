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
from cmt.base_tasks.preprocessing import DatasetCategoryWrapperTask, PreprocessRDF


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


class CutFlowTable(ConfigTaskWithCategory, DatasetWrapperTask):
    """
    Task to generate CutFlow tables for several datasets.

    Example command:

    ``law run CutFlowTable --version test --category-names etau \
--config-name base_config --dataset-names tt_dl,tt_sl --workers 10``
    """

    keys = ["total", "rel", "rel_step"]

    def requires(self):
        return {
            dataset.name: MergeCutFlow.vreq(self, dataset_name=dataset.name)
            for dataset in self.datasets
        }

    def output(self):
        return {
            key: {
                ext: self.local_target(f"table_{key}.{ext}")
                for ext in ["txt", "tex"]
            }
            for key in self.keys
        }

    def run(self):
        inputs = self.input()
        tables = {key: [] for key in self.keys}
        cutflows = OrderedDict()
        for dataset in self.datasets:
            with open(inputs[dataset.name].path) as f:
                cutflows[dataset.name] = json.load(f, object_pairs_hook=OrderedDict)
        filters = list(cutflows.values())[0].keys()

        tables["total"].append(["total"] + [
            elem[self.category.name]["all"] for elem in cutflows.values()])
        tables["rel"].append(["total"] + [100 for elem in cutflows.values()])
        tables["rel_step"].append(["total"] + ["-" for elem in cutflows.values()])

        for filt in filters:
            tables["total"].append([filt] + [
                cutflows[dataset.name][filt]["pass"]
                for idat, dataset in enumerate(self.datasets)
            ])
            tables["rel"].append([filt] + [
                "{:.2f}".format(100. * cutflows[dataset.name][filt]["pass"] / tables["total"][0][idat + 1])
                for idat, dataset in enumerate(self.datasets)
            ])
            tables["rel_step"].append([filt] + [
                "{:.2f}".format(100. * cutflows[dataset.name][filt]["pass"] / cutflows[dataset.name][filt]["all"])
                for idat, dataset in enumerate(self.datasets)
            ])

        for key in self.keys:
            for ext, fmt in zip(["txt", "tex"], ["", "latex_raw"]):
                if ext == "txt":
                    headers = [d.process.name for d in self.datasets]
                else:
                    headers = [d.process.label.latex for d in self.datasets]
                fancy_tab = tabulate.tabulate(tables[key], headers=headers, tablefmt=fmt)
                if ext == "tex":
                    fancy_tab = fancy_tab.replace("\$", "$")
                with open(create_file_dir(self.output()[key][ext].path), "w+") as f:
                    f.write(fancy_tab)

