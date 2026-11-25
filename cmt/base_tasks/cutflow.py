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
import matplotlib.pyplot as plt

import law
import luigi

from analysis_tools.utils import join_root_selection as jrs
from analysis_tools.utils import import_root, create_file_dir

from cmt.base_tasks.base import ConfigTaskWithCategory, DatasetTaskWithCategory, DatasetWrapperTask
from cmt.base_tasks.preprocessing import DatasetCategoryWrapperTask, PreprocessRDF, Categorization


class MergeCutFlow(DatasetTaskWithCategory, law.tasks.ForestMerge):
    """
    Merges the output from the PreProcessRDF task if the option
    compute-filter-efficiency is used.
    """ 

    from_categorization = luigi.BoolParameter(description="whether to get cutflow from"
        "Categorization, default: False", default=False)
    base_category_name = luigi.Parameter(description="base category to use in PreprocessRDF"
        "default: None", default="")

    # regions not supported
    region_name = None

    merge_factor = -1

    def merge_workflow_requires(self):
       if not self.from_categorization:
           return PreprocessRDF.vreq(self, compute_filter_efficiency=True, _prefer_cli=["workflow"])
       return Categorization.vreq(self, compute_filter_efficiency=True, _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        if not self.from_categorization:
            return PreprocessRDF.vreq(self, compute_filter_efficiency=True,
                workflow="local", branches=((start_leaf, end_leaf),), _exclude={"branch"})
        return Categorization.vreq(self, compute_filter_efficiency=True,
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

    add_categorization = luigi.Parameter(default="", description="Category name to run on the "
        "Categorization task to add on top of the standard Cutflow, default: None")

    keys = ["total", "rel", "rel_step"]

    def requires(self):
        d = {
            "preprocess": {
                dataset.name: MergeCutFlow.vreq(self, dataset_name=dataset.name)
                for dataset in self.datasets
                }
        }
        if self.add_categorization:
            d["cat"] = {
                dataset.name: MergeCutFlow.vreq(self, dataset_name=dataset.name,
                    from_categorization=True, category_name=self.add_categorization,
                    base_category_name=self.category_name)
                for dataset in self.datasets
            }
        return d

    def output(self):
        # tables
        outputs = {
            key: {
                ext: self.local_target(f"table_{key}.{ext}")
                for ext in ["txt", "tex", "csv"]
            }
            for key in self.keys
        }
        # plots
        outputs["plots"] = {
            key: {
                ext: self.local_target(f"plot_{key}.{ext}")
                for ext in ["png", "pdf"]
            }
            for key in self.keys
        }
        return outputs
        
    def run(self):
        import csv

        inputs = self.input()
        tables = {key: [] for key in self.keys}
        cutflows = OrderedDict()
        for dataset in self.datasets:
            with open(inputs["preprocess"][dataset.name].path) as f:
                cutflows[dataset.name] = json.load(f, object_pairs_hook=OrderedDict)
        if self.add_categorization:
            for dataset in self.datasets:
                with open(inputs["cat"][dataset.name].path) as f:
                    cutflows[dataset.name].update(json.load(f, object_pairs_hook=OrderedDict))

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

            # csv printing
            # first line includes the different filter names
            # next includes the dataset name and the values after each filter
            csv_table = [["dataset"] + [line[0] for line in tables[key]]]
            for id, d in enumerate(self.datasets):
                csv_table.append([d.process.name] + [line[id + 1] for line in tables[key]])

            with open(create_file_dir(self.output()[key]["csv"].path), 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(csv_table)

        # draw corresponding plots
        # create a plot for the efficiency per filter
        plot_info = {
            "total":    { "ylabel": "Counts per step",           "ylim": None},
            "rel":      { "ylabel": "Efficiency over total [%]", "ylim": (0, 105)},
            "rel_step": { "ylabel": "Step efficiency [%]",       "ylim": (0, 105)}
        }
        for key, info in plot_info.items():
            rows = tables[key]
            filters = [row[0] for row in rows]
            fig, ax = plt.subplots(figsize=(6, 5))
            
            # get values
            for i, dataset in enumerate(self.datasets):
                values = [ float(row[i + 1]) if row[i + 1] != "-" else float("nan") for row in rows]
                ax.plot(filters, values, marker="o", markersize=5, linewidth=1.8, label=dataset.process.name)

            # plot style
            ax.set_ylabel(info["ylabel"])
            ax.set_xlabel("Filter")
            ax.tick_params(axis="x", rotation=45)
            ax.grid(True, axis="y", linestyle="--", linewidth=0.7, alpha=0.5)
            if info["ylim"] is not None:
                ax.set_ylim(*info["ylim"])
            ax.text(0.00, 1.02, "CMS", transform=ax.transAxes, fontsize=18, fontweight="bold", va="bottom")
            ax.text(0.125, 1.025, "Work in progress", transform=ax.transAxes, fontsize=13, style="italic", va="bottom")
            ax.legend(loc="best", frameon=False, fontsize=9)
            fig.tight_layout()
            fig.savefig(create_file_dir(self.output()["plots"][key]["png"].path), dpi=200, bbox_inches="tight")
            fig.savefig(create_file_dir(self.output()["plots"][key]["pdf"].path), dpi=200, bbox_inches="tight")
            plt.close(fig)
