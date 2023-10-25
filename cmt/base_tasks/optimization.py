# coding: utf-8

"""
Optimization tasks.
"""

import law
import luigi
from copy import deepcopy as copy
import itertools
import math
import json

from analysis_tools.utils import import_root, create_file_dir

from cmt.base_tasks.plotting import FeaturePlot
from cmt.base_tasks.base import HTCondorWorkflow, SGEWorkflow


class BaseOptimizationTask(FeaturePlot, law.LocalWorkflow, HTCondorWorkflow, SGEWorkflow):
    n_mini_bins = 100
    n_max_bins = 10

    def __init__(self, *args, **kwargs):
        super(BaseOptimizationTask, self).__init__(*args, **kwargs)

    def create_branch_map(self):
        """
        :return: number of features
        :rtype: int
        """
        return len(self.features)

    def workflow_requires(self):
        return {"histo": FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False,
            n_bins=self.n_mini_bins, plot_systematics=False, optimization_method="")}

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        return {"histo": FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False,
            n_bins=self.n_mini_bins, plot_systematics=False, optimization_method="")}

    def output(self):
        return self.local_target("binning__{}_{}_bins.json".format(
            self.features[self.branch].name, self.n_max_bins))

    def run(self):
        pass


class BayesianBlocksOptimization(BaseOptimizationTask):
    def add_to_bin_list(self, bin_lists, n_bins, bin_edges, bin_contents):
        if len(bin_lists[0]) == n_bins - 1:
            l = [0] + bin_lists[0] + [bin_edges[-1]]
            return [(l, self.get_fitness_function(l, bin_contents))]
        new_bin_lists = []
        for bin_list in bin_lists:
            for i in range(bin_list[-1] + 1, len(bin_edges) - n_bins + len(bin_list) + 1):
                new_bin_lists += self.add_to_bin_list(
                    [bin_list + [i]], n_bins, bin_edges, bin_contents)
        vals = [elem[1] for elem in new_bin_lists]
        max_val = max(vals)
        return [new_bin_lists[vals.index(max_val)]]

    def get_fitness_function(self, bin_list, bin_contents):
        val = 0
        for iv in range(0, len(bin_list) - 1):
            bc = sum(bin_contents[bin_list[iv]: bin_list[iv + 1]])
            bin_size = bin_list[iv + 1] - bin_list[iv]
            val += bc * (math.log(bc) - math.log(bin_size))
        return val

    def run(self):
        ROOT = import_root()

        self.background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal]
        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        self.signal_names = [p.name for p in self.processes_datasets.keys() if p.isSignal]

        if self.background_names:
            process_names = self.background_names
        elif self.data_names:
            process_names = self.data_names
        else:
            process_names = self.signal_names

        feature = self.features[self.branch]
        x_min = float(feature.binning[1])
        x_max = float(feature.binning[2])
        initial_bin_size = (x_max - x_min) / self.n_mini_bins

        tf = ROOT.TFile.Open(self.input()["histo"]["root"].targets[feature.name].path)
        histo = None
        for name in process_names:
            if not histo:
                histo = copy(tf.Get("histograms/" + name))
            else:
                histo = histo.Add(tf.Get("histograms/" + name))
        bin_contents = [histo.GetBinContent(i) for i in range(1, self.n_mini_bins + 1)]
        bin_edges = [0]
        merged_bin_contents = [0]
        for ib in range(self.n_mini_bins):
            if merged_bin_contents[-1] != 0:
                merged_bin_contents.append(0)
            if bin_contents[ib] > 0.0:
                bin_edges.append(ib + 1)
                merged_bin_contents[-1] += bin_contents[ib]

        if bin_edges[-1] != self.n_mini_bins - 1:
            merged_bin_contents = merged_bin_contents[:-1]
            bin_edges[-1] = self.n_mini_bins - 1

        if len(bin_edges) > self.n_max_bins + 1:
            n_bins = self.n_max_bins
            saved_val = None
            saved_comb = None
            bin_lists = []
            for a in range(1, len(bin_edges) - n_bins + 1):
                l = [a]
                if len(l) != n_bins - 1:
                    bin_lists += self.add_to_bin_list([l], n_bins, bin_edges, merged_bin_contents)
                else:
                    bin_lists.append((l, 1))
    
            vals = [elem[1] for elem in bin_lists]
            max_val = max(vals)
            saved_comb = bin_lists[vals.index(max_val)][0]

        else:
            saved_comb = bin_edges

        opt_edges = []
        for elem in saved_comb:
            opt_edges.append(histo.GetBinCenter(elem + 1) - initial_bin_size / 2.)
        opt_edges.append(histo.GetBinCenter(saved_comb[-1] + 1) + initial_bin_size / 2.)
        print(x_min, x_max, initial_bin_size)
        print(bin_contents)
        print(opt_edges)
        with open(create_file_dir(self.output().path), "w") as f:
            json.dump(opt_edges, f, indent=4)

