# coding: utf-8

"""
Optimization tasks.
"""

import law
import luigi
from copy import deepcopy as copy
from collections import OrderedDict
import itertools
import math
import json
import numpy as np
from ctypes import c_double

from analysis_tools.utils import import_root, create_file_dir, randomize

from cmt.base_tasks.plotting import BasePlotTask, PrePlot, FeaturePlot
from cmt.base_tasks.preprocessing import MergePreCounter
from cmt.base_tasks.base import (
    HTCondorWorkflow, SGEWorkflow, ProcessGroupNameTask, ConfigTaskWithCategory,
    FlatSignalBinMerger,FlatSigCumulativeRebinner,FlatSignalBackgroundBinMerger
)


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
        return self.local_target("binning__{}__pgn_{}__{}_bins.json".format(
            self.features[self.branch].name, self.process_group_name, self.n_max_bins))

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
            if bc > 0.:
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
                histo.Add(tf.Get("histograms/" + name))
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


class FlatSignalBinMergerTask(ConfigTaskWithCategory, ProcessGroupNameTask, BasePlotTask):
    features_to_flatten = law.CSVParameter(default=("dnn_HHbbtt_kl_1","dnn_HHbbtt_HH"), description="names of features to plot, uses all "
        "features when empty, default: (dnn_HHbbtt_kl_1,dnn_HHbbtt_HH)")
    save_root = luigi.BoolParameter(default=False, description="whether to save created histograms "
        "in root files, default: False")
    use_cumulative = luigi.BoolParameter(default=False, description="whether to optimize binning using the FlagSigCumulativeRebinner" "default: False") 
    use_bkg_flattening = luigi.BoolParameter(default=False, description="whether to optimize binning using the FlagSigBkgBinMerger" "default: False") 
    additional_scaling = {"dummy": 1}  # Temporary fix, the DictParameter fails when empty
    directions = ["up", "down"]

    def __init__(self, *args, **kwargs):
        super(FlatSignalBinMergerTask, self).__init__(*args, **kwargs)
        self.norm_syst_list = []
        weights = self.config.weights.total_events_weights
        for weight in weights:
            try:
                feature = self.config.features.get(weight)
                for syst in feature.systematics:
                    if syst not in self.norm_syst_list:
                        self.norm_syst_list.append(syst)
            except:  # weight not defined as a feature -> no syst available
                continue
    
    def requires(self):
        """
        All requirements needed:
            * Histograms coming from the PrePlot task.
            * Number of total events coming from the MergePreCounter task
              (to normalize MC histograms).
        """

        reqs = {}
        reqs["data"] = OrderedDict(
            ((dataset.name, category.name), PrePlot.vreq(self,
                dataset_name=dataset.name, category_name=self.get_data_category(category).name))
            for dataset, category in itertools.product(
                self.datasets_to_run, self.expand_category())
        )

        reqs["stats"] = OrderedDict()
        for dataset in self.datasets_to_run:
            if dataset.process.isData:
                continue
            reqs["stats"][dataset.name] = {}
            
            for elem in ["central"] + [f"{syst}_{d}"
                        for (syst, d) in itertools.product(self.norm_syst_list, self.directions)]:
                    syst = ""
                    d = ""
                    if "_" in elem:
                        syst = "_".join(elem.split("_")[0:-1])
                        d = elem.split("_")[-1]
                    if dataset.get_aux("secondary_dataset", None):
                        reqs["stats"][dataset.name][elem] = MergePreCounter.vreq(self,
                            dataset_name=dataset.get_aux("secondary_dataset"),
                            systematic=syst, systematic_direction=d)
                    else:
                        reqs["stats"][dataset.name][elem] = MergePreCounter.vreq(self,
                            dataset_name=dataset.name, systematic=syst, systematic_direction=d)

        return reqs

    def output(self):
        """
        Output files to be filled: pdf, png, root or json
        """
        # output definitions, i.e. key, file prefix, extension
        output_data = []
        output_data.append(("txt", "", "txt"))
        if self.save_root:
            output_data.append(("root", "", "root"))

        channel = self.region.name.split("_")[0]

        return {
            key: law.SiblingFileCollection(OrderedDict(
                (feature.name, self.local_target("{}{}_{}_{}.{}".format(
                    prefix, feature.name, channel,
                    "tb"+str(feature.get_aux("target_bin_count", 20)), ext)))
                for feature in self.features if feature.name in self.features_to_flatten
            ))
            for key, prefix, ext in output_data
        }

    def complete(self):
        """
        Task is completed when all output are present
        """
        return ConfigTaskWithCategory.complete(self)

    def get_nevents(self, inputs=None):
        """ Open MergePreCounter outputs and load json files with nevents and nweightedevents (for normalization)
        Arguments : inputs : results of self.input(), facultative, for caching
        Returns tuple :
         - nevents (event count or weights depending on self.apply_weights)
         - nweightedevents : sum of weights
         - nunweightedevents : event count
        """
        nevents, nweightedevents, nunweightedevents = {}, {}, {}
        if inputs is None:
            inputs = self.input() # this is quite slow so bring it outside the loop (maybe we could enable cache_requirements ?)
        for iproc, (process, datasets) in enumerate(self.processes_datasets.items()):
            if not process.isData:
                for dataset in datasets:
                    nevents[dataset.name] = {}
                    nweightedevents[dataset.name] = {}
                    nunweightedevents[dataset.name] = {}
                    directions = ["up", "down"]

                    for elem in ["central"] + [f"{syst}_{d}"
                            for (syst, d) in itertools.product(self.norm_syst_list, directions)]:
                        inp = inputs["stats"][dataset.name][elem]
                        with open(inp.path) as f:
                            stats = json.load(f)
                            nweightedevents[dataset.name][elem] = stats["nweightedevents"]
                            nunweightedevents[dataset.name][elem] = stats["nevents"]
                            if self.apply_weights:
                                nevents[dataset.name][elem] = stats["nweightedevents"]
                            else:
                                nevents[dataset.name][elem] = stats["nevents"]

        return nevents, nweightedevents, nunweightedevents

    def get_normalization_factor(self, dataset, elem):
        if not type(self.config.lumi_pb) == dict:
            lumi = self.config.lumi_pb
        elif self.run_era != "":
            lumi = self.config.lumi_pb[dataset.runPeriod][self.run_era]
        else:
            lumi = sum(self.config.lumi_pb.get(dataset.runPeriod, {}).values())

        if dataset.get_aux("stitchingNormalization", False) and self.apply_weights:
            # Normalization for stitched datasets, where generator weights are scaled to their average
            # needs to be combined with appropriate stitching weights
            return dataset.xs * lumi / (self.nweightedevents[dataset.name][elem] / self.nunweightedevents[dataset.name][elem])
        else:
            return dataset.xs * lumi / self.nevents[dataset.name][elem]

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()

        self.nevents, self.nweightedevents, self.nunweightedevents = self.get_nevents(inputs)

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        self.background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal]

        for ifeat, feature in enumerate(self.features):
            # skip features that are not requested to be flattened
            if feature.name not in self.features_to_flatten: continue

            self.histos = {"background": [], "signal": [], "DY": [], "TT": [], "shape_bkg": {}, "shape_DY": {}, "shape_TT": {}}

            shape_systematics = self.get_systs(feature, True)

            systs_directions = [("central", "")]
            systs_directions += [("CMS_scale_j", "up"), ("CMS_scale_j", "down"), ("CMS_scale_t", "up"), ("CMS_scale_t", "down"), \
                                 ("CMS_res_j", "up"), ("CMS_res_j", "down"), ("CMS_res_e", "up"), ("CMS_res_e", "down"), \
                                 ("CMS_scale_e", "up"), ("CMS_scale_e", "down")]
            for (syst, d) in systs_directions:
                if syst != "central":
                    self.histos["shape_bkg"]["%s_%s" % (syst, d)] = []

            binning_args, y_axis_adendum = self.get_binning(feature, ifeat)
            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = "Events" + y_axis_adendum
            hist_title = "; %s; %s" % (x_title, y_title)

            for iproc, (process, datasets) in enumerate(self.processes_datasets.items()):
                for (syst, d) in systs_directions:
                    feature_name = feature.name if syst == "central" \
                        else "%s_%s_%s" % (feature.name, syst, d)
                    if process.isData: continue

                    process_histo = ROOT.TH1D(randomize(process.name), hist_title, *binning_args)
                    process_histo.process_label = str(process.label)
                    process_histo.cmt_process_name = process.name
                    process_histo.Sumw2()
                    for dataset in datasets:
                        dataset_histo = ROOT.TH1D(randomize("tmp"), hist_title, *binning_args)
                        dataset_histo.Sumw2()
                        for category in self.expand_category():
                            inp = inputs["data"][
                                (dataset.name, category.name)].collection.targets.values()
                            for elem in inp:
                                rootfile = ROOT.TFile.Open(elem.path)
                                if self.preplot_foldered_by_feature:
                                    histo = copy(rootfile.Get(f"histograms/{feature.name}_dir/{feature_name}"))
                                else:
                                    histo = copy(rootfile.Get(feature_name))
                                rootfile.Close()
                                if not histo:
                                    print(f"****WARNING: Histogram not found: {feature_name}   in file: {elem.path}")
                                if not isinstance(histo, ROOT.TH1):
                                    print(f"****WARNING: Object {feature_name} is not a TH1 histogram in file: {elem.path}")
                                if histo.GetEntries() != 0:
                                    dataset_histo.Add(histo)
                            elem = ("central"
                                    if syst == "central" or syst not in self.norm_syst_list
                                    else f"{syst}_{d}")
                            if self.nevents[dataset.name][elem] != 0:
                                dataset_histo.Scale(self.get_normalization_factor(dataset, elem))
                                scaling = dataset.get_aux("scaling", None)
                                if scaling:
                                    print(" ### Scaling {} histo by {} +- {}".format(
                                        dataset.name, scaling[0], scaling[1]))
                                    old_errors = [dataset_histo.GetBinError(ibin)\
                                        / dataset_histo.GetBinContent(ibin)
                                        if dataset_histo.GetBinContent(ibin) != 0 else 0
                                        for ibin in range(1, dataset_histo.GetNbinsX() + 1)]
                                    new_errors = [
                                        math.sqrt(elem ** 2 + (scaling[1] / scaling[0]) ** 2)
                                        for elem in old_errors]
                                    dataset_histo.Scale(scaling[0])
                                    for ibin in range(1, dataset_histo.GetNbinsX() + 1):
                                        dataset_histo.SetBinError(
                                            ibin, dataset_histo.GetBinContent(ibin)
                                                * new_errors[ibin - 1])

                        process_histo.Add(dataset_histo)

                    if process.name in self.additional_scaling:
                        process_histo.Scale(self.additional_scaling[process.name])

                    yield_error = c_double(0.)
                    process_histo.cmt_yield = process_histo.IntegralAndError(0,
                        process_histo.GetNbinsX() + 1, yield_error)
                    process_histo.cmt_yield_error = yield_error.value

                    process_histo.cmt_bin_yield = []
                    process_histo.cmt_bin_yield_error = []
                    for ibin in range(1, process_histo.GetNbinsX() + 1):
                        process_histo.cmt_bin_yield.append(process_histo.GetBinContent(ibin))
                        process_histo.cmt_bin_yield_error.append(process_histo.GetBinError(ibin))
                    if syst == "central":
                        if process.isSignal:
                            self.histos["signal"].append(process_histo)
                        else:
                            self.histos["background"].append(process_histo)
                        if process.name == "DY":
                            self.histos["DY"] = process_histo.Clone()
                        if process.name == "TT":
                            self.histos["TT"] = process_histo.Clone()
                    else:
                        if not process.isSignal:
                            self.histos["shape_bkg"]["%s_%s" % (syst, d)].append(process_histo)
                        if process.name == "DY":
                            self.histos["shape_DY"]["%s_%s" % (syst, d)] = process_histo.Clone()
                        if process.name == "TT":
                            self.histos["shape_TT"]["%s_%s" % (syst, d)] = process_histo.Clone()

            signal_sum = None
            background_sum = None
            bkg_syst = {}
            for hist in self.histos["signal"]:
                if not signal_sum: signal_sum = hist.Clone()
                else:              signal_sum.Add(hist.Clone())
            for hist in self.histos["background"]:
                if not background_sum: background_sum = hist.Clone()
                else:                  background_sum.Add(hist.Clone())
            for (syst, d) in systs_directions:  
                # for hist in self.histos["shape_sig"][["%s_%s" % (syst, d)]]:
                #     if not sig_syst["%s_%s" % (syst, d)]: sig_syst["%s_%s" % (syst, d)] = hist.Clone()
                #     else:                                 sig_syst["%s_%s" % (syst, d)].Add(hist.Clone())
                if syst == "central": continue
                for hist in self.histos["shape_bkg"]["%s_%s" % (syst, d)]:
                    if not "%s_%s" % (syst, d) in bkg_syst: bkg_syst["%s_%s" % (syst, d)] = hist.Clone()
                    else:                                          bkg_syst["%s_%s" % (syst, d)].Add(hist.Clone())

            if self.use_bkg_flattening:
                self.histogram_bin_merger = FlatSignalBackgroundBinMerger(
                    sgn_histo=signal_sum,
                    bkg_histo=background_sum,
                    bkg_syst=bkg_syst,
                    dy_histo = self.histos["DY"],
                    tt_histo = self.histos["TT"],
                    dy_syst = self.histos["shape_DY"],
                    tt_syst = self.histos["shape_TT"],
                    target_bin_count=feature.get_aux("target_bin_count", 20),
                    min_MC_events=feature.get_aux("min_MC_events", 10),
                    min_MC_events_lower_bins=feature.get_aux("min_MC_events_lower_bins", 25)
                )
            elif self.use_cumulative:
                self.histogram_bin_merger = FlatSigCumulativeRebinner(
                    sgn_histo=signal_sum,
                    bkg_histo=background_sum,
                    target_bin_count=feature.get_aux("target_bin_count", 20),
                    min_MC_events=feature.get_aux("min_MC_events", 10)
                )
            else:
                self.histogram_bin_merger = FlatSignalBinMerger(
                    sgn_histo=signal_sum,
                    bkg_histo=background_sum,
                    target_bin_count=feature.get_aux("target_bin_count", 20),
                    min_MC_events=feature.get_aux("min_MC_events", 10)
                )
            for idx, hist in enumerate(self.histos["background"]):
                self.histos["background"][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)
            for idx, hist in enumerate(self.histos["signal"]):
                self.histos["signal"][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)

            # save binning
            np.savetxt(
                    create_file_dir(self.output()["txt"].targets[feature.name].path),
                    self.histogram_bin_merger.edges_array
                )

            if self.save_root:
                f = ROOT.TFile.Open(create_file_dir(
                    self.output()["root"].targets[feature.name].path), "RECREATE")
                f.cd()

                data_already_stored=False
                hist_dir = f.mkdir("histograms")
                hist_dir.cd()

                background_sum.Write("background")
                for hist in self.histos["background"]:
                    hist.Write(hist.cmt_process_name)

                for hist in self.histos["signal"]:
                    hist.Write(hist.cmt_process_name)

                f.Close()
