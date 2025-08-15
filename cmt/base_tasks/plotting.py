# coding: utf-8

"""
Plotting tasks.
"""

__all__ = []

import os
from copy import deepcopy as copy
import json
import math
import itertools
import array
from collections import OrderedDict
import numpy as np
import pandas as pd

import law
import luigi
<<<<<<< HEAD
from cmt.util import hist_to_graph
=======
from cmt.util import hist_to_array, hist_to_graph, get_graph_maximum, update_graph_values, get_lumi_string
>>>>>>> d4f8e23 (Fixing labels + proper handle of multiple configs)

from ctypes import c_double

from analysis_tools import Process
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection, randomize
)
from plotting_tools.root import get_labels

from cmt.base_tasks.base import (
    ConfigTask, DatasetTaskWithCategory, ProcessGroupNameTask, MultiConfigProcessGroupNameTask,
    HTCondorWorkflow, SGEWorkflow, SlurmWorkflow, ConfigTaskWithCategory,
    ConfigTaskWithRegion, RDFModuleTask, InputData, FitBase, QCDABCDTask,
    get_categorization_merging_factor, FlatSignalBinMerger, FakeFactorsTask
)

from cmt.base_tasks.preprocessing import (
    Categorization, MergeCategorization, MergePreCounter, DatasetCategoryWrapperTask
)

EMPTY = -1.e5

directions = ["up", "down"]

class BasePlotTask(ConfigTaskWithRegion):
    """
    Task that wraps parameters used in all plotting tasks. Can't be run.

    :param feature_names: names of features to plot. Uses all features when empty.
    :type feature_names: csv string

    :param feature_tags: list of tags for filtering features selected via feature names.
    :type feature_tags: csv string

    :param skip_feature_names: names or name pattern of features to skip
    :type skip_feature_names: csv string

    :param skip_feature_tags: list of tags of features to skip
    :type skip_feature_tags: csv string

    :param apply_weights: whether to apply weights and scaling to all histograms.
    :type apply_weights: bool

    :param n_bins: Custom number of bins for plotting,
        defaults to the value configured by the feature when empty.
    :type n_bins: int

    :param systematics: NOT YET IMPLEMENTED. List of custom systematics to be considered.
    :type systematics: csv list

    :param store_systematics: whether to store systematic templates inside the output root files.
    :type store_systematics: bool

    :param remove_horns: NOT YET IMPLEMENTED. Whether to remove the eta horns present in 2017
    :type remove_horns: bool

    :param optimization_method: Optimization method to be used. Only `bayesian_blocks` available.
    :type optimization_method: str

    """

    feature_names = law.CSVParameter(default=(), description="names of features to plot, uses all "
        "features when empty, default: ()")
    feature_tags = law.CSVParameter(default=(), description="list of tags for filtering features "
        "selected via feature_names, default: ()")
    skip_feature_names = law.CSVParameter(default=(), description="names or name pattern of "
        "features to skip, default: ()")
    skip_feature_tags = law.CSVParameter(default=(), description="list of tags of "
        "features to skip, default: (multiclass_dnn,)")
    apply_weights = luigi.BoolParameter(default=True, description="whether to apply "
        "mc weights to histograms, default: True")
    n_bins = luigi.IntParameter(default=law.NO_INT, description="custom number of bins for "
        "plotting, defaults to the value configured by the feature when empty, default: empty")
    systematics = law.CSVParameter(default=(), description="list of systematics, default: empty")
    store_systematics = luigi.BoolParameter(default=True, description="whether to store "
        "systematic templates inside root files, default: True")
    remove_horns = luigi.BoolParameter(default=False, description="whether to remove horns "
        "from distributions, default: False")
    optimization_method = luigi.ChoiceParameter(default="", choices=("", "flat_sgn", "bayesian_blocks"),
        significant=False, description="optimization method to be used, default: none")
    preplot_foldered_by_feature=luigi.BoolParameter(default=False, description="whether to store"
        " PrePlot histograms organised in folders in the ROOT file, default: False")

    def __init__(self, *args, **kwargs):
        super(BasePlotTask, self).__init__(*args, **kwargs)
        # select features
        self.features = self.get_features()

    def _find_features(self, names, tags, config=None):
        if not config:
            config = self.config

        features = []

        used_names = {name: False for name in names if "(" not in name}
        for pattern in names:
            for feature in config.features:
                if law.util.multi_match(feature.name, pattern):
                    used_names[pattern] = True
                    features.append(feature)

        if not all(used_names.values()):
            raise ValueError(
                f"Feature names/patterns {[key for key, value in used_names.items() if not value]} "
                "are not used. Check spelling or remove from the command. "
            )

        used_tags = {tag: False for tag in tags}
        for tag in tags:
            for feature in config.features:
                if feature.has_tag(tag):
                    used_tags[tag] = True
                    if feature not in features:
                        features.append(feature)

        if not all(used_tags.values()):
            raise ValueError(
                f"Feature tags {[key for key, value in used_tags.items() if not value]} "
                "are not used. Check spelling or remove from the command. "
            )

        return features

    def get_features(self):
        features = []

        # first get features to skip
        skip_features = self._find_features(self.skip_feature_names, self.skip_feature_tags)

        features = [feature for feature in self._find_features(self.feature_names, self.feature_tags)
            if feature not in skip_features]

        # add extra features created on the fly
        for feature_name in self.feature_names:
            if "(" in feature_name:
                features.append(eval(feature_name))

        if len(features) == 0:
            raise ValueError("No features were included. Did you spell them correctly?")
        return features

    def round(self, number):
        if abs(number - round(number)) < 0.0001:
            return number
        if number > 10.:
            return round(number)
        elif number > 1.:
            return round(number, 1)
        i = 1
        while True:
            if number > (1 / (10 ** i)):
                return round(number, i + 1)
            i += 1

    def get_binning(self, feature, ifeat=0):
        if self.optimization_method == "bayesian_blocks":
            y_axis_adendum = ""
            binning = self.input()["bin_opt"].collection.targets[ifeat].load(formatter="json")
            n_bins = len(binning) - 1
            binning_args = n_bins, array.array("f", binning)
        elif isinstance(feature.binning, tuple):
            if self.n_bins == law.NO_INT:
                nbins = feature.binning[0]
            else:
                nbins = self.n_bins
            y_axis_adendum = (" / %s %s" % (
                self.round((feature.binning[2] - feature.binning[1]) / nbins),
                    feature.get_aux("units")) if feature.get_aux("units") else "")
            binning = (nbins, feature.binning[1], feature.binning[2])
            binning_args = tuple(binning)
        else:
            y_axis_adendum = ""
            if self.n_bins == law.NO_INT:
                n_bins = len(feature.binning) - 1
                binning_args = n_bins, array.array("f", feature.binning)
            else:
                n_bins = self.n_bins
                binning_args = tuple(n_bins, feature.binning[0], feature.binning[-1])

        return binning_args, y_axis_adendum

    def get_feature_systematics(self, feature):
        return feature.systematics

    def get_systs(self, feature, isMC, category=None):
        if not isMC:
            return []
        if category == None:
            category = self.category
        systs = self.get_feature_systematics(feature) \
            + self.config.get_weights_systematics(self.config.weights[category.name], isMC)
        systs += self.config.get_systematics_from_expression(category.selection)
        if self.region:
            systs += self.config.get_systematics_from_expression(self.region.selection)
        return self.get_unique_systs(systs)

    def get_unique_systs(self, systs):
        unique_systs = []
        for syst in systs:
            if syst not in unique_systs:
                unique_systs.append(syst)
        return unique_systs

    def get_output_postfix(self):
        postfix = ""
        if self.region:
            postfix += "__" + self.region.name
        if not self.apply_weights:
            postfix += "__noweights"
        if self.optimization_method == "bayesian_blocks":
            postfix += "__opt_bb"
        if self.n_bins != law.NO_INT:
            postfix += "__%s_bins" % self.n_bins
        return postfix


class PrePlot(RDFModuleTask, DatasetTaskWithCategory, BasePlotTask, law.LocalWorkflow,
        HTCondorWorkflow, SGEWorkflow, SlurmWorkflow, FakeFactorsTask):
    """
    Performs the filling of histograms for all features considered. If systematics are considered,
    it also produces the same histograms after applying those.

    :param skip_processing: whether to skip the preprocessing and categorization steps.
    :type skip_processing: bool
    :param skip_merging: whether to skip the MergeCategorization task.
    :type skip_merging: bool
    :param preplot_modules_file: filename inside ``cmt/config/`` or ``../config/`` (w/o extension)
        with the RDF modules to run.
    :type preplot_modules_file: str
    """

    skip_processing = luigi.BoolParameter(default=False, description="whether to skip"
        " preprocessing and categorization steps, default: False")
    skip_merging = luigi.BoolParameter(default=False, description="whether to skip"
        " MergeCategorization task, default: False")
    preplot_modules_file = luigi.Parameter(description="filename with modules to run RDataFrame",
        default=law.NO_STR)
    dataset_names = law.CSVParameter(description="dataset_names to use for optimization",
        default=())

    def __init__(self, *args, **kwargs):
        super(PrePlot, self).__init__(*args, **kwargs)
        self.custom_output_tag = "_%s" % self.region_name
        self.threshold = self.dataset.get_aux("event_threshold", None)
        self.merging_factor = self.dataset.get_aux("preprocess_merging_factor", None)

        self.syst_list = self.get_syst_list()

    def get_syst_list(self):
        """
        Returns a list of systematic names that affect present or past selections, so dedicated
        input ntuples are needed as requirements
        """
        if self.skip_processing:
            return []
        syst_list = []
        isMC = self.dataset.process.isMC
        for feature in self.features:
            systs = self.get_unique_systs(self.get_systs(feature, isMC))
            for syst in systs:
                if syst in syst_list:
                    continue
                try:
                    systematic = self.config.systematics.get(syst)
                    if self.category.name in systematic.get_aux("affected_categories", []):
                        syst_list.append(syst)
                except:
                    continue
        other_systs = []
        if isMC:
            other_systs += self.config.get_systematics_from_expression(self.category.selection)
            if self.region:
                other_systs += self.config.get_systematics_from_expression(self.region.selection)
        for syst in other_systs:
            if syst in syst_list:
                continue
            try:
                systematic = self.config.systematics.get(syst)
                if self.category.name in systematic.get_aux("affected_categories", []):
                    syst_list.append(syst)
            except:
                continue

        return syst_list

    def create_branch_map(self):
        """
        :return: number of files after merging (usually 1) unless skip_processing == True
        :rtype: int
        """
        # number of files of InputData. In a lambda to not compute i unless needed
        input_data_count = lambda : len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config_name), add_prefix=False,
                check_empty=True))
        if self.skip_processing:
            return input_data_count()
        elif self.skip_merging:
            categorization_max_events = self.dataset.get_aux("categorization_max_events", None)
            if categorization_max_events is None:
                categorization_merging_factor = get_categorization_merging_factor(self.dataset, self.category)
                if categorization_merging_factor:
                    return min(categorization_merging_factor,input_data_count())
                else:
                    return input_data_count()
            else:
                # in case we have used the Categorization splitting output
                with open(create_file_dir(os.path.expandvars(
                        "$CMT_TMP_DIR/%s/splitted_branches_categorization_%s/%s.json" % (
                        self.config_name, categorization_max_events, self.dataset.name)))) as f:
                    return len(json.load(f))
        return self.n_files_after_merging # in case we use MergeCategorization

    def workflow_requires(self):
        """
        """
        if self.skip_merging:
            reqs = {"data": {"central": Categorization.vreq(self, workflow="local")}}
            if self.store_systematics:
                for syst, d in itertools.product(self.syst_list, directions):
                    reqs["data"][f"{syst}_{d}"] = Categorization.vreq(self, workflow="local",
                        systematic=syst, systematic_direction=d)
        elif self.skip_processing:
            reqs= {"data": {"central": InputData.req(self)}}
        else:
            reqs = {"data": {"central": MergeCategorization.vreq(self, workflow="local")}}
            if self.store_systematics:
                for syst, d in itertools.product(self.syst_list, directions):
                    reqs["data"][f"{syst}_{d}"] = MergeCategorization.vreq(self, workflow="local",
                        systematic=syst, systematic_direction=d)
        if self.optimization_method == "bayesian_blocks":
            from cmt.base_tasks.optimization import BayesianBlocksOptimization
            reqs["bin_opt"] = BayesianBlocksOptimization.vreq(self, plot_systematics=False,
                _exclude=["branch", "branches", "custom_output_tag", "plot_systematics", "workflow"])
        return reqs

    def requires(self):
        """
        Each branch requires one input file for the central value and two per systematic considered
        """
        if self.skip_merging:
            reqs = {"central": Categorization.vreq(self, workflow="local", branch=self.branch)}
            if self.store_systematics:
                for syst, d in itertools.product(self.syst_list, directions):
                    reqs[f"{syst}_{d}"] = Categorization.vreq(self, workflow="local",
                        systematic=syst, systematic_direction=d, branch=self.branch)
        elif self.skip_processing:
            reqs= {"central": InputData.req(self, file_index=self.branch)}
        else:
            reqs = {"central": MergeCategorization.vreq(self, workflow="local", branch=self.branch)}
            if self.store_systematics:
                for syst, d in itertools.product(self.syst_list, directions):
                    reqs[f"{syst}_{d}"] = MergeCategorization.vreq(self, workflow="local",
                        systematic=syst, systematic_direction=d, branch=self.branch)
        if self.optimization_method == "bayesian_blocks":
            from cmt.base_tasks.optimization import BayesianBlocksOptimization
            reqs["bin_opt"] = BayesianBlocksOptimization.vreq(self, plot_systematics=False,
                _exclude=["branch", "branches", "custom_output_tag", "plot_systematics", "workflow"])
        return reqs

    def output(self):
        """
        :return: One file per input file with all histograms to be plotted for each feature
        :rtype: `.root`
        """
        return self.local_target("data{}_{}.root".format(
            self.get_output_postfix(), self.branch))

    def get_weight(self, category, syst_name, syst_direction, **kwargs):
        """
        Obtains the product of all weights depending on the category/channel applied.
        Returns "1" if it's a data sample or the apply_weights parameter is set to False.

        :return: Product of all weights to be applied
        :rtype: str
        """

        # retrieve weigths from the main configuration file
        weights = self.config.get_weights_expression(self.config.weights[category], syst_name, syst_direction)
        if self.do_ff:
            FFcomb = self.config.get_weights_expression(self.config.weights["FakeFactor"], syst_name, syst_direction)

        # no weigth applied at all
        if not self.apply_weights:
            return "1"

        if self.config.processes.get(self.dataset.process.name).isData:
            # apply it also to data because the fakes are estimated as (data-MC)*FFcomb
            # --> at the PrePlot step we can distribute the FFcomb weight
            if self.do_ff and self.region.name.endswith(self.ff_shape_region):
                return FFcomb
            # no weigth applied to data otherwise
            else:
                return "1"

        # all weights applied to MC
        else:
            return weights

        # apply default weights if no other condition is fulfilled
        return self.config.weights.default

    def define_histograms(self, dfs, nentries):
        ROOT = import_root()
        histos = []
        isMC = self.dataset.process.isMC
        for ifeat, feature in enumerate(self.features):
            binning_args, y_axis_adendum = self.get_binning(feature, ifeat)
            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = "Events" + y_axis_adendum
            title = "; %s; %s" % (x_title, y_title)

            systs_directions = [("central", "")]
            if isMC and self.store_systematics:
                systs = self.get_systs(feature, isMC)
                systs_directions += list(itertools.product(systs, directions))

            # loop over systematics and up/down variations
            for syst_name, direction in systs_directions:

                # Select the appropriate RDF (input file) depending on the syst and direction
                key = "central"
                if f"{syst_name}_{direction}" in dfs:
                    key = f"{syst_name}_{direction}"
                df = dfs[key]

                # apply selection if needed
                if feature.selection and nentries[key] > 0:
                    feat_df = df.Define("feat_selection", self.config.get_object_expression(
                        feature.selection, isMC, syst_name, direction)).Filter("feat_selection")
                else:
                    feat_df = df
                # define tag just for the histogram's name
                if syst_name != "central" and isMC:
                    tag = "_%s" % syst_name
                    if direction != "":
                        tag += "_%s" % direction
                else:
                    tag = ""
                feature_expression = self.config.get_object_expression(
                    feature, isMC, syst_name, direction)
                feature_name = feature.name + tag
                hist_base = ROOT.TH1D(feature_name, title, *binning_args)

                if nentries[key] > 0:
                    hmodel = ROOT.RDF.TH1DModel(hist_base)
                    histos.append(
                        feat_df.Define(
                            "weight", "{}".format(self.get_weight(
                                self.category.name, syst_name, direction))
                        ).Define(
                            "var", feature_expression).Histo1D(hmodel, "var", "weight")
                    )
                else:  # no entries available, append empty histogram
                    histos.append(hist_base)
        return histos

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules
        and produces a set of plots per each feature, one for the nominal value
        and others (if available) for all systematics.
        """
        ROOT = import_root()

        # prepare inputs and outputs
        if self.skip_processing:
            inp = self.get_input()
        else:
            inp = self.input()
        outp = self.output().path

        ROOT.ROOT.EnableThreadSafety()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # create one RDF for the central value and each needed systematic
        dfs = {}
        nentries = {}
        systs_list_to_store = []
        if self.store_systematics:
            systs_list_to_store = [f"{syst}_{d}"
                for (syst, d) in itertools.product(self.syst_list, directions)]

        for elem in ["central"] + systs_list_to_store:
            if self.skip_processing:
                inp_to_consider = self.get_path(inp[elem])[0]
                if not self.dataset.friend_datasets:
                    dfs[elem] = self.RDataFrame(self.tree_name, self.get_path(inp[elem]),
                        allow_redefinition=self.allow_redefinition)
                # friend tree
                else:
                    tchain = ROOT.TChain()
                    for f in self.get_path(inp[elem]):
                        tchain.Add("{}/{}".format(f, self.tree_name))
                    friend_tchain = ROOT.TChain()
                    for f in self.get_path(inp[elem], 1):
                        friend_tchain.Add("{}/{}".format(f, self.tree_name))
                    tchain.AddFriend(friend_tchain, "friend")
                    dfs[elem] = self.RDataFrame(tchain, allow_redefinition=self.allow_redefinition)
            elif self.skip_merging:
                inp_to_consider = inp[elem]["root"].path
                dfs[elem] = self.RDataFrame(self.tree_name, inp_to_consider,
                    allow_redefinition=self.allow_redefinition)
            else:
                inp_to_consider = inp[elem].targets[self.branch].path
                dfs[elem] = self.RDataFrame(self.tree_name, inp_to_consider,
                    allow_redefinition=self.allow_redefinition)

            empty_file = False
            tf = ROOT.TFile.Open(inp_to_consider)
            tree = tf.Get(self.tree_name)
            if not tree: # no tree inside the file
                raise RuntimeError(f"PrePlot : Input file '{inp_to_consider}' has no TTree named '{self.tree_name}'. "
                                    "Try removing the file and running the task again.")
            nentries[elem] = tree.GetEntries()
            if nentries[elem] == 0: # tree with 0 entries
                empty_file = True
            tf.Close()

            # applying modules according to the systematic considered
            syst = ""
            d = ""
            if "_" in elem:
                syst = elem.rsplit("_", 1)[0]
                d = elem.rsplit("_", 1)[1]
            if not empty_file:
                modules = self.get_feature_modules(self.preplot_modules_file,
                    systematic=syst, systematic_direction=d)
                if len(modules) > 0:
                    for module in modules:
                        dfs[elem], _ = module.run(dfs[elem])

                selection = "1"
                dataset_selection = self.config.get_object_expression(
                    self.dataset.get_aux("selection", "1"), self.dataset.process.isMC, syst, d)

                if self.skip_processing:
                    selection = self.config.get_object_expression(
                        self.category, self.dataset.process.isMC, syst, d)

                if dataset_selection and dataset_selection != "1":
                    if selection != "1":
                        selection = join_root_selection(dataset_selection, selection, op="and")
                    else:
                        selection = dataset_selection

                if self.region_name != law.NO_STR:
                    region_selection = self.config.get_object_expression(
                        self.config.regions.get(self.region_name).selection,
                        self.dataset.process.isMC, syst, d)
                    if selection != "1":
                        selection = join_root_selection(region_selection, selection, op="and")
                    else:
                        selection = region_selection

                if selection != "1":
                    dfs[elem] = dfs[elem].Define("selection", selection).Filter("selection")

        histos = self.define_histograms(dfs, nentries)

        out = ROOT.TFile.Open(create_file_dir(outp), "RECREATE")

        if self.preplot_foldered_by_feature:
            out.mkdir("histograms")
            out.cd("histograms")
            for feature in self.features:
                feature_name = feature.name
                out.mkdir(f"histograms/{feature_name}_dir")
                out.cd(f"histograms/{feature_name}_dir")

                for histo in histos:
                    if not feature_name in histo.GetName(): continue

                    histo = histo.Clone()
                    histo.Sumw2()
                    histo.Write()

                out.cd()

        else:
            for histo in histos:
                histo = histo.Clone()
                histo.Sumw2()
                out.cd()
                histo.Write()


        out.Close()


class PrePlotWrapper(DatasetCategoryWrapperTask, BasePlotTask):

    def atomic_requires(self, dataset, category):
        return PrePlot.req(self, dataset_name=dataset.name, category_name=category.name)


class EqualBinWidthTransformer:
    """
    Helper tool that, given a model histogram, will change
    the x axis values to make the bins have equal width
    """
    def __init__(self, h_model):
        self.n_bins = h_model.GetNbinsX()
        self.h_model = h_model

    def convert(self, old_h):
        """ convert the histogram to fixed-width plotting binning """
        ROOT = import_root()
        assert old_h.GetNbinsX() == self.n_bins
        new_h = ROOT.TH1D(randomize(old_h.GetName()), old_h.GetTitle(), self.n_bins, 0, self.n_bins)
        new_h.Set(old_h.GetNbinsX()+2, old_h.GetArray())
        new_h.GetSumw2().__assign__(old_h.GetSumw2())

        attributes = ["hist_type", "process_label", "legend_style", "cmt_scale",
                      "cmt_process_name", "cmt_yield", "cmt_yield_error",
                      "cmt_bin_yield", "cmt_bin_yield_error"]
        for histo_attr in attributes:
            try:
                setattr(new_h, histo_attr, getattr(old_h, histo_attr))
            except AttributeError: pass

        return new_h

    def convert_labels(self, dummy_hist, show_ratio):
        """ Set the histogram labels on the given dummy_hist """
        dummy_hist.GetXaxis().SetNdivisions(self.n_bins, 0, 0, False)
        precision = 3 # Use precision fixed to 3 decimals
        for label_i in range(1, self.n_bins+2): # Labels start at 1
            # Label options here below
            # 90 -> angle of the text
            # 22 -> to center label on the axis tick (see TAttTex alignment)
            dummy_hist.GetXaxis().ChangeLabel(label_i, 90, -1, 22, -1, -1, f"{self.h_model.GetXaxis().GetBinLowEdge(label_i):.{precision}f}")
            if show_ratio:
                dummy_hist.GetXaxis().SetLabelOffset(0.08)
                dummy_hist.GetXaxis().SetTitleOffset(1.9)
            else:
                dummy_hist.GetXaxis().SetLabelOffset(0.02)
                dummy_hist.GetXaxis().SetTitleOffset(1.9)


class FeaturePlot(ConfigTaskWithCategory, BasePlotTask, FitBase, ProcessGroupNameTask,
                  QCDABCDTask, FakeFactorsTask):
    """
    Performs the actual histogram plotting: loads the histograms obtained in the PrePlot tasks,
    rescales them if needed and plots and saves them.

    Example command:

    ``law run FeaturePlot --version test --category-name etau --config-name ul_2018 \
--process-group-name etau --feature-names Htt_svfit_mass,lep1_pt,bjet1_pt,lep1_eta,bjet1_eta \
--workers 20 --PrePlot-workflow local --stack --hide-data False --do-qcd --region-name etau_os_iso\
--dataset-names tt_dl,tt_sl,dy_high,wjets,data_etau_a,data_etau_b,data_etau_c,data_etau_d \
--MergePreCounter-version test_old``

    :param stack: whether to show all backgrounds stacked (True) or normalized to 1 (False)
    :type stack: bool

    :param show_ratio: whether to show the data/mc ratio plot (True) or not (False)
    :type stack: bool

    :param ratio_min: minimum value for the y axis in the ratio-plot
    :type max_y: float

    :param ratio_maz: maximum value for the y axis in the ratio-plot
    :type max_y: float

    :param ratio_ndivisions: number of divisions for the y axis in the ratio-plot
    :type max_y: int

    :param hide_data: whether to show (False) or hide (True) the data histograms
    :type hide_data: bool

    :param normalize_signals: whether to normalize signals to the
        total background yield (True) or not (False)
    :type normalize_signals: bool

    :param avoid_normalization: whether to avoid normalizing by cross section and initial
        number of events
    :type avoid_normalization: bool

    :param blinded: whether to blind data in specified regions. The blinding ranges are specified
        using the `blinded_range` parameter in the Feature definition. This parameter can include a
        list ([initial, final]) or a list of lists ([[init_1, fin_1], [init_2, fin_2], ...])
    :type blinded: bool

    :param save_png: whether to save plots in png
    :type save_png: bool

    :param save_pdf: whether to save plots in pdf
    :type save_pdf: bool

    :param save_root: whether to write plots in a root file
    :type save_root: bool

    :param save_yields: whether to save histogram yields in a json file
    :type save_yields: bool

    :param process_group_name: name of the process grouping name
    :type process_group_name: str

    :param bins_in_x_axis: (NOT YET IMPLEMENTED) whether to plot histograms with the bin numbers
        in the x axis instead of the actual values
    :type bins_in_x_axis: bool

    :param plot_systematics: (NOT YET FULLY IMPLEMENTED) whether to plot histograms
        with their uncertainties
    :type plot_systematics: bool

    :param fixed_colors: whether to plot histograms with their defined colors (False) or fixed colors
        (True) starting from ``ROOT`` color ``2``.
    :type fixed_colors: bool

    :param max_y: maximum value for the y axis. Applies only to 1D plots.
    :type max_y: float

    :param min_y: minimum value for the y axis. Applies only to 1D plots.
    :type min_y: float

    :param log_y: whether to set y axis to log scale
    :type log_y: bool

    :param log_x: whether to set y axis to log scale
    :type log_x: bool

    :param include_fit: YAML file inside config folder (w/o extension) including input parameters
        for the fit
    :type include_fit: str

    :param propagate_syst_qcd: whether to propagate systematics to qcd background
    :type propagate_syst_qcd: bool

    :param run_period: parameter to run only over the datasets for the specified run period of a
        given year. It also allows you to plot the run period with the proper format.
    :type run_period: str

    :param run_era: parameter to run only over the era dataset that has been specified. The lumi labels
        and the luminosity are also set accordingly.
    :type run_era: str

    :param equal_bin_width: make all bins have equal plot width
    :type equal_bin_width: bool

    """
    stack = luigi.BoolParameter(default=False, description="when set, stack backgrounds, weight "
        "them with dataset and category weights, and normalize afterwards, default: False")
    show_ratio = luigi.BoolParameter(default=True, description="Allow, in the defined cases, that "
        "data/mc plot is shown, default: True")
    ratio_min = luigi.FloatParameter(default=0.5, description="ratio plot y-axis minimum value, default: 0.5")
    ratio_max = luigi.FloatParameter(default=1.5, description="ratio plot y-axis maximum value, default: 1.5")
    ratio_ndivisions = luigi.IntParameter(default=5, description="number of y-axis divisions "
        "in the ratio-plot, default: 5")
    hide_data = luigi.BoolParameter(default=True, description="hide data events, default: True")
    normalize_signals = luigi.BoolParameter(default=False, description="whether to normalize "
        "signals to the total bkg yield, default: True")
    avoid_normalization = luigi.BoolParameter(default=False, description="whether to avoid "
        "normalizing by cross section and initial number of events, default: False")
    blinded = luigi.BoolParameter(default=False, description="whether to blind bins above a "
        "certain feature value, default: False")
    save_png = luigi.BoolParameter(default=False, description="whether to save created histograms "
        "in png, default: True")
    save_pdf = luigi.BoolParameter(default=True, description="whether to save created histograms "
        "in pdf, default: True")
    save_root = luigi.BoolParameter(default=False, description="whether to save created histograms "
        "in root files, default: False")
    save_yields = luigi.BoolParameter(default=False, description="whether to save event yields per "
        "process, default: False")
    bins_in_x_axis = luigi.BoolParameter(default=False, description="whether to show in the x axis "
        "bin numbers instead of the feature value, default: False")
    plot_systematics = luigi.BoolParameter(default=True, description="whether plot systematics, "
        "default: True")
    fixed_colors = luigi.BoolParameter(default=False, description="whether to use fixed colors "
        "for plotting, default: False")
    min_y = luigi.FloatParameter(default=law.NO_FLOAT, description="y-axis minimum value, "
        "default: None (varies with feature). Aplies only to 1D plots.")
    max_y = luigi.FloatParameter(default=law.NO_FLOAT, description="y-axis maximum value, "
        "default: None (varies with feature). Aplies only to 1D plots.")
    log_y = luigi.BoolParameter(default=False, description="set logarithmic scale for Y axis, "
        "default: False")
    log_x = luigi.BoolParameter(default=False, description="set logarithmic scale for X axis, "
        "default: False")
    include_fit = luigi.Parameter(default="", description="fit to be included in the plots, "
        "default: None")
    propagate_syst_qcd = luigi.BoolParameter(default=False, description="whether to propagate systematics to qcd background, "
        "default: False")
    run_period = luigi.Parameter(default="", description="plot only the specified period, "
        "default: None")
    run_era = luigi.Parameter(default="", description="plot only the specified era, "
        "default: None")
    equal_bin_width = luigi.BoolParameter(default=False, description="make all bins have equal plot width. "
        "default: False")
    # # optimization parameters
    # bin_opt_version = luigi.Parameter(default=law.NO_STR, description="version of the binning "
        # "optimization task to use, not used when empty, default: empty")
    # n_start_bins = luigi.IntParameter(default=10, description="number of bins to be used "
        # "as initial value for scans, default: 10")
    # threshold = luigi.FloatParameter(default=-1., description="threshold per bin, "
        # "default: -1.")
    # threshold_method = luigi.ChoiceParameter(default="yield",
        # choices=("yield", "raw_events", "tt_dy", "tt_and_dy", "tt_dy_and_yield",
            # "tt_and_dy_and_yield", "rel_error"),
        # significant=False, description="threshold method to be used, default: yield")
    # region_to_optimize = luigi.Parameter(default=law.NO_STR, description="region used for the "
        # "optimization, default: same region as plot")

    # Not implemented yet
    bin_opt_version = None
    # remove_horns = False

    default_process_group_name = "default"

    additional_scaling = {"dummy": 1}  # Temporary fix, the DictParameter fails when empty

    def __init__(self, *args, **kwargs):
        super(FeaturePlot, self).__init__(*args, **kwargs)
        # select processes and datasets
        assert sum([self.do_qcd, self.do_sideband, self.do_ff]) <= 1

        # get QCD regions when requested
        if self.do_qcd:
            self.qcd_regions = self.get_qcd_regions()

        self.sideband_regions = None
        if self.do_sideband:  # Several fixes may be needed later for this
            assert self.region.name == "signal"
            self.sideband_regions = {key: self.config.regions.get(key)
                for key in ["signal", "background"]}

        if self.do_ff:
            # complain when no data is present
            if not any(dataset.process.isData for dataset in self.datasets):
                raise Exception("No real dataset passed for Fake Factor method application")

            # get shape region for application of fake factor method
            self.ff_regions = self.config.get_ff_regions(region=self.region, category=self.category,
                shape_region=self.ff_shape_region, signal_region=self.ff_signal_region)

        # obtain the list of systematics that apply to the normalization only if this is done
        self.norm_syst_list = []
        if self.store_systematics:
            weights = self.config.weights.total_events_weights
            for weight in weights:
                try:
                    feature = self.config.features.get(weight)
                    for syst in feature.systematics:
                        if syst not in self.norm_syst_list:
                            self.norm_syst_list.append(syst)
                except:  # weight not defined as a feature -> no syst available
                    continue

        # initialise empty list to avoid missing variable error being thrown
        self.features_to_flatten = []

    # aux functions to be used inside __init__
    def get_qcd_regions(self, config=None):
        if not config:
            config = self.config
        wp = self.qcd_wp if self.qcd_wp != law.NO_STR else ""
        qcd_regions = config.get_qcd_regions(region=self.region, category=self.category,
            wp=wp, shape_region=self.shape_region, signal_region_wp=self.qcd_signal_region_wp,
            sym=self.qcd_sym_shape)

        # complain when no data is present
        if not any(process.isData or process.get_aux("isFakeData", False)
                for process in self.processes_datasets):
            raise Exception("no real dataset passed for QCD estimation")

        return qcd_regions

    def requires(self):
        """
        All requirements needed:

            * Histograms coming from the PrePlot task.

            * Number of total events coming from the MergePreCounter task \
              (to normalize MC histograms).

            * If estimating QCD, FeaturePlot for the three additional QCD regions needed.

            * If requested flat sgn distribution, output from FlatSignalBinMergerTask.
        """

        reqs = {}

        if self.optimization_method == "flat_sgn":
            from cmt.base_tasks.optimization import FlatSignalBinMergerTask
            channel_signal_region = self.region_name.split("_")[0]+"_os_iso"
            reqs["bin_opt"] = FlatSignalBinMergerTask.vreq(self, region_name=channel_signal_region, save_root=False)
            self.features_to_flatten = reqs["bin_opt"].features_to_flatten
            self.use_cumulative = reqs["bin_opt"].use_cumulative
        reqs["data"] = OrderedDict(
            ((dataset.name, category.name), PrePlot.vreq(self,
                dataset_name=dataset.name, category_name=self.get_data_category(category).name))
            for dataset, category in itertools.product(
                self.datasets_to_run, self.expand_category())
        )
        if not self.avoid_normalization:
            reqs["stats"] = OrderedDict()
            for dataset in self.datasets_to_run:
                if dataset.process.isData:
                    continue
                reqs["stats"][dataset.name] = {}
                for elem in ["central"] + [f"{syst}_{d}"
                        for (syst, d) in itertools.product(self.norm_syst_list, directions)]:
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

        if self.do_qcd:
            reqs["qcd"] = {
                key: self.req(self, region_name=region.name, blinded=False, hide_data=False,
                    do_qcd=False, stack=True, save_root=True, save_pdf=True, save_yields=False,
                    remove_horns=False,_exclude=["feature_tags", "shape_region",
                    "qcd_category_name", "qcd_sym_shape", "qcd_signal_region_wp"])
                for key, region in self.qcd_regions.items()
            }
            if self.qcd_category_name != "default":  # to be fixed
                raise ValueError("qcd_category_name is not currently implemented")
                reqs["qcd"]["ss_iso"] = FeaturePlot.vreq(self,
                    region_name=self.qcd_regions.ss_iso.name, category_name=self.qcd_category_name,
                    blinded=False, hide_data=False, do_qcd=False, stack=True, save_root=True,
                    save_pdf=True, save_yields=False, feature_names=(self.qcd_feature,),
                    bin_opt_version=law.NO_STR, remove_horns=self.remove_horns, _exclude=["feature_tags",
                        "shape_region", "qcd_category_name", "qcd_sym_shape", "bin_opt_version",
                        "qcd_signal_region_wp"])
                # reqs["qcd"]["os_inviso"] = FeaturePlot.vreq(self,
                    # region_name=self.qcd_regions.os_inviso.name, category_name=self.qcd_category_name,
                    # blinded=False, hide_data=False, do_qcd=False, stack=True, save_root=True,
                    # save_pdf=True, save_yields=True, feature_names=(self.qcd_feature,),
                    # remove_horns=self.remove_horns, _exclude=["feature_tags", "bin_opt_version"])
                reqs["qcd"]["ss_inviso"] = FeaturePlot.vreq(self,
                    region_name=self.qcd_regions.ss_inviso.name,
                    category_name=self.qcd_category_name,
                    blinded=False, hide_data=False, do_qcd=False, stack=True, save_root=True,
                    save_pdf=True, save_yields=False, feature_names=(self.qcd_feature,),
                    bin_opt_version=law.NO_STR, remove_horns=self.remove_horns, _exclude=["feature_tags",
                        "shape_region", "qcd_category_name", "qcd_sym_shape", "bin_opt_version",
                        "qcd_signal_region_wp"])

        if self.do_sideband:
            reqs["sideband"] = {
                key: self.req(self, region_name=region.name, blinded=False, hide_data=False,
                    do_sideband=False, stack=True, save_root=True, save_pdf=False, save_yields=False,
                    remove_horns=False,_exclude=["feature_tags", "shape_region",
                    "qcd_category_name", "qcd_sym_shape", "qcd_signal_region_wp"])
                for key, region in self.sideband_regions.items()
            }

        if self.do_ff:
            # PrePlot needs to be required for the fakes template region with do_ff=True
            # to make sure that the FFcomb weight is picked up
            reqs["ff"] = OrderedDict(
                ((dataset.name, category.name), PrePlot.vreq(self, do_ff=True,
                    region_name=self.ff_regions[self.ff_shape_region].name,
                    dataset_name=dataset.name, category_name=self.get_data_category(category).name))
                for dataset, category in itertools.product(
                    self.datasets_to_run, self.expand_category())
            )

            # FeaturePlot needs to be required for the fakes template region with do_ff=False
            # to make sure that it does not try to apply the FF procedure to itself
            for key, region in self.ff_regions.items():
                reqs["ff"][key] = self.req(self, region_name=region.name, blinded=False, hide_data=False,
                    do_qcd=False, do_ff=False, stack=True, save_root=True, save_pdf=True, save_yields=False,
                    remove_horns=False,_exclude=["feature_tags", "shape_region",
                    "qcd_category_name", "qcd_sym_shape", "qcd_signal_region_wp",
                    "ff_signal_region", "ff_shape_region"])

        if self.optimization_method == "bayesian_blocks":
            from cmt.base_tasks.optimization import BayesianBlocksOptimization
            reqs["bin_opt"] = BayesianBlocksOptimization.vreq(self, plot_systematics=False)

        if self.include_fit:
            import yaml
            from cmt.utils.yaml_utils import ordered_load
            with open(self.retrieve_file("config/{}.yaml".format(self.include_fit))) as f:
                fit_params = ordered_load(f, yaml.SafeLoader)

            # what if the process_name use for fitting is not considered in the process_group_name?
            # in principle it should crash, but let's give it a chance by checking whether the
            # process_group_name it's actually a process. In that case, swap the process_name with
            # the process_group_name
            if fit_params["process_name"] not in [p.name for p in self.processes_datasets.keys()]:
                try:
                    process = self.config.processes.get(self.process_group_name)
                    fit_params["process_name"] = self.process_group_name
                except ValueError:
                    raise ValueError(f"{fit_params['process_name']} is not among the processes "
                        f"considered by the process_group_name {self.process_group_name} and "
                        f"{self.process_group_name} is not a process, so the fit can't be included.")

            from cmt.base_tasks.analysis import Fit
            params = ", ".join([f"{param}='{value}'"
                for param, value in fit_params.items() if param != "fit_parameters"])
            if "fit_parameters" in fit_params:
                params += ", fit_parameters={" + ", ".join([f"'{param}': '{value}'"
                for param, value in fit_params["fit_parameters"].items()]) + "}"
            reqs["fit"] = eval(f"Fit.vreq(self, {params}, _exclude=['include_fit'])")

        return reqs

    def get_output_postfix(self, key="pdf"):
        """
        :return: string to be included in the output filenames
        :rtype: str
        """
        postfix = super(FeaturePlot, self).get_output_postfix()
        if self.process_group_name != self.default_process_group_name:
            postfix += "__pg_" + self.process_group_name
        if self.do_qcd:
            postfix += "__qcd"
        if self.do_ff:
            postfix += "__ff"
        if self.do_sideband:
            postfix += "__sideband"
        if self.hide_data:
            postfix += "__nodata"
        elif self.blinded and key not in ("root", "yields"):
            postfix += "__blinded"
        if self.stack and key not in ("root", "yields"):
            postfix += "__stack"
        if self.include_fit:
            postfix += "__withfit"
        if self.log_y and key not in ("root", "yields"):
            postfix += "__logY"
        if self.log_x and key not in ("root", "yields"):
            postfix += "__logX"
        if self.normalize_signals and key not in ("root", "yields"):
            postfix += "__norm_sig"
        if self.equal_bin_width:
            postfix += "__equalBinWidth"
        return postfix

    def output(self):
        """
        Output files to be filled: pdf, png, root or json
        """
        # output definitions, i.e. key, file prefix, extension
        output_data = []
        if self.save_pdf:
            output_data.append(("pdf", "", "pdf"))
        if self.save_png:
            output_data.append(("png", "", "png"))
        if self.save_root:
            output_data.append(("root", "root/", "root"))
        if self.save_yields:
            output_data.append(("yields", "yields/", "json"))
        return {
            key: law.SiblingFileCollection(OrderedDict(
                (feature.name, self.local_target("{}{}{}.{}".format(
                    prefix, feature.name, self.get_output_postfix(key), ext)))
                for feature in self.features
            ))
            for key, prefix, ext in output_data
        }

    def complete(self):
        """
        Task is completed when all output are present
        """
        return ConfigTaskWithCategory.complete(self)


    def setup_signal_hist(self, hist, color):
        """
        Method to apply signal format to an histogram
        """
        ROOT = import_root()
        hist.hist_type = "signal"
        hist.legend_style = "l"
        hist.SetLineColor(color)
        hist.SetLineWidth(2)

    def setup_background_hist(self, hist, color):
        """
        Method to apply background format to an histogram
        """
        ROOT = import_root()
        hist.hist_type = "background"
        if self.stack:
            hist.SetLineColor(ROOT.kBlack)
            hist.SetLineWidth(1)
            hist.SetFillColor(color)
            hist.legend_style = "f"
        else:
            hist.SetLineColor(color)
            hist.legend_style = "l"
            hist.SetLineWidth(2)

    def setup_data_hist(self, hist, color):
        """
        Method to apply data format to an histogram
        """
        ROOT = import_root()
        hist.legend_style = "lp"
        hist.hist_type = "data"
        hist.SetMarkerStyle(20)
        hist.SetMarkerColor(color)
        hist.SetLineColor(color)
        hist.SetBinErrorOption((ROOT.TH1.kPoisson if self.stack else ROOT.TH1.kNormal))

    def get_norm_systematics(self):
        return self.config.get_norm_systematics(self.processes_datasets, self.region)

    def get_labels_to_draw(self, signal_hists, data_hists, bkg_histo, draw_hists, maximum, label_scaling):

        print("Running get_labels_to_draw from FeaturePlot")

        # get text to plot inside the figure
        inner_text = self.config.get_inner_text_for_plotting(self.category, self.region)

        if self.normalize_signals and self.stack and signal_hists and bkg_histo:
            scale_text = []
            for hist in signal_hists:
                scale = hist.cmt_scale
                if scale != 1.:
                    if scale < 100:
                        scale = "{:.1f}".format(scale)
                    elif scale < 10000:
                        scale = "{}".format(int(round(scale)))
                    else:
                        scale = "{:.2e}".format(scale).replace("+0", "").replace("+", "")
                    scale_text.append("{} x{}".format(hist.process_label, scale))
            inner_text.append("#scale[0.75]{{{}}}".format(",  ".join(scale_text)))

        if (maximum > 1e4 or maximum < 1e-4) and not self.log_y:
            upper_left_offset = 0.05
        else:
            upper_left_offset = 0.0

        if not (self.hide_data or not len(data_hists) > 0) or self.stack:
            if not type(self.config.lumi_fb) == dict:
                upper_right="{}, {} ".format(
                    self.config.year,
                    get_lumi_string(self.config.lumi_fb),
                ) + "({} TeV)".format(self.config.ecm)
            else:
                if self.run_era != "":
                    era_label = " " + self.run_era
                    lumi_label = self.config.lumi_fb[self.config.get_run_period_from_run_era(self.run_era)][self.run_era]
                else:
                    era_label = self.run_period
                    if self.run_period != "":
                        era_label = " " + era_label
                        lumi_label = sum(self.config.lumi_fb.get(self.run_period, {}).values())
                    else:
                        lumi_label = sum(sum(period_dict.values()) for period_dict in self.config.lumi_fb.values())
                upper_right="{}{}, {} ".format(
                    self.config.year,
                    era_label,
                    get_lumi_string(lumi_label),
                ) + "({} TeV)".format(self.config.ecm)
        else:
            upper_right="{} Simulation ({} TeV)".format(
                self.config.year,
                self.config.ecm,
            )

        try:
            m = max([hist.GetMaximum() for hist in draw_hists]) if not self.log_y else None
        except AttributeError:  # TEfficiency doesn't have GetMaximum()
            m = None

        return get_labels(
            upper_left=self.config.upper_left_text,
            upper_left_offset=upper_left_offset,
            upper_right=upper_right,
            scaling=label_scaling,
            inner_text=inner_text,
            max=m
        )

    def plot(self, feature, ifeat=0, compute_qcd=None):
        """
        Performs the actual plotting.
        """
        ROOT = import_root()
        import plotlib.root as r
        from plotting_tools.root import Canvas, RatioCanvas

        if compute_qcd == None:
            compute_qcd = self.do_qcd

        # helper to extract the qcd shape in a region
        # same helper can be used to get the fake factors shape template
        def get_qcd(region, files, syst='', bin_limit=0.):
            d_hist = files[region].Get("histograms/" + self.data_names[0])
            if not d_hist:
                raise Exception("data histogram '{}' not found for region '{}' in tfile {}".format(
                    self.data_names[0], region, files[region]))

            if self.optimization_method == "flat_sgn":
                d_hist = self.histogram_bin_merger.rebin(d_hist, inplace=True, equal_bin_width=self.equal_bin_width)

            b_hists = []
            for b_name in self.background_names:
                b_hist = files[region].Get("histograms/" + b_name + syst)
                if not b_hist:
                    raise Exception("background histogram '{}' not found in region '{}'".format(
                        b_name, region))
                if self.optimization_method == "flat_sgn":
                    b_hist = self.histogram_bin_merger.rebin(b_hist, inplace=True, equal_bin_width=self.equal_bin_width)
                b_hists.append(b_hist)

            qcd_hist = d_hist.Clone(randomize("qcd_" + region + syst))
            for hist in b_hists:
                qcd_hist.Add(hist, -1.)

            # removing negative bins
            for ibin in range(1, qcd_hist.GetNbinsX() + 1):
                if qcd_hist.GetBinContent(ibin) < bin_limit:
                    qcd_hist.SetBinContent(ibin, 1.e-6)

            return qcd_hist

        # helper to extract data and bkg histos in sideband and signal regions
        def get_sideband(region, files, bin_limit=0.):
            d_hist = files[region].Get("histograms/" + self.data_names[0]).Clone(
                randomize("bkg_" + region))
            if not d_hist:
                raise Exception("data histogram '{}' not found for region '{}' in tfile {}".format(
                    self.data_names[0], region, files[region]))

            b_hist = None
            for b_name in self.background_names:
                if not b_hist:
                    b_hist = files[region].Get("histograms/" + b_name).Clone(
                        randomize("bkg_" + region))
                else:
                    b_hist.Add(files[region].Get("histograms/" + b_name))

            return d_hist, b_hist

        def get_integral_and_error(hist):
            error = c_double(0.)
            integral = hist.IntegralAndError(0, hist.GetNbinsX() + 1, error)
            error = error.value
            compatible = True if integral <= 0 else False
            # compatible = True if integral - error <= 0 else False
            return integral, error, compatible

        def get_ratio(num, den):
            if den == 0:
                return 0
            return num / den

        # I think these are for the SIGNAL REGION ONLY
        background_hists = self.histos["background"]
        signal_hists = self.histos["signal"]
        data_hists = self.histos["data"]
        all_hists = self.histos["all"]
        if self.plot_systematics:
            bkg_histo_syst = self.histos["bkg_histo_syst"]

        # qcd shape files
        qcd_shape_files = None
        if compute_qcd:
            qcd_shape_files = {}
            for key, region in self.qcd_regions.items():
                if self.qcd_category_name != "default":
                    my_feature = (feature.name
                        if "shape" in key or \
                            key == f"{self.config.qcd_var1.nominal}_{self.config.qcd_var2.inverted}"
                        else self.qcd_feature)
                else:
                    my_feature = feature.name
                qcd_shape_files[key] = ROOT.TFile.Open(self.input()["qcd"][key]["root"].targets[my_feature].path)

            # do the qcd extrapolation
            if "shape" in qcd_shape_files:
                qcd_hist = get_qcd("shape", qcd_shape_files).Clone(randomize("qcd"))
                n_qcd_hist, n_qcd_hist_error, n_qcd_hist_compatible = get_integral_and_error(qcd_hist)
                if not n_qcd_hist_compatible:
                    qcd_hist.Scale(1. / n_qcd_hist)
            else: #sym shape
                qcd_hist = get_qcd("shape1", qcd_shape_files).Clone(randomize("qcd"))
                qcd_hist2 = get_qcd("shape2", qcd_shape_files).Clone(randomize("qcd"))
                n_qcd_hist1, n_qcd_hist1_error, n_qcd_hist1_compatible = get_integral_and_error(qcd_hist)
                n_qcd_hist2, n_qcd_hist2_error, n_qcd_hist2_compatible = get_integral_and_error(qcd_hist2)
                qcd_hist.Scale(1. / n_qcd_hist1)
                qcd_hist2.Scale(1. / n_qcd_hist2)
                qcd_hist.Add(qcd_hist2)
                qcd_hist.Scale(0.5)

            n_os_inviso, n_os_inviso_error, n_os_inviso_compatible = get_integral_and_error(
                get_qcd(f"{self.config.qcd_var1.nominal}_{self.config.qcd_var2.inverted}",
                qcd_shape_files, bin_limit=-999)) # C
            n_ss_iso, n_ss_iso_error, n_ss_iso_compatible = get_integral_and_error(
                get_qcd(f"{self.config.qcd_var1.inverted}_{self.config.qcd_var2.nominal}",
                qcd_shape_files, bin_limit=-999)) # B
            n_ss_inviso, n_ss_inviso_error, n_ss_inviso_compatible = get_integral_and_error(
                get_qcd(f"{self.config.qcd_var1.inverted}_{self.config.qcd_var2.inverted}",
                qcd_shape_files, bin_limit=-999)) # D
            # if not n_ss_iso or not n_ss_inviso:
            if n_os_inviso_compatible or n_ss_iso_compatible or n_ss_inviso_compatible:
                print("****WARNING: QCD normalization failed (negative yield), Removing QCD!")
                qcd_scaling = 0.
                # qcd_inviso = 0.
                # qcd_inviso_error = 0.
                qcd_hist.Scale(qcd_scaling)
            else:
                # qcd_inviso = n_os_inviso / n_ss_inviso # C/D
                # qcd_inviso_error = qcd_inviso * math.sqrt(
                #     (n_os_inviso_error / n_os_inviso) * (n_os_inviso_error / n_os_inviso)
                #     + (n_ss_inviso_error / n_ss_inviso) * (n_ss_inviso_error / n_ss_inviso)
                #     # + 4 * (n_ss_inviso_error / n_ss_inviso) * (n_ss_inviso_error / n_ss_inviso)
                # )
                qcd_scaling = n_os_inviso * n_ss_iso / n_ss_inviso # C*B/D
                os_inviso_rel_error = n_os_inviso_error / n_os_inviso
                ss_iso_rel_error = n_ss_iso_error / n_ss_iso
                ss_inviso_rel_error = n_ss_inviso_error / n_ss_inviso
                new_errors_sq = []
                for ib in range(1, qcd_hist.GetNbinsX() + 1):
                    if qcd_hist.GetBinContent(ib) > 0:
                        bin_rel_error = qcd_hist.GetBinError(ib) / qcd_hist.GetBinContent(ib)
                    else:
                        bin_rel_error = 0
                    new_errors_sq.append(bin_rel_error * bin_rel_error
                        + os_inviso_rel_error * os_inviso_rel_error
                        + ss_iso_rel_error * ss_iso_rel_error
                        + ss_inviso_rel_error * ss_inviso_rel_error)
                qcd_hist.Scale(qcd_scaling)
                # fix errors
                for ib in range(1, qcd_hist.GetNbinsX() + 1):
                    qcd_hist.SetBinError(ib, qcd_hist.GetBinContent(ib)
                        * math.sqrt(new_errors_sq[ib - 1]))

            # store and style
            yield_error = c_double(0.)
            qcd_hist.cmt_yield = qcd_hist.IntegralAndError(
                0, qcd_hist.GetNbinsX() + 1, yield_error)
            qcd_hist.cmt_yield_error = yield_error.value
            qcd_hist.cmt_bin_yield = []
            qcd_hist.cmt_bin_yield_error = []
            for ibin in range(1, qcd_hist.GetNbinsX() + 1):
                qcd_hist.cmt_bin_yield.append(qcd_hist.GetBinContent(ibin))
                qcd_hist.cmt_bin_yield_error.append(qcd_hist.GetBinError(ibin))
            qcd_hist.cmt_scale = 1.
            qcd_hist.cmt_process_name = "qcd"
            qcd_hist.process_label = "QCD"
            qcd_hist.SetTitle("QCD")
            qcd_c = tuple([255, 87, 215])
            qcd_color = ROOT.TColor.GetColor(*qcd_c)
            self.setup_background_hist(qcd_hist, qcd_color)
            background_hists.append(qcd_hist)
            all_hists.append(qcd_hist)

            if self.propagate_syst_qcd:
                print("****INFO: Propagating shape uncertainties to QCD")
                for syst_dir in self.histos["shape"].keys():
                    syst_dir_name = "_{}".format(syst_dir)

                    # do the qcd extrapolation
                    if "shape" in qcd_shape_files:
                        qcd_hist = get_qcd("shape", qcd_shape_files, syst=syst_dir_name).Clone(randomize("qcd"))
                        n_qcd_hist, n_qcd_hist_error, n_qcd_hist_compatible = get_integral_and_error(qcd_hist)
                        if not n_qcd_hist_compatible:
                            qcd_hist.Scale(1. / n_qcd_hist)
                    else: #sym shape
                        qcd_hist = get_qcd("shape1", qcd_shape_files, syst=syst_dir_name).Clone(randomize("qcd"))
                        qcd_hist2 = get_qcd("shape2", qcd_shape_files, syst=syst_dir_name).Clone(randomize("qcd"))
                        n_qcd_hist1, n_qcd_hist1_error, n_qcd_hist1_compatible = get_integral_and_error(qcd_hist)
                        n_qcd_hist2, n_qcd_hist2_error, n_qcd_hist2_compatible = get_integral_and_error(qcd_hist2)
                        qcd_hist.Scale(1. / n_qcd_hist1)
                        qcd_hist2.Scale(1. / n_qcd_hist2)
                        qcd_hist.Add(qcd_hist2)
                        qcd_hist.Scale(0.5)

                    n_os_inviso, n_os_inviso_error, n_os_inviso_compatible = get_integral_and_error(
                        get_qcd(f"{self.config.qcd_var1.nominal}_{self.config.qcd_var2.inverted}",
                        qcd_shape_files, syst=syst_dir_name, bin_limit=-999)) # C
                    n_ss_iso, n_ss_iso_error, n_ss_iso_compatible = get_integral_and_error(
                        get_qcd(f"{self.config.qcd_var1.inverted}_{self.config.qcd_var2.nominal}",
                        qcd_shape_files, syst=syst_dir_name, bin_limit=-999)) # B
                    n_ss_inviso, n_ss_inviso_error, n_ss_inviso_compatible = get_integral_and_error(
                        get_qcd(f"{self.config.qcd_var1.inverted}_{self.config.qcd_var2.inverted}",
                        qcd_shape_files, syst=syst_dir_name, bin_limit=-999)) # D
                    # if not n_ss_iso or not n_ss_inviso:
                    if n_os_inviso_compatible or n_ss_iso_compatible or n_ss_inviso_compatible:
                        print("****WARNING: QCD normalization failed (negative yield), Removing QCD!")
                        qcd_scaling = 0.
                        qcd_hist.Scale(qcd_scaling)
                    else:
                        qcd_scaling = n_os_inviso * n_ss_iso / n_ss_inviso # C*B/D
                        os_inviso_rel_error = n_os_inviso_error / n_os_inviso
                        ss_iso_rel_error = n_ss_iso_error / n_ss_iso
                        ss_inviso_rel_error = n_ss_inviso_error / n_ss_inviso
                        new_errors_sq = []
                        for ib in range(1, qcd_hist.GetNbinsX() + 1):
                            if qcd_hist.GetBinContent(ib) > 0:
                                bin_rel_error = qcd_hist.GetBinError(ib) / qcd_hist.GetBinContent(ib)
                            else:
                                bin_rel_error = 0
                            new_errors_sq.append(bin_rel_error * bin_rel_error
                                + os_inviso_rel_error * os_inviso_rel_error
                                + ss_iso_rel_error * ss_iso_rel_error
                                + ss_inviso_rel_error * ss_inviso_rel_error)
                        qcd_hist.Scale(qcd_scaling)
                        # fix errors
                        for ib in range(1, qcd_hist.GetNbinsX() + 1):
                            qcd_hist.SetBinError(ib, qcd_hist.GetBinContent(ib)
                                * math.sqrt(new_errors_sq[ib - 1]))

                    qcd_hist.cmt_process_name = "qcd"
                    self.histos["shape"][syst_dir].append(qcd_hist)

        if self.do_ff:
            ff_shape_files = {}
            for key, region in self.ff_regions.items():
                ff_shape_files[key] = ROOT.TFile.Open(self.input()["ff"][key]["root"].targets[feature.name].path)

            ff_hist = get_qcd(self.ff_shape_region, ff_shape_files).Clone(randomize("fakes"))

            # store and style
            yield_error = c_double(0.)
            ff_hist.cmt_yield = ff_hist.IntegralAndError(
                0, ff_hist.GetNbinsX() + 1, yield_error)
            ff_hist.cmt_yield_error = yield_error.value
            ff_hist.cmt_bin_yield = []
            ff_hist.cmt_bin_yield_error = []
            for ibin in range(1, ff_hist.GetNbinsX() + 1):
                ff_hist.cmt_bin_yield.append(ff_hist.GetBinContent(ibin))
                ff_hist.cmt_bin_yield_error.append(ff_hist.GetBinError(ibin))
            ff_hist.cmt_scale = 1.
            ff_hist.cmt_process_name = "fakes"
            ff_hist.process_label = "Fakes"
            ff_hist.SetTitle("Fakes")
            ff_c = tuple([255, 87, 215])
            ff_color = ROOT.TColor.GetColor(*ff_c)
            self.setup_background_hist(ff_hist, ff_color)
            background_hists.append(ff_hist)
            all_hists.append(ff_hist)

        # sideband files
        sideband_files = None
        if self.do_sideband:
            sideband_files = {}
            for key, region in self.sideband_regions.items():
                my_feature = feature.name
                sideband_files[key] = ROOT.TFile.Open(
                    self.input()["sideband"][key]["root"].targets[my_feature].path)

            # do the qcd extrapolation
            data_hist, bkg_hist_background_region = get_sideband("background", sideband_files)
            _, bkg_hist_signal_region = get_sideband("signal", sideband_files)
            n_bkg_background, n_bkg_background_error, _ = get_integral_and_error(
                bkg_hist_background_region)
            n_bkg_signal, n_bkg_signal_error, _ = get_integral_and_error(
                bkg_hist_signal_region)

            bkg_hist = data_hist.Clone(randomize("bkg"))
            n_bkg_background_rel_error = get_ratio(n_bkg_background_error, n_bkg_background)
            n_bkg_signal_rel_error = get_ratio(n_bkg_signal_error, n_bkg_signal)
            new_errors_sq = []
            for ib in range(1, bkg_hist.GetNbinsX() + 1):
                if bkg_hist.GetBinContent(ib) > 0:
                    bin_rel_error = bkg_hist.GetBinError(ib) / bkg_hist.GetBinContent(ib)
                else:
                    bin_rel_error = 0
                new_errors_sq.append(bin_rel_error * bin_rel_error
                    + n_bkg_background_rel_error * n_bkg_background_rel_error
                    + n_bkg_signal_rel_error * n_bkg_signal_rel_error)
            bkg_hist.Scale(n_bkg_signal / n_bkg_background)
            # fix errors
            for ib in range(1, bkg_hist.GetNbinsX() + 1):
                bkg_hist.SetBinError(ib, bkg_hist.GetBinContent(ib)
                    * math.sqrt(new_errors_sq[ib - 1]))

            # store and style
            yield_error = c_double(0.)
            bkg_hist.cmt_yield = bkg_hist.IntegralAndError(
                0, bkg_hist.GetNbinsX() + 1, yield_error)
            bkg_hist.cmt_yield_error = yield_error.value
            bkg_hist.cmt_bin_yield = []
            bkg_hist.cmt_bin_yield_error = []
            for ibin in range(1, bkg_hist.GetNbinsX() + 1):
                bkg_hist.cmt_bin_yield.append(bkg_hist.GetBinContent(ibin))
                bkg_hist.cmt_bin_yield_error.append(bkg_hist.GetBinError(ibin))
            bkg_hist.cmt_scale = 1.
            bkg_hist.cmt_process_name = "background"
            bkg_hist.process_label = "Background"
            bkg_hist.SetTitle("Background")
            try:
                bkg_color = ROOT.TColor.GetColor(*self.config.processes.get("background").color)
            except:  # background process not defined in config
                bkg_color = ROOT.kRed
            self.setup_background_hist(bkg_hist, bkg_color)
            background_hists = [bkg_hist]
            all_hists.append(bkg_hist)

        # Change bins to have equal width
        if self.equal_bin_width:

            # Define transformer
            equal_bin_width_transformer = EqualBinWidthTransformer(self.histos["all"][0])

            # Apply equal bin transformation before plotting
            for idx, hist in enumerate(signal_hists):
                color = hist.GetLineColor()
                sig_hist = equal_bin_width_transformer.convert(hist)
                self.setup_signal_hist(sig_hist, color)
                signal_hists[idx] = sig_hist
            for idx, hist in enumerate(background_hists):
                color = hist.GetFillColor()
                bkg_hist = equal_bin_width_transformer.convert(hist)
                self.setup_background_hist(bkg_hist, color)
                background_hists[idx] = bkg_hist
            for idx, hist in enumerate(data_hists):
                color = hist.GetLineColor()
                dat_hist = equal_bin_width_transformer.convert(hist)
                self.setup_data_hist(dat_hist, color)
                data_hists[idx] = dat_hist
            for idx, hist in enumerate(all_hists):
                if hist.hist_type == 'background':
                    color = hist.GetFillColor()
                    all_hist = equal_bin_width_transformer.convert(hist)
                    self.setup_background_hist(all_hist, color)
                    all_hists[idx] = all_hist
                else:
                    all_hists[idx] = equal_bin_width_transformer.convert(hist)
            if self.store_systematics:
                for shape in self.histos["shape"]:
                    for idx, hist in enumerate(self.histos["shape"][shape]):
                        self.histos["shape"][shape][idx] = equal_bin_width_transformer.convert(hist)
            if self.plot_systematics:
                self.histos["bkg_histo_syst"] = equal_bin_width_transformer.convert(self.histos["bkg_histo_syst"])

        if not self.hide_data:
            all_hists += data_hists

        assert len(all_hists) > 0, "No histograms were included. If you aim to plot just data, "\
            "remember to use --hide-data False"

        bkg_histo = None
        data_histo = None
        if not self.stack:
            for hist in all_hists:
                scale = 1. / (hist.Integral() or 1.)
                hist.Scale(scale)
                hist.scale = scale
            draw_hists = all_hists[::-1]
            for hist in background_hists[::-1]:
                if not bkg_histo:
                    bkg_histo = hist.Clone()
                else:
                    bkg_histo.Add(hist.Clone())
            for hist in data_hists:
                if not data_histo:
                    data_histo = hist.Clone()
                else:
                    data_histo.Add(hist.Clone())
            if bkg_histo:
                scale = 1. / (bkg_histo.Integral() or 1.)
                bkg_histo.Scale(scale)
                if self.plot_systematics:
                    scale = 1. / (bkg_histo_syst.Integral() or 1.)
                    bkg_histo_syst.Scale(scale)
            if data_histo:
                scale = 1. / (data_histo.Integral() or 1.)
                data_histo.Scale(scale)
        else:
            background_stack = ROOT.THStack(randomize("stack"), "")
            for hist in background_hists[::-1]:
                # hist.SetFillColor(ROOT.kRed)
                background_stack.Add(hist)
                if not bkg_histo:
                    bkg_histo = hist.Clone()
                else:
                    bkg_histo.Add(hist.Clone())
            background_stack.hist_type = "background"

            # Normalize signal histograms to background sum
            if self.normalize_signals and bkg_histo:
                for hist in signal_hists:
                    signal_yield = hist.cmt_yield
                    scale = (bkg_histo.Integral() / signal_yield if signal_yield != 0 else 1.)
                    hist.Scale(scale)
                    hist.cmt_scale = scale

            draw_hists = [background_stack] + signal_hists[::-1]
            if not self.hide_data:
                # blinding
                blinded_range = feature.get_aux("blinded_range", None)
                if blinded_range and self.blinded:
                    for hist in data_hists:
                        for ib in range(1, hist.GetNbinsX() + 1):
                            blind = False
                            if isinstance(blinded_range[0], list):
                                for iblinded_range in blinded_range:
                                    if (hist.GetBinCenter(ib) >= iblinded_range[0] and
                                            hist.GetBinCenter(ib) <= iblinded_range[1]):
                                        blind = True
                                        break
                            else:
                                if (hist.GetBinCenter(ib) >= blinded_range[0] and
                                        hist.GetBinCenter(ib) <= blinded_range[1]):
                                    blind = True

                            if blind:
                                hist.SetBinContent(ib, -999)
                                hist.SetBinError(ib, 0)

                draw_hists.extend(data_hists[::-1])
                for hist in data_hists:
                    if not data_histo:
                        data_histo = hist.Clone()
                    else:
                        data_histo.Add(hist.Clone())

        # Create a dummy histogram for plotting axes and stuff
        dummy_hist = all_hists[0].Clone(randomize("dummy"))
        # Clear away all the content and initial plotting options
        dummy_hist.Reset("M")
        binning_args, y_axis_adendum = self.get_binning(feature, ifeat)
        x_title = (str(feature.get_aux("x_title"))
            + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
        if not getattr(self, "isEfficiency", False):
            y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
        else:
            y_title = "Efficiency"
        hist_title = "; %s; %s" % (x_title, y_title)
        dummy_hist.SetTitle(hist_title)

        # Draw
        self.show_ratio = self.show_ratio and not (self.hide_data or len(data_hists) == 0
            or len(background_hists) == 0)
        if not self.show_ratio:
            c = Canvas()
            if self.log_y:
                c.SetLogy()
            if self.log_x:
                c.SetLogx()
            label_scaling = 1
        else:
            c = RatioCanvas()
            c.get_pad(1).cd()
            if self.log_y:
                c.get_pad(1).SetLogy()
            if self.log_x:
                c.get_pad(1).SetLogx()
            label_scaling = self.config.label_size

        r.setup_hist(dummy_hist)

        # In case there is a ratio plot: labels are changed on ratio plot later.
        # In case no ratio plot: labels are updated here on the dummy hist.
        if self.show_ratio:
            r.setup_y_axis(dummy_hist.GetYaxis(), pad=c.get_pad(1))
        else:
            if self.equal_bin_width:
                equal_bin_width_transformer.convert_labels(dummy_hist, show_ratio=self.show_ratio)
                ROOT.gPad.SetBottomMargin(0.13)

        if "TEfficiency" not in str(type(dummy_hist)):
            dummy_hist.GetYaxis().SetMaxDigits(4)

            if self.max_y == law.NO_FLOAT:
                maximum = max([hist.GetMaximum() for hist in draw_hists])
                dummy_hist.SetMaximum(100 * maximum if self.log_y else 1.35 * maximum)
            else:
                maximum = self.max_y
                dummy_hist.SetMaximum(self.max_y)

            if self.min_y == law.NO_FLOAT:
                dummy_hist.SetMinimum(0.0011 if self.log_y else 0.001)
            else:
                dummy_hist.SetMinimum(self.min_y)
        else:
            maximum = 1.05

        draw_labels = self.get_labels_to_draw(signal_hists, data_hists, bkg_histo, draw_hists,
            maximum, label_scaling)

        dummy_hist.Draw()

        for hist in draw_hists:
            option = "HIST,SAME" if hist.hist_type != "data" else "PE0Z,SAME"
            hist.Draw(option)

        for label in draw_labels:
            label.Draw("same")

        # Define entries object to be used later when filling the legend
        # Can be updated with the fits and the uncertainty bands
        entries = [(hist, hist.process_label, hist.legend_style) for hist in all_hists]

        if self.show_ratio:
            dummy_ratio_hist = dummy_hist.Clone(randomize("dummy"))
            r.setup_hist(dummy_ratio_hist, pad=c.get_pad(2),
                props={"Minimum": self.ratio_min, "Maximum": self.ratio_max})
            r.setup_y_axis(dummy_ratio_hist.GetYaxis(), pad=c.get_pad(2),
                props={"Ndivisions": self.ratio_ndivisions})
            dummy_ratio_hist.GetYaxis().SetTitle("Data / MC")

            data_graph = hist_to_graph(data_histo, remove_zeros=False, errors=True,
                asymm=True, overflow=False, underflow=False,
                attrs=["cmt_process_name", "cmt_hist_type", "cmt_legend_style"])
            bkg_graph = hist_to_graph(bkg_histo, remove_zeros=False, errors=True,
                asymm=True, overflow=False, underflow=False,
                attrs=["cmt_process_name", "cmt_hist_type", "cmt_legend_style"])

            ratio_graph = ROOT.TGraphAsymmErrors(binning_args[0])
            mc_unc_graph = ROOT.TGraphErrors(binning_args[0])
            setattr(mc_unc_graph, "title", "MC stat.")
            r.setup_graph(ratio_graph, props={"MarkerStyle": 20, "MarkerSize": 0.5})
            if self.plot_systematics:
                r.setup_graph(mc_unc_graph, props={"FillStyle": 3017, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kBlue + 2})
                entries.append((mc_unc_graph, mc_unc_graph.title, "f"))
                syst_graph = hist_to_graph(bkg_histo_syst, remove_zeros=False, errors=True,
                    asymm=True, overflow=False, underflow=False,
                    attrs=["cmt_process_name", "cmt_hist_type", "cmt_legend_style"])
                syst_unc_graph = ROOT.TGraphErrors(binning_args[0])
                setattr(syst_unc_graph, "title", "Norm. syst.")
                r.setup_graph(syst_unc_graph, props={"FillStyle": 3005, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kRed + 2})
                all_unc_graph = ROOT.TGraphErrors(binning_args[0])
                setattr(all_unc_graph, "title", "MC Stat. + Norm. Syst.")
                entries.append((all_unc_graph, all_unc_graph.title, "f"))
                r.setup_graph(all_unc_graph, props={"FillStyle": 3001, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kGray + 2})
            else:
                r.setup_graph(mc_unc_graph, props={"FillStyle": 3001, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kGray + 2})
                entries.append((mc_unc_graph, mc_unc_graph.title, "f"))

            for i in range(binning_args[0]):
                x, d, b = c_double(0.), c_double(0.), c_double(0.)
                data_graph.GetPoint(i, x, d)
                bkg_graph.GetPoint(i, x, b)
                d = d.value
                b = b.value
                if d == EMPTY or b == 0:
                    ratio_graph.SetPoint(i, x, EMPTY)
                else:
                    ratio_graph.SetPoint(i, x, d / b)
                    ratio_graph.SetPointEYhigh(i, data_graph.GetErrorYhigh(i) / b)
                    ratio_graph.SetPointEYlow(i, data_graph.GetErrorYlow(i) / b)
                # set the mc uncertainty
                if b == 0:
                    mc_unc_graph.SetPoint(i, x, EMPTY)
                else:
                    mc_unc_graph.SetPoint(i, x, 1.)
                    mc_error = bkg_graph.GetErrorYhigh(i) / b
                    mc_unc_graph.SetPointError(i, dummy_ratio_hist.GetBinWidth(i + 1) / 2.,
                        mc_error)
                    if self.plot_systematics:
                    # syst only
                        syst_unc_graph.SetPoint(i, x, 1.)
                        syst_error = syst_graph.GetErrorYhigh(i) / b
                        syst_unc_graph.SetPointError(i, dummy_ratio_hist.GetBinWidth(i + 1) / 2.,
                            syst_error)
                        # syst + stat
                        all_unc_graph.SetPoint(i, x, 1.)
                        tot_unc = math.sqrt(mc_error ** 2 + syst_error ** 2)
                        all_unc_graph.SetPointError(i, dummy_ratio_hist.GetBinWidth(i + 1) / 2.,
                            tot_unc)

            c.get_pad(2).cd()
            c.get_pad(2).SetGridy()
            if self.equal_bin_width:
                equal_bin_width_transformer.convert_labels(dummy_ratio_hist, show_ratio=self.show_ratio)
                c.get_pad(2).SetBottomMargin(0.45)
            dummy_ratio_hist.Draw()
            if not self.hide_data:
                # Draw options (from TGraphPainter)
                # P -> plot marker
                # 0 -> draw error bars even when point is outside range
                # Z -> do not draw small horizontal and vertical lines the end of the error bars
                ratio_graph.Draw("P0Z,SAME")
            if self.plot_systematics:
                all_unc_graph.Draw("2,SAME")
            mc_unc_graph.Draw("2,SAME")

            lines = []
            for y in [0.5, 1.0, 1.5]:
                if isinstance(feature.binning, tuple):
                    l = ROOT.TLine(binning_args[1], y, binning_args[2], y)
                else:
                    l = ROOT.TLine(binning_args[1][0], y, binning_args[1][-1], y)
                r.setup_line(l, props={"NDC": False, "LineStyle": 3, "LineWidth": 1,
                    "LineColor": 1})
                lines.append(l)
            for line in lines:
                line.Draw("same")

        if self.show_ratio:
            c.get_pad(1).cd()

        # Draw fit if required
        fits = []
        if self.include_fit:
            with open(self.input()["fit"][feature.name]["json"].path) as f:
                d = json.load(f)
            d = d[""]

            fit_task = self.requires()["fit"]
            x_range = fit_task.x_range
            x = ROOT.RooRealVar("x", "x", float(x_range[0]), float(x_range[1]))
            l = ROOT.RooArgList(x)
            xframe = x.frame()
            process_name = fit_task.process_name
            if self.stack:
                for hist in all_hists:
                    if hist.cmt_process_name == process_name:
                        data = ROOT.RooDataHist("data_obs", "data_obs", l, hist)
                        data.plotOn(xframe,
                            ROOT.RooFit.MarkerColor(ROOT.TColor.GetColorTransparent(0, 0.0)),
                            ROOT.RooFit.LineColor(ROOT.TColor.GetColorTransparent(0, 0.0)))

            colors = [2, 3, 4, 6]
            if fit_task.method != "envelope":
                fit, _ = self.get_fit(fit_task.method, d, x)
                fit.plotOn(xframe, ROOT.RooFit.LineColor(colors[0]), ROOT.RooFit.Name(f"{fit_task.method} fit"))
                entries.append((f"{fit_task.method} fit", f"{fit_task.method} fit", "l"))
            else:
                for im, method in enumerate(fit_task.functions):
                    fit, _ = self.get_fit(method.strip(), d, x)
                    fits.append(fit)
                    name = fit_task.functions[im].strip() + " fit"
                    color = colors[im]
                    fits[-1].plotOn(xframe, ROOT.RooFit.LineColor(color), ROOT.RooFit.Name(name))
                    entries.append((name, name, "l"))
            xframe.Draw("same")

        n_entries = len(entries)
        if n_entries <= 4:
            n_cols = 1
            col_width = getattr(self.config, "single_column_width", 0.2)
        elif n_entries <= 8:
            n_cols = 2
            col_width = getattr(self.config, "double_column_width", 0.15)
        else:
            n_cols = 3
            col_width = 0.1
        n_rows = math.ceil(n_entries / float(n_cols))
        row_width = 0.06
        legend_x2 = 0.88
        legend_x1 = legend_x2 - n_cols * col_width
        legend_y2 = 0.88
        legend_y1 = legend_y2 - n_rows * row_width

        legend = ROOT.TLegend(legend_x1, legend_y1, legend_x2, legend_y2)
        legend.SetBorderSize(0)
        legend.SetNColumns(n_cols)
        for entry in entries:
            legend.AddEntry(*entry)
        legend.Draw("same")

        # Make sure tick mark are not hidden behind
        # the other objects plotted with "SAME"
        ROOT.gPad.RedrawAxis()

        outputs = []
        if self.save_png:
            outputs.append(self.output()["png"].targets[feature.name].path)
        if self.save_pdf:
            outputs.append(self.output()["pdf"].targets[feature.name].path)
        for output in outputs:
            c.SaveAs(create_file_dir(output))

        if self.save_root:
            f = ROOT.TFile.Open(create_file_dir(
                self.output()["root"].targets[feature.name].path), "RECREATE")
            f.cd()
            # c.Write("canvas") #FIXME

            data_already_stored=False
            hist_dir = f.mkdir("histograms")
            hist_dir.cd()
            for hist in all_hists:
                hist.Write(hist.cmt_process_name)

                if not data_already_stored and "data" in hist.cmt_process_name:
                    data_already_stored=True

            # force saving of the data histograms also when not displayed in FeaturePlot
            # this gives the possibility to blind the plots from CreateDatacards
            if not data_already_stored:
                for hist in data_hists:
                    hist.Write(hist.cmt_process_name)

            if bkg_histo:
                bkg_histo.Write("background")

            if self.store_systematics:
                for syst_dir, shape_hists in self.histos["shape"].items():
                    for hist in shape_hists:
                        hist.Write("%s_%s" % (hist.cmt_process_name, syst_dir))
            f.Close()

        if self.save_yields:
            yields = OrderedDict()
            for hist in signal_hists + background_hists + data_hists:
                yields[hist.cmt_process_name] = {
                    "Total yield": hist.cmt_yield,
                    "Total yield error": hist.cmt_yield_error,
                    "Entries": getattr(hist, "cmt_entries", hist.GetEntries()),
                    "Binned yield": hist.cmt_bin_yield,
                    "Binned yield error": hist.cmt_bin_yield_error,
                }
            if bkg_histo:
                yields["background"] = {
                    "Total yield": sum([hist.cmt_yield for hist in background_hists]),
                    "Total yield error": math.sqrt(sum([(hist.cmt_yield_error)**2
                        for hist in background_hists])),
                    "Entries": getattr(bkg_histo, "cmt_entries", bkg_histo.GetEntries()),
                    "Binned yield": [sum([hist.cmt_bin_yield[i] for hist in background_hists])
                        for i in range(0, background_hists[0].GetNbinsX())],
                    "Binned yield error": [math.sqrt(sum([(hist.cmt_bin_yield_error[i])**2
                        for hist in background_hists]))
                        for i in range(0, background_hists[0].GetNbinsX())],
                }
            with open(create_file_dir(self.output()["yields"].targets[feature.name].path), "w") as f:
                json.dump(yields, f, indent=4)

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
            if not process.isData and not self.avoid_normalization:
                for dataset in datasets:
                    nevents[dataset.name] = {}
                    nweightedevents[dataset.name] = {}
                    nunweightedevents[dataset.name] = {}
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
        """
        Splits processes into data, signal and background. Creates histograms from each process
        loading them from the input files. Scales the histograms and applies the correct format
        to them.
        """
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData or p.get_aux("isFakeData", False)]
        self.background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal and not p.get_aux("isFakeData", False)]

        if self.plot_systematics:
            systematics = self.get_norm_systematics()

        if self.fixed_colors:
            colors = list(range(2, 2 + len(self.processes_datasets.keys())))

        self.nevents, self.nweightedevents, self.nunweightedevents = self.get_nevents(inputs)

        for ifeat, feature in enumerate(self.features):
            self.histos = {"background": [], "signal": [], "data": [], "all": []}

            binning_args, y_axis_adendum = self.get_binning(feature, ifeat)
            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
            hist_title = "; %s; %s" % (x_title, y_title)

            systs_directions = [("central", "")]
            if self.plot_systematics:
                self.histos["bkg_histo_syst"] = ROOT.TH1D(
                    randomize("syst"), hist_title, *binning_args)
            if self.store_systematics:
                self.histos["shape"] = {}
                shape_systematics = self.get_systs(feature, True)
                systs_directions += list(itertools.product(shape_systematics, directions))

            for (syst, d) in systs_directions:
                feature_name = feature.name if syst == "central" else "%s_%s_%s" % (
                    feature.name, syst, d)
                if syst != "central":
                    self.histos["shape"]["%s_%s" % (syst, d)] = []
                for iproc, (process, datasets) in enumerate(self.processes_datasets.items()):
                    if syst != "central" and process.isData:
                        continue
                    if self.do_sideband and not process.isData and not process.isSignal:
                        continue
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
                            if not process.isData and not self.avoid_normalization:
                                elem = ("central" if syst == "central" or syst not in self.norm_syst_list
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
                        if self.plot_systematics and not process.isData and not process.isSignal \
                                and syst == "central":
                            dataset_histo_syst = dataset_histo.Clone()
                            for ibin in range(1, dataset_histo_syst.GetNbinsX() + 1):
                                # some datasets may not have any systematics
                                if dataset.name in systematics:
                                    dataset_histo_syst.SetBinError(ibin,
                                        float(dataset_histo.GetBinContent(ibin))\
                                            * systematics[dataset.name]
                                    )
                            self.histos["bkg_histo_syst"].Add(dataset_histo_syst)

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
                        if self.fixed_colors:
                            color = colors[iproc]
                        elif type(process.color) == tuple:
                            color = ROOT.TColor.GetColor(*process.color)
                        else:
                            color = process.color

                        if process.isSignal:
                            self.setup_signal_hist(process_histo, color)
                            self.histos["signal"].append(process_histo)
                        elif process.isData or process.get_aux("isFakeData", False):
                            self.setup_data_hist(process_histo, color)
                            self.histos["data"].append(process_histo)
                        else:
                            self.setup_background_hist(process_histo, color)
                            self.histos["background"].append(process_histo)
                        if not (process.isData or process.get_aux("isFakeData", False)): #or not self.hide_data:
                           self.histos["all"].append(process_histo)
                    else:
                        self.histos["shape"]["%s_%s" % (syst, d)].append(process_histo)

            if self.optimization_method == "flat_sgn" and feature.name in self.features_to_flatten:
                self.histogram_bin_merger = FlatSignalBinMerger(bins_txt_path=self.input()["bin_opt"]["txt"][feature.name].path)

                for idx, hist in enumerate(self.histos["signal"]):
                    self.histos["signal"][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)
                for idx, hist in enumerate(self.histos["background"]):
                    self.histos["background"][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)
                for idx, hist in enumerate(self.histos["data"]):
                    self.histos["data"][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)
                for idx, hist in enumerate(self.histos["all"]):
                    self.histos["all"][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)
                if self.store_systematics:
                    for shape in self.histos["shape"]:
                        for idx, hist in enumerate(self.histos["shape"][shape]):
                            self.histos["shape"][shape][idx] = self.histogram_bin_merger.rebin(hist, inplace=True)
                if self.plot_systematics:
                    self.histos["bkg_histo_syst"] = self.histogram_bin_merger.rebin(self.histos["bkg_histo_syst"], inplace=True)

            self.plot(feature)

#####################################################################################################
#####################################################################################################
#####################################################################################################

class FeaturePlotSyst(FeaturePlot):
    """
    Performs the histogram plotting with up and down variations due to systematics:
    loads the histograms obtained in the FeaturePlot task, rescales them if needed
    and plots and saves them.

    Example command:

    ``law run FeaturePlotSyst --version test --category-name etau --config-name ul_2018 \
--process-group-name etau --feature-names lep1_pt,lep1_eta \
--workers 20 --PrePlot-workflow local --stack --hide-data False --do-qcd --region-name etau_os_iso\
--dataset-names tt_dl,tt_sl,dy_high,wjets,data_etau_a,data_etau_b,data_etau_c,data_etau_d \
--MergePreCounter-version test_old``

    """

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        return FeaturePlot.vreq(self, save_root=True, stack=True)

    def output(self):
        """
        Output files to be filled: pdf or png
        """

        output_data = []
        if self.save_pdf:
            output_data.append(("pdf", "", "pdf"))
        if self.save_png:
            output_data.append(("png", "", "png"))

        process_names = [process.name for process in self.processes_datasets.keys()
                        if not process.isData]
        if self.do_qcd:
            process_names.append("qcd")

        out = {
            key: law.SiblingFileCollection(OrderedDict(
                ("%s_%s_%s" %(process_name, feature.name, syst),
                    self.local_target("{}{}_{}_{}{}.{}".format(
                    prefix, process_name, feature.name, syst, self.get_output_postfix(key), ext)))
                for feature in self.features
                for syst in self.get_unique_systs(self.get_systs(feature, True) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], True))
                for process_name in process_names
            ))
            for key, prefix, ext in output_data
        }

        return out

    def setup_syst_hist(self, hist, dir):
        ROOT = import_root()
        if dir == "central":
            hist.SetFillStyle(0)
            hist.SetLineColor(1)
            hist.legend_style = "l"
            hist.SetLineWidth(2)
        if dir == "up":
            hist.SetFillStyle(0)
            hist.SetLineColor(ROOT.kAzure)
            hist.legend_style = "l"
            hist.SetLineWidth(2)
            # hist.SetLineStyle(4)
        if dir == "down":
            hist.SetFillStyle(0)
            hist.SetLineColor(ROOT.kMagenta)
            hist.legend_style = "l"
            hist.SetLineWidth(2)
            # hist.SetLineStyle(8)

    def get_syst_label(self, shape_syst):
        if self.config.systematics.get(shape_syst).get_aux("label"):
            return self.config.systematics.get(shape_syst).get_aux("label")
        else:
            return shape_syst

    def plot(self, feature):
        ROOT = import_root()
        import plotlib.root as r
        from plotting_tools.root import get_labels, Canvas, RatioCanvas

        for process, p_label in zip(self.process_names, self.process_labels):
            # central histogram for each process
            histo_syst_central = self.histos[process]["central"]
            self.setup_syst_hist(histo_syst_central, "central")

            for shape_syst in self.shape_syst_list:
                # up and down variation histograms for each systematic
                histo_syst_up = self.histos[process]["%s_up" %shape_syst]
                histo_syst_down = self.histos[process]["%s_down" %shape_syst]
                self.setup_syst_hist(histo_syst_up, "up")
                self.setup_syst_hist(histo_syst_down, "down")

                draw_hists = [histo_syst_central, histo_syst_up, histo_syst_down]

                shape_syst_label = self.get_syst_label(shape_syst)
                entries = [ (histo_syst_central, "Nominal", histo_syst_central.legend_style),
                            (histo_syst_up, shape_syst_label + " Up", histo_syst_up.legend_style),
                            (histo_syst_down, shape_syst_label + " Down", histo_syst_down.legend_style)]

                n_entries = len(entries)
                if n_entries <= 4:
                    n_cols = 1
                elif n_entries <= 8:
                    n_cols = 2
                else:
                    n_cols = 3
                if len(shape_syst_label) < 5:
                    col_width = 0.20
                elif len(shape_syst_label) < 12:
                    col_width = 0.30
                else:
                    col_width = 0.40
                n_rows = math.ceil(n_entries / float(n_cols))
                row_width = 0.06
                legend_x2 = 0.88
                legend_x1 = legend_x2 - n_cols * col_width
                legend_y2 = 0.88
                legend_y1 = legend_y2 - n_rows * row_width

                legend = ROOT.TLegend(legend_x1, legend_y1, legend_x2, legend_y2)
                legend.SetBorderSize(0)
                legend.SetNColumns(1)
                for entry in entries:
                    legend.AddEntry(*entry)

                binning_args, y_axis_adendum = self.get_binning(feature)
                x_title = (str(feature.get_aux("x_title"))
                    + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
                y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
                hist_title = "; %s; %s" % (x_title, y_title)
                dummy_hist = ROOT.TH1F(randomize("dummy"), hist_title, *binning_args)

                # Draw
                c = RatioCanvas()
                dummy_hist.GetXaxis().SetLabelOffset(100)
                dummy_hist.GetXaxis().SetTitleOffset(100)
                c.get_pad(1).cd()
                if self.log_y:
                    c.get_pad(1).SetLogy()
                if self.log_x:
                    c.get_pad(1).SetLogx()
                label_scaling = self.config.label_size

                r.setup_hist(dummy_hist)
                r.setup_y_axis(dummy_hist.GetYaxis(), pad=c.get_pad(1))
                dummy_hist.GetYaxis().SetMaxDigits(4)
                dummy_hist.GetYaxis().SetTitleOffset(1.22)
                maximum = max([hist.GetMaximum() for hist in draw_hists])
                if self.log_y:
                    dummy_hist.SetMaximum(100 * maximum)
                    dummy_hist.SetMinimum(0.0011)
                else:
                    dummy_hist.SetMaximum(1.25 * maximum)
                    dummy_hist.SetMinimum(0.001)

                inner_text=[self.category.label + " category"]
                if self.region:
                    if isinstance(self.region.label, list):
                        inner_text += self.region.label
                    else:
                        inner_text.append(self.region.label)
                inner_text.append(p_label)

                if maximum > 1e4 and not self.log_y:
                    upper_left_offset = 0.05
                else:
                    upper_left_offset = 0.0

                upper_right="{} Simulation ({} TeV)".format(
                    self.config.year,
                    self.config.ecm,
                )

                m = max([hist.GetMaximum() for hist in draw_hists]) if not self.log_y else None

                draw_labels = get_labels(
                    upper_left_offset=upper_left_offset,
                    upper_right=upper_right,
                    scaling=label_scaling,
                    inner_text=inner_text,
                    max=m
                )

                dummy_hist.Draw()
                histo_syst_central.Draw("HIST,SAME")
                histo_syst_up.Draw("HIST,SAME")
                histo_syst_down.Draw("HIST,SAME")

                for label in draw_labels:
                    label.Draw("same")
                legend.Draw("same")

                dummy_ratio_hist = ROOT.TH1F(randomize("dummy"), hist_title, *binning_args)
                r.setup_hist(dummy_ratio_hist, pad=c.get_pad(2),
                    props={"Minimum": self.ratio_min, "Maximum": self.ratio_max})
                r.setup_y_axis(dummy_ratio_hist.GetYaxis(), pad=c.get_pad(2),
                    props={"Ndivisions": self.ratio_ndivisions})
                dummy_ratio_hist.GetYaxis().SetTitle("Ratio")
                dummy_ratio_hist.GetXaxis().SetTitleOffset(3)
                dummy_ratio_hist.GetYaxis().SetTitleOffset(1.22)

                ratio_hist_up = ROOT.TH1F(randomize("ratio_hist_up"), hist_title, *binning_args)
                ratio_hist_down = ROOT.TH1F(randomize("ratio_hist_down"), hist_title, *binning_args)
                self.setup_syst_hist(ratio_hist_up, "up")
                self.setup_syst_hist(ratio_hist_down, "down")
                ratio_hist_up.SetLineStyle(0)
                ratio_hist_down.SetLineStyle(0)

                mc_unc_graph = ROOT.TGraphErrors(binning_args[0])
                r.setup_graph(mc_unc_graph, props={"FillStyle": 3001, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kGray + 2})

                for i in range(binning_args[0]+1):
                    x = histo_syst_central.GetBinCenter(i)
                    y = histo_syst_central.GetBinContent(i)
                    sigma_x = histo_syst_central.GetBinWidth(i)/2.
                    sigma_y = histo_syst_central.GetBinError(i)
                    if histo_syst_central.GetBinContent(i) != 0:
                        ratio_hist_up.SetBinContent(i, histo_syst_up.GetBinContent(i)/histo_syst_central.GetBinContent(i))
                        ratio_hist_down.SetBinContent(i, histo_syst_down.GetBinContent(i)/histo_syst_central.GetBinContent(i))
                        mc_unc_graph.SetPoint(i, x, 1.)
                        mc_unc_graph.SetPointError(i, sigma_x, sigma_y/y)
                    else:
                        ratio_hist_up.SetBinContent(i, EMPTY)
                        ratio_hist_down.SetBinContent(i, EMPTY)
                        mc_unc_graph.SetPoint(i, x, EMPTY)

                c.get_pad(2).cd()
                c.get_pad(2).SetGridy()
                dummy_ratio_hist.Draw()
                ratio_hist_up.Draw("SAME")
                ratio_hist_down.Draw("SAME")
                mc_unc_graph.Draw("2,SAME")

                lines = []
                for y in [0.5, 1.0, 1.5]:
                    if isinstance(feature.binning, tuple):
                        l = ROOT.TLine(binning_args[1], y, binning_args[2], y)
                    else:
                        l = ROOT.TLine(binning_args[1][0], y, binning_args[1][-1], y)
                    r.setup_line(l, props={"NDC": False, "LineStyle": 3, "LineWidth": 1,
                        "LineColor": 1})
                    lines.append(l)
                for line in lines:
                    line.Draw("same")

                outputs = []
                if self.save_png:
                    outputs.append(self.output()["png"].targets["%s_%s_%s" %(process, feature.name, shape_syst)].path)
                if self.save_pdf:
                    outputs.append(self.output()["pdf"].targets["%s_%s_%s" %(process, feature.name, shape_syst)].path)
                for output in outputs:
                    c.SaveAs(create_file_dir(output))

    def run(self):
        """
        Splits the processes into data and non-data. Per feature, loads the input histograms,
        creates the output plots with up and down variations.
        """

        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        inputs = self.input()

        self.process_names = [process.name for process in self.processes_datasets.keys() if not process.isData]
        self.process_labels = [process.label for process in self.processes_datasets.keys() if not process.isData]

        if self.do_qcd:
            self.process_names.append("qcd")
            self.process_labels.append("QCD")

        for feature in self.features:
            self.shape_syst_list = self.get_unique_systs(self.get_systs(feature, True) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], True))

            tf = ROOT.TFile.Open(inputs["root"].targets[feature.name].path)

            self.histos = {}
            for process in self.process_names:
                self.histos[process] = {}
                self.histos[process]["central"] = copy(tf.Get("histograms/%s" % process))

                for shape_syst in self.shape_syst_list:
                    self.histos[process]["%s_up" %shape_syst] = copy(tf.Get("histograms/%s_%s_up" % (process, shape_syst)))
                    self.histos[process]["%s_down" %shape_syst] = copy(tf.Get("histograms/%s_%s_down" % (process, shape_syst)))

            tf.Close()

        self.plot(feature)

#################################################################################################################################
#################################################################################################################################
#################################################################################################################################

class BasePlotMultiDTask(BasePlotTask):
    n_features = -1
    def get_features(self):
        features = []
        for feature_pair_name in self.feature_names:
            try:
                feature_pair_names = feature_pair_name.split(":")
            except:
                print("%s cannot be considered for a 2D plot" % feature_pair_name)
                continue
            feature_pair = []
            for feature_name in feature_pair_names:
                for feature in self.config.features:
                    if feature.name == feature_name:
                        feature_pair.append(feature)
                        break
            if self.n_features > 1:
                if len(feature_pair) == self.n_features:
                    features.append(tuple(feature_pair))
                else:
                    raise ValueError(f"Task {type(self)} only allows {self.n_features} per group.")
            else:
                features.append(tuple(feature_pair))

        if len(features) == 0:
            raise ValueError("No features were included. Did you spell them correctly?")
        return features

    def get_binning(self, feature):
        binning_args = []
        for feature_elem in feature:
            if isinstance(feature_elem.binning, tuple):
                binning_args += feature_elem.binning
            else:
                binning_args += [len(feature_elem.binning) - 1, np.array(feature_elem.binning, dtype=np.float64)]
        return tuple(binning_args), ""


class PrePlot2D(PrePlot, BasePlotMultiDTask):
    n_features = 2
    def get_syst_list(self):
        if self.skip_processing:
            return []
        syst_list = []
        isMC = self.dataset.process.isMC
        for feature_pair in self.features:
            for feature in feature_pair:
                systs = self.get_unique_systs(self.get_systs(feature, isMC))
                for syst in systs:
                    if syst in syst_list:
                        continue
                    try:
                        systematic = self.config.systematics.get(syst)
                        if self.category.name in systematic.get_aux("affected_categories", []):
                            syst_list.append(syst)
                    except:
                        continue
        return syst_list

    def define_histograms(self, dfs, nentries):
        ROOT = import_root()
        histos = []
        isMC = self.dataset.process.isMC

        for feature_pair in self.features:
            binning_args, _ = self.get_binning(feature_pair)
            x_title = (str(feature_pair[0].get_aux("x_title"))
                + (" [%s]" % feature_pair[0].get_aux("units")
                if feature_pair[0].get_aux("units") else ""))
            y_title = (str(feature_pair[1].get_aux("x_title"))
                + (" [%s]" % feature_pair[1].get_aux("units")
                if feature_pair[1].get_aux("units") else ""))

            title = "; %s; %s; Events" % (x_title, y_title)
            systs = self.get_unique_systs(self.get_systs(feature_pair[0], isMC)
                + self.get_systs(feature_pair[1], isMC) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], isMC)
            )
            systs_directions = [("central", "")]
            if isMC and self.store_systematics:
                systs_directions += list(itertools.product(systs, directions))

            # loop over systematics and up/down variations
            for syst_name, direction in systs_directions:
                # Select the appropriate RDF (input file) depending on the syst and direction
                key = "central"
                if f"{syst_name}_{direction}" in dfs:
                    key = f"{syst_name}_{direction}"
                df = dfs[key]

                # apply selection if needed
                feat_df = df
                for ifeat, feature in enumerate(feature_pair):
                    if feature.selection:
                        feat_df = feat_df.Define(
                            "feat%s_selection" % ifeat, self.config.get_object_expression(
                                feature.selection, isMC, syst_name, direction)).Filter(
                            "feat%s_selection" % ifeat)

                # define tag just for histograms's name
                if syst_name != "central" and isMC:
                    tag = "_%s" % syst_name
                    if direction != "":
                        tag += "_%s" % direction
                else:
                    tag = ""
                feature_expressions = [
                    self.config.get_object_expression(
                        feature_pair[0], isMC, syst_name, direction),
                    self.config.get_object_expression(
                        feature_pair[1], isMC, syst_name, direction),
                ]
                feature_name = "_".join([feature.name for feature in feature_pair]) + tag
                hist_base = ROOT.TH2D(feature_name, title, *binning_args)
                if nentries[key] > 0:
                    hmodel = ROOT.RDF.TH2DModel(hist_base)
                    histos.append(
                        feat_df.Define(
                            "weight", "{}".format(self.get_weight(
                                self.category.name, syst_name, direction))
                            ).Define("var1", feature_expressions[0]).Define("var2", feature_expressions[1]
                            ).Histo2D(hmodel, "var1", "var2", "weight")
                    )
                else:  # no entries available, append empty histogram
                    histos.append(hist_base)
        return histos


class FeaturePlot2D(FeaturePlot, BasePlotMultiDTask):

    """
    Performs the actual histogram plotting: loads the histograms obtained in the PrePlot2D tasks,
    rescales them if needed and plots and saves them.

    Example command:

    ``law run FeaturePlot2D --version test --category-name etau --config-name ul_2018 \
--process-group-name etau --feature-names lep1_pt:lep1_eta,Hbb_mass:Htt_svfit_mass \
--workers 20 --PrePlot2D-workflow local --stack --hide-data False --do-qcd --region-name etau_os_iso\
--dataset-names tt_dl,tt_sl,dy_high,wjets,data_etau_a,data_etau_b,data_etau_c,data_etau_d \
--MergePreCounter-version test_old``

    :param log_z: whether to set y axis to log scale
    :type log_z: bool
    """

    log_z = luigi.BoolParameter(default=False, description="set logarithmic scale for Z axis, "
        "default: False")

    n_features = 2

    def requires(self):
        """
        All requirements needed:

            * Histograms coming from the PrePlot2D task.

            * Number of total events coming from the MergePreCounter task \
              (to normalize MC histograms).

            * If estimating QCD, FeaturePlot2D for the three additional QCD regions needed.

        """

        reqs = {}
        reqs["data"] = OrderedDict(
            ((dataset.name, category.name), PrePlot2D.vreq(self,
                dataset_name=dataset.name, category_name=self.get_data_category(category).name))
            for dataset, category in itertools.product(
                self.datasets_to_run, self.expand_category())
        )
        if not self.avoid_normalization:
            reqs["stats"] = OrderedDict()
            for dataset in self.datasets_to_run:
                if dataset.process.isData:
                    continue
                reqs["stats"][dataset.name] = {}
                for elem in ["central"] + [f"{syst}_{d}"
                        for (syst, d) in itertools.product(self.norm_syst_list, directions)]:
                    syst = ""
                    d = ""
                    if "_" in elem:
                        syst = elem.split("_")[0]
                        d = elem.split("_")[1]
                    if dataset.get_aux("secondary_dataset", None):
                        reqs["stats"][dataset.name][elem] = MergePreCounter.vreq(self,
                            dataset_name=dataset.get_aux("secondary_dataset"),
                            systematic=syst, systematic_direction=d)
                    else:
                        reqs["stats"][dataset.name][elem] = MergePreCounter.vreq(self,
                            dataset_name=dataset.name, systematic=syst, systematic_direction=d)

        if self.do_qcd:
            reqs["qcd"] = {
                key: self.req(self, region_name=region.name, blinded=False, hide_data=False,
                    do_qcd=False, stack=True, save_root=True, save_pdf=True, save_yields=False,
                    remove_horns=False,_exclude=["feature_tags", "shape_region",
                    "qcd_category_name", "qcd_sym_shape", "qcd_signal_region_wp"])
                for key, region in self.qcd_regions.items()
            }
            if self.qcd_category_name != "default":
                reqs["qcd"]["ss_iso"] = FeaturePlot2D.vreq(self,
                    region_name=self.qcd_regions.ss_iso.name, category_name=self.qcd_category_name,
                    blinded=False, hide_data=False, do_qcd=False, stack=True, save_root=True,
                    save_pdf=True, save_yields=False, feature_names=(self.qcd_feature,),
                    bin_opt_version=law.NO_STR, remove_horns=self.remove_horns, _exclude=["feature_tags",
                        "shape_region", "qcd_category_name", "qcd_sym_shape", "bin_opt_version",
                        "qcd_signal_region_wp"])
                # reqs["qcd"]["os_inviso"] = FeaturePlot2D.vreq(self,
                    # region_name=self.qcd_regions.os_inviso.name, category_name=self.qcd_category_name,
                    # blinded=False, hide_data=False, do_qcd=False, stack=True, save_root=True,
                    # save_pdf=True, save_yields=True, feature_names=(self.qcd_feature,),
                    # remove_horns=self.remove_horns, _exclude=["feature_tags", "bin_opt_version"])
                reqs["qcd"]["ss_inviso"] = FeaturePlot2D.vreq(self,
                    region_name=self.qcd_regions.ss_inviso.name,
                    category_name=self.qcd_category_name,
                    blinded=False, hide_data=False, do_qcd=False, stack=True, save_root=True,
                    save_pdf=True, save_yields=False, feature_names=(self.qcd_feature,),
                    bin_opt_version=law.NO_STR, remove_horns=self.remove_horns, _exclude=["feature_tags",
                        "shape_region", "qcd_category_name", "qcd_sym_shape", "bin_opt_version",
                        "qcd_signal_region_wp"])

        return reqs

    def get_output_postfix(self, key="pdf"):
        postfix = super(FeaturePlot2D, self).get_output_postfix(key)
        if self.log_z:
            postfix += "__logZ"
        return postfix

    def output(self):
        """
        Output files to be filled: pdf, png, root or json
        """
        def get_feature_name(feature_pair):
            return "_".join([feature.name for feature in feature_pair])

        # output definitions, i.e. key, file prefix, extension
        output_data = []
        if self.save_root:
            output_data.append(("root", "root/", "root"))
        if self.save_yields:
            output_data.append(("yields", "yields/", "json"))
        out = {
            key: law.SiblingFileCollection(OrderedDict(
                (get_feature_name(feature_pair), self.local_target("{}{}{}.{}".format(
                    prefix, get_feature_name(feature_pair),
                    self.get_output_postfix(key), ext)))
                for feature_pair in self.features
            ))
            for key, prefix, ext in output_data
        }
        output_data = []
        if self.save_pdf:
            output_data.append(("pdf", "", "pdf"))
        if self.save_png:
            output_data.append(("png", "", "png"))
        out.update({
            key: law.SiblingFileCollection(OrderedDict(
                (get_feature_name(feature_pair), OrderedDict(
                    (process.name, self.local_target("{}{}{}_{}.{}".format(
                        prefix, get_feature_name(feature_pair),
                        self.get_output_postfix(key), process.name, ext)))
                    for process in self.processes_datasets
                    if not process.isData or not self.hide_data
                ))
                for feature_pair in self.features
            ))
            for key, prefix, ext in output_data
        })
        return out

    def plot(self, feature):
        """
        Performs the actual plotting.
        """
        ROOT = import_root()
        import plotlib.root as r
        from plotting_tools.root import get_labels, Canvas, RatioCanvas

        # helper to extract the qcd shape in a region
        def get_qcd(region, files, bin_limit=0.):
            d_hist = files[region].Get("histograms/" + self.data_names[0])
            if not d_hist:
                raise Exception("data histogram '{}' not found for region '{}' in tfile {}".format(
                    self.data_names[0], region, files[region]))

            b_hists = []
            for b_name in self.background_names:
                b_hist = files[region].Get("histograms/" + b_name)
                if not b_hist:
                    raise Exception("background histogram '{}' not found in region '{}'".format(
                        b_name, region))
                b_hists.append(b_hist)

            qcd_hist = d_hist.Clone(randomize("qcd_" + region))
            for hist in b_hists:
                qcd_hist.Add(hist, -1.)

            # removing negative bins
            for ibinx, ibiny in itertools.product(
                    range(1, qcd_hist.GetNbinsX() + 1),
                    range(1, qcd_hist.GetNbinsY() + 1)):
                if qcd_hist.GetBinContent(ibinx, ibiny) < bin_limit:
                    qcd_hist.SetBinContent(ibinx, ibiny, 1.e-6)

            return qcd_hist

        def get_integral_and_error(hist):
            error = c_double(0.)
            integral = hist.IntegralAndError(
                0, hist.GetNbinsX() + 1,
                0, hist.GetNbinsY() + 1, error)
            error = error.value
            compatible = True if integral <= 0 else False
            # compatible = True if integral - error <= 0 else False
            return integral, error, compatible

        background_hists = self.histos["background"]
        signal_hists = self.histos["signal"]
        data_hists = self.histos["data"]
        all_hists = self.histos["all"]
        if self.plot_systematics:
            bkg_histo_syst = self.histos["bkg_histo_syst"]

        binning_args, _ = self.get_binning(feature)
        x_title = (str(feature[0].get_aux("x_title"))
            + (" [%s]" % feature[0].get_aux("units") if feature[0].get_aux("units") else ""))
        y_title = (str(feature[1].get_aux("x_title"))
            + (" [%s]" % feature[1].get_aux("units") if feature[1].get_aux("units") else ""))
        z_title = ("Events" if self.stack else "Normalized Events")
        hist_title = "; %s; %s; %s" % (x_title, y_title, z_title)

        feature_pair_name = "_".join([elem.name for elem in feature])
        # qcd shape files
        qcd_shape_files = None
        if self.do_qcd:
            qcd_shape_files = {}
            for key, region in self.qcd_regions.items():
                if self.qcd_category_name != "default":
                    my_feature = (feature_pair_name if "shape" in key or key == "os_inviso"
                        else self.qcd_feature)
                else:
                    my_feature = feature_pair_name
                qcd_shape_files[key] = ROOT.TFile.Open(self.input()["qcd"][key]["root"].targets[my_feature].path)

            # do the qcd extrapolation
            if "shape" in qcd_shape_files:
                qcd_hist = get_qcd("shape", qcd_shape_files).Clone(randomize("qcd"))
                qcd_hist.Scale(1. / qcd_hist.Integral())
            else:  #sym shape
                qcd_hist = get_qcd("shape1", qcd_shape_files).Clone(randomize("qcd"))
                qcd_hist2 = get_qcd("shape2", qcd_shape_files).Clone(randomize("qcd"))
                qcd_hist.Scale(1. / qcd_hist.Integral())
                qcd_hist2.Scale(1. / qcd_hist2.Integral())
                qcd_hist.Add(qcd_hist2)
                qcd_hist.Scale(0.5)

            n_os_inviso, n_os_inviso_error, n_os_inviso_compatible = get_integral_and_error(
                get_qcd("os_inviso", qcd_shape_files, -999))
            n_ss_iso, n_ss_iso_error, n_ss_iso_compatible = get_integral_and_error(
                get_qcd("ss_iso", qcd_shape_files, -999))
            n_ss_inviso, n_ss_inviso_error, n_ss_inviso_compatible = get_integral_and_error(
                get_qcd("ss_inviso", qcd_shape_files, -999))
            # if not n_ss_iso or not n_ss_inviso:
            if n_os_inviso_compatible or n_ss_iso_compatible or n_ss_inviso_compatible:
                print("****WARNING: QCD normalization failed (negative yield), Removing QCD!")
                qcd_scaling = 0.
                qcd_inviso = 0.
                qcd_inviso_error = 0.
                qcd_hist.Scale(qcd_scaling)
            else:
                qcd_inviso = n_os_inviso / n_ss_inviso
                qcd_inviso_error = qcd_inviso * math.sqrt(
                    (n_os_inviso_error / n_os_inviso) * (n_os_inviso_error / n_os_inviso)
                    + (n_ss_inviso_error / n_ss_inviso) * (n_ss_inviso_error / n_ss_inviso)
                )
                qcd_scaling = n_os_inviso * n_ss_iso / n_ss_inviso
                os_inviso_rel_error = n_os_inviso_error / n_os_inviso
                ss_iso_rel_error = n_ss_iso_error / n_ss_iso
                ss_inviso_rel_error = n_ss_inviso_error / n_ss_inviso
                new_errors_sq = {}
                for ibinx, ibiny in itertools.product(
                        range(1, qcd_hist.GetNbinsX() + 1),
                        range(1, qcd_hist.GetNbinsY() + 1)):
                    if qcd_hist.GetBinContent(ibinx, ibiny) > 0:
                        bin_rel_error = (qcd_hist.GetBinError(ibinx, ibiny)
                            / qcd_hist.GetBinContent(ibinx, ibiny))
                    else:
                        bin_rel_error = 0
                    new_errors_sq[(ibinx, ibiny)] = (bin_rel_error * bin_rel_error
                        + os_inviso_rel_error * os_inviso_rel_error
                        + ss_iso_rel_error * ss_iso_rel_error
                        + ss_inviso_rel_error * ss_inviso_rel_error)
                qcd_hist.Scale(qcd_scaling)
                # fix errors
                for ibinx, ibiny in itertools.product(
                        range(1, qcd_hist.GetNbinsX() + 1),
                        range(1, qcd_hist.GetNbinsY() + 1)):
                    qcd_hist.SetBinError(ibinx, ibiny, qcd_hist.GetBinContent(ibinx, ibiny)
                        * math.sqrt(new_errors_sq[(ibinx, ibiny)]))

            # store and style
            yield_error = c_double(0.)
            qcd_hist.cmt_yield = qcd_hist.IntegralAndError(
                0, qcd_hist.GetNbinsX() + 1, 0, qcd_hist.GetNbinsY() + 1, yield_error)
            qcd_hist.cmt_yield_error = yield_error.value
            qcd_hist.cmt_bin_yield = []
            qcd_hist.cmt_bin_yield_error = []
            for ibiny in range(1, qcd_hist.GetNbinsY() + 1):
                qcd_hist.cmt_bin_yield.append([
                    qcd_hist.GetBinContent(ibinx, ibiny)
                    for ibinx in range(1, qcd_hist.GetNbinsX() + 1)
                ])
                qcd_hist.cmt_bin_yield_error.append([
                    qcd_hist.GetBinError(ibinx, ibiny)
                    for ibinx in range(1, qcd_hist.GetNbinsX() + 1)
                ])
            qcd_hist.cmt_scale = 1.
            qcd_hist.cmt_process_name = "qcd"
            qcd_hist.process_label = "QCD"
            qcd_hist.SetTitle("QCD")
            background_hists.append(qcd_hist)
            all_hists.append(qcd_hist)

        if not self.hide_data:
            all_hists += data_hists

        bkg_histo = None
        if not self.stack:
            for hist in all_hists:
                scale = 1. / (hist.Integral() or 1.)
                hist.Scale(scale)
                hist.scale = scale
            draw_hists = all_hists
        else:
            draw_hists = all_hists
            for hist in background_hists:
                if not bkg_histo:
                    bkg_histo = hist.Clone()
                else:
                    bkg_histo.Add(hist.Clone())
            if not self.hide_data:
                draw_hists.extend(data_hists)

        dummy_hist = ROOT.TH2F(randomize("dummy"), hist_title, *binning_args)
        r.setup_hist(dummy_hist)
        dummy_hist.GetZaxis().SetMaxDigits(4)
        dummy_hist.GetZaxis().SetTitleOffset(1.22)  # FIXME

        if self.log_z:
            dummy_hist.SetMinimum(0.0011)
        else:
            dummy_hist.SetMinimum(0.001)

        inner_text=[self.category.label + " category"]
        if self.region:
            if isinstance(self.region.label, list):
                inner_text += self.region.label
            else:
                inner_text.append(self.region.label)

        if not (self.hide_data or not len(data_hists) > 0) or self.stack:
            ### Old implementation ###
            #upper_right="{}, {} TeV ({:.1f} ".format(
            #    self.config.year,
            #    self.config.ecm,
            #    self.config.lumi_fb
            #) + "fb^{-1})"
            ### New implementation: NOT TESTED ###
            if not type(self.config.lumi_fb) == dict:
                upper_right="{} , {:.1f} ".format(
                    self.config.year,
                    self.config.lumi_fb,
                ) + "fb^{-1} " + "({} TeV)".format(self.config.ecm)
            else:
                if self.run_era != "":
                    era_label = self.run_era
                    lumi_label = self.config.lumi_fb[self.config.get_run_period_from_run_era(self.run_era)][self.run_era]
                else:
                    era_label = self.run_period
                    if self.run_period != "":
                        lumi_label = sum(self.config.lumi_pb.get(self.run_period, {}).values())
                    else:
                        lumi_label = sum(sum(period_dict.values()) for period_dict in self.config.lumi_pb.values())
                upper_right="{} {}, {:.1f} ".format(
                    self.config.year,
                    era_label,
                    lumi_label,
                ) + "fb^{-1} " + "({} TeV)".format(self.config.ecm)
        else:
            upper_right="{} Simulation ({} TeV)".format(
                self.config.year,
                self.config.ecm,
            )

        draw_labels = get_labels(
            upper_right=upper_right,
            inner_text=inner_text
        )

        for ih, hist in enumerate(draw_hists):
            c = Canvas()
            if self.log_z:
                c.SetLogz()
            dummy_hist.SetMaximum(hist.GetMaximum())
            dummy_hist.Draw()
            hist.Draw("COLZ,SAME")
            for label in draw_labels:
                label.Draw("same")

            outputs = []
            if self.save_png:
                outputs.append(self.output()["png"].targets[feature_pair_name][hist.cmt_process_name].path)
            if self.save_pdf:
                outputs.append(self.output()["pdf"].targets[feature_pair_name][hist.cmt_process_name].path)
            for output in outputs:
                c.SaveAs(create_file_dir(output))

        if self.save_root:
            f = ROOT.TFile.Open(create_file_dir(
                self.output()["root"].targets[feature_pair_name].path), "RECREATE")
            f.cd()
            # c.Write("canvas") #FIXME

            hist_dir = f.mkdir("histograms")
            hist_dir.cd()
            for hist in all_hists:
                hist.Write(hist.cmt_process_name)
            if bkg_histo:
               bkg_histo.Write("background")

            if self.store_systematics:
                for syst_dir, shape_hists in self.histos["shape"].items():
                    for hist in shape_hists:
                        hist.Write("%s_%s" % (hist.cmt_process_name, syst_dir))
            f.Close()

        if self.save_yields:
            yields = OrderedDict()
            for hist in signal_hists + background_hists + data_hists:
                yields[hist.cmt_process_name] = {
                    "Total yield": hist.cmt_yield,
                    "Total yield error": hist.cmt_yield_error,
                    "Entries": getattr(hist, "cmt_entries", hist.GetEntries()),
                    "Binned yield": hist.cmt_bin_yield,
                    "Binned yield error": hist.cmt_bin_yield_error,
                }
            if bkg_histo:
                yields["background"] = {
                    "Total yield": sum([hist.cmt_yield for hist in background_hists]),
                    "Total yield error": math.sqrt(sum([(hist.cmt_yield_error)**2 for hist in background_hists])),
                    "Entries": getattr(bkg_histo, "cmt_entries", bkg_histo.GetEntries()),
                    "Binned yield": [[
                        sum([hist.cmt_bin_yield[j][i] for hist in background_hists])
                        for i in range(0, background_hists[0].GetNbinsX())]
                        for j in range(0, background_hists[0].GetNbinsY())],
                    "Binned yield error": [[
                        math.sqrt(sum([(hist.cmt_bin_yield_error[j][i]) ** 2
                            for hist in background_hists]))
                        for i in range(0, background_hists[0].GetNbinsX())]
                        for j in range(0, background_hists[0].GetNbinsY())],
                }
            with open(create_file_dir(self.output()["yields"].targets[feature_pair_name].path), "w") as f:
                json.dump(yields, f, indent=4)

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        """
        Splits processes into data, signal and background. Creates histograms from each process
        loading them from the input files. Scales the histograms and applies the correct format
        to them.
        """

        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        self.background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal]

        self.nevents, self.nweightedevents, self.nunweightedevents = self.get_nevents(inputs)

        for feature_pair in self.features:
            self.histos = {"background": [], "signal": [], "data": [], "all": []}

            binning_args, _ = self.get_binning(feature_pair)
            x_title = (str(feature_pair[0].get_aux("x_title"))
                + (" [%s]" % feature_pair[0].get_aux("units")
                if feature_pair[0].get_aux("units") else ""))
            y_title = (str(feature_pair[1].get_aux("x_title"))
                + (" [%s]" % feature_pair[1].get_aux("units")
                if feature_pair[1].get_aux("units") else ""))
            z_title = ("Events" if self.stack else "Normalized Events")
            hist_title = "; %s; %s: %s" % (x_title, y_title, z_title)

            systs_directions = [("central", "")]
            if self.plot_systematics:
                self.histos["bkg_histo_syst"] = ROOT.TH2D(
                    randomize("syst"), hist_title, *binning_args)
            if self.store_systematics:
                self.histos["shape"] = {}
                shape_systematics = self.get_unique_systs(
                    self.get_systs(feature_pair[0], True) \
                    + self.get_systs(feature_pair[1], True) \
                    + self.config.get_weights_systematics(self.config.weights[self.category.name],
                        True))

                systs_directions += list(itertools.product(shape_systematics, directions))

            feature_pair_name = "_".join([feature.name for feature in feature_pair])
            for (syst, d) in systs_directions:
                feature_name = feature_pair_name if syst == "central" else "%s_%s_%s" % (
                    feature_pair_name, syst, d)
                if syst != "central":
                    self.histos["shape"]["%s_%s" % (syst, d)] = []
                for iproc, (process, datasets) in enumerate(self.processes_datasets.items()):
                    if syst != "central" and process.isData:
                        continue
                    process_histo = ROOT.TH2D(randomize(process.name), hist_title, *binning_args)
                    process_histo.process_label = str(process.label)
                    process_histo.cmt_process_name = process.name
                    process_histo.Sumw2()
                    for dataset in datasets:
                        dataset_histo = ROOT.TH2D(randomize("tmp"), hist_title, *binning_args)
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
                                dataset_histo.Add(histo)
                            if not process.isData and not self.avoid_normalization:
                                elem = ("central"
                                    if syst == "central" or syst not in self.norm_syst_list
                                    else f"{syst}_{d}")
                                if self.nevents[dataset.name][elem] != 0:
                                    dataset_histo.Scale(
                                        self.get_normalization_factor(dataset, elem))
                                    scaling = dataset.get_aux("scaling", None)
                                    if scaling:
                                        print(" ### Scaling {} histo by {} +- {}".format(
                                            dataset.name, scaling[0], scaling[1]))
                                        old_errors = [[dataset_histo.GetBinError(ibinx, ibiny)\
                                                / dataset_histo.GetBinContent(ibinx, ibiny)
                                                if dataset_histo.GetBinContent(ibinx, ibiny) != 0
                                                else 0
                                                for ibinx in range(1, dataset_histo.GetNbinsX() + 1)]
                                            for ibiny in range(1, dataset_histo.GetNbinsY() + 1)
                                        ]
                                        new_errors = [[
                                                math.sqrt(elem ** 2 + (scaling[1] / scaling[0]) ** 2)
                                                for elem in line]
                                            for line in old_errors]
                                        dataset_histo.Scale(scaling[0])
                                        for ibinx, ibiny in itertools.product(
                                                range(1, dataset_histo.GetNbinsX() + 1),
                                                range(1, dataset_histo.GetNbinsY() + 1)):
                                            dataset_histo.SetBinError(
                                                ibinx, ibiny, dataset_histo.GetBinContent(ibinx, ibiny)
                                                    * new_errors[ibiny - 1][ibinx - 1])

                        process_histo.Add(dataset_histo)
                        if self.plot_systematics and not process.isData and not process.isSignal \
                                and syst == "central":
                            dataset_histo_syst = dataset_histo.Clone()
                            self.histos["bkg_histo_syst"].Add(dataset_histo_syst)

                    yield_error = c_double(0.)
                    process_histo.cmt_yield = process_histo.IntegralAndError(
                        0, process_histo.GetNbinsX() + 1,
                        0, process_histo.GetNbinsY() + 1, yield_error)
                    process_histo.cmt_yield_error = yield_error.value

                    process_histo.cmt_bin_yield = []
                    process_histo.cmt_bin_yield_error = []
                    for ibiny in range(1, process_histo.GetNbinsY() + 1):
                        process_histo.cmt_bin_yield.append(
                            [process_histo.GetBinContent(ibinx, ibiny)
                            for ibinx in range(1, process_histo.GetNbinsX() + 1)]
                        )
                        process_histo.cmt_bin_yield_error.append(
                            [process_histo.GetBinError(ibinx, ibiny)
                            for ibinx in range(1, process_histo.GetNbinsX() + 1)]
                        )

                    if syst == "central":
                        if process.isSignal:
                            self.histos["signal"].append(process_histo)
                        elif process.isData:
                            self.histos["data"].append(process_histo)
                        else:
                            self.histos["background"].append(process_histo)
                        if not process.isData: #or not self.hide_data:
                           self.histos["all"].append(process_histo)
                    else:
                        self.histos["shape"]["%s_%s" % (syst, d)].append(process_histo)

            self.plot(feature_pair)


class ComparisonPlot(FeaturePlot, BasePlotMultiDTask):

    """
    Plots histograms from different features.

    Example command:

    ``law run ComparisonPlot --version test --category-name etau --config-name ul_2018 \
--process-group-name etau --feature-names lep1_pt:lep1_pt_sel,lep1_eta:lep1_eta_sel1:lep1_eta_sel2 \
--workers 20 --PrePlot-workflow local --stack --hide-data False --do-qcd --region-name etau_os_iso\
--dataset-names tt_dl,tt_sl,dy_high,wjets,data_etau_a,data_etau_b,data_etau_c,data_etau_d \
--MergePreCounter-version test_old``

    """

    n_features = -1
    show_ratio = False
    plot_systematics = False
    store_systematics = False

    def __init__(self, *args, **kwargs):
        super(ComparisonPlot, self).__init__(*args, **kwargs)
        self.feature_list = set([f for fs in self.features for f in fs])
        self.do_qcd_bis = self.do_qcd
        self.do_qcd = False

    def requires(self):
        """
        Root files storing the histograms coming from FeaturePlot
        """
        return FeaturePlot.vreq(self, feature_names=[f.name for f in self.feature_list],
            save_root=True)

    def output(self):
        """
        Output files to be filled: pdf, png, root or json
        """
        def get_feature_name(feature_set):
            return "_".join([feature.name for feature in feature_set])

        # output definitions, i.e. key, file prefix, extension
        output_data = []
        if self.save_pdf:
            output_data.append(("pdf", "", "pdf"))
        if self.save_png:
            output_data.append(("png", "", "png"))
        if self.save_root:
            output_data.append(("root", "root/", "root"))
        if self.save_yields:
            output_data.append(("yields", "yields/", "json"))
        return {
            key: law.SiblingFileCollection(OrderedDict(
                (get_feature_name(feature), self.local_target("{}{}{}.{}".format(
                    prefix, get_feature_name(feature), self.get_output_postfix(key), ext)))
                for feature in self.features
            ))
            for key, prefix, ext in output_data
        }

    def get_binning(self, feature, ifeat=0):
        return FeaturePlot.get_binning(self, feature, ifeat)

    def requires(self):
        """
        Root files storing the histograms coming from FeaturePlot
        """
        return FeaturePlot.vreq(self, feature_names=[f.name for f in self.feature_list],
            save_root=True, plot_systematics=False, store_systematics=False,
            _exclude=["min_y", "max_y"])

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        """
        Splits processes into data, signal and background. Creates histograms from each process
        loading them from the input files. Scales the histograms and applies the correct format
        to them.
        """

        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()
        processes = list(self.processes_datasets.keys())
        if self.do_qcd_bis:
            processes.append(self.config.get(self.qcd_process_name))
        nprocesses = len(processes)

        for feature_set in self.features:
            self.histos = {"background": [], "signal": [], "data": [], "all": []}
            colors = list(range(2, 2 + len(feature_set) * nprocesses))
            ihisto = -1

            for process in processes:
                for feature in feature_set:
                    tf = ROOT.TFile.Open(inputs["root"].targets[feature.name].path)
                    ihisto += 1
                    process_histo = copy(tf.Get("histograms/" + process.name))
                    process_histo.cmt_process_name = process.name
                    process_histo.process_label = str(process.label)
                    if feature.get_aux("selection_name"):
                        process_histo.process_label += f", {feature.get_aux('selection_name')}"

                    self.setup_signal_hist(process_histo, colors[ihisto])
                    self.histos["signal"].append(process_histo)
                    self.histos["all"].append(process_histo)

            feature_to_save = copy(feature_set[0])
            feature_to_save.name = "_".join([f.name for f in feature_set])
            self.plot(feature_to_save)


class EfficiencyPlot(ComparisonPlot):

    """
    Plots efficiencies for the features and processes included as input.

    Example command:

    ``law run EfficiencyPlot --version version_name --category-name category_name
--config-name config_name --feature-names numerator1:denominator1,numerator2:denominator2``

    """

    # law run EfficiencyPlot --version version_name --category-name category_name
    # --config-name config_name --feature-names numerator1:denominator1,numerator2:denominator2

    max_y = 1.
    stack = True
    isEfficiency = True  # uses Efficiency as y axis title

    def requires(self):
        """
        Root files storing the histograms coming from FeaturePlot
        """
        return FeaturePlot.vreq(self, feature_names=[f.name for f in self.feature_list],
            save_root=True, plot_systematics=False, store_systematics=False, stack=True,
            _exclude=["min_y", "max_y"])

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        """
        Splits processes into data, signal and background. Creates histograms from each process
        loading them from the input files. Scales the histograms and applies the correct format
        to them.
        """

        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()
        processes = list(self.processes_datasets.keys())
        if self.do_qcd_bis:
            processes.append(self.config.get(self.qcd_process_name))
        nprocesses = len(processes)

        for feature_set in self.features:
            self.histos = {"background": [], "signal": [], "data": [], "all": []}
            colors = list(range(2, 2 + len(feature_set) * nprocesses))

            for iprocess, process in enumerate(processes):
                num_histo, den_histo = None, None
                for feature in reversed(feature_set):
                    tf = ROOT.TFile.Open(inputs["root"].targets[feature.name].path)

                    if not den_histo:
                        den_histo = copy(tf.Get("histograms/" + process.name))
                        den_histo.SetTitle(f"CACA; {den_histo.GetXaxis().GetTitle()}; Efficiency")
                    else:
                        num_histo = copy(tf.Get("histograms/" + process.name))
                        den_histo.SetTitle(f"CACA; {den_histo.GetXaxis().GetTitle()}; Efficiency")

                process_histo = ROOT.TEfficiency(num_histo, den_histo)
                process_histo.SetTitle(f"CACA; {den_histo.GetXaxis().GetTitle()}; Efficiency")
                process_histo.cmt_process_name = process.name
                process_histo.process_label = str(process.label)
                self.setup_signal_hist(process_histo, colors[iprocess])
                self.histos["signal"].append(process_histo)
                self.histos["all"].append(process_histo)

            feature_to_save = copy(feature_set[1])
            feature_to_save.name = "_".join([f.name for f in feature_set])
            self.plot(feature_to_save)

<<<<<<< HEAD
#########################
#  FeatureDump Methods  #
#########################

class FeatureDump(PrePlot):
    """
    Applies all the needed selections (category, channel, feature) and dumps in
    and output dictionary all the requested features.

    :param run_period_name: name of the run period to be added as feature.
    :type run_period_name: str
    """

    # Name of the run period to be stored in the output dictionary
    run_period_name = luigi.Parameter(default="", description="Run period name,"
        " allowed values: 2022, 2022EE, 2023, 2023BPix, 2024 - default: None")

    # We only care about the central value
    store_systematics = False

    def __init__(self, *args, **kwargs):
        super(FeatureDump, self).__init__(*args, **kwargs)
        if self.run_period_name not in ["2022", "2022EE", "2023", "2023BPix", "2024"]:
            raise ValueError(f"Run period requested ({self.run_period_name}) not"
                " among allowed options: 2022, 2022EE, 2023, 2023BPix, 2024")

    def output(self):
        """
        :return: One output file per input file
        :rtype: `.json`
        """
        return self.local_target("data{}_{}.json".format(
            self.get_output_postfix(), self.branch))

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, applies selections and
        dumps requested features into the output dictionary
        """
        ROOT = import_root()

        # prepare inputs and outputs
        if self.skip_processing:
            inp = self.get_input()
        else:
            inp = self.input()
        outp = self.output().path

        ROOT.ROOT.EnableThreadSafety()
        ROOT.ROOT.EnableImplicitMT(self.request_cpus)

        # Create RDF
        elem = "central"
        if self.skip_processing:
            inp_to_consider = self.get_path(inp[elem])[0]
            if not self.dataset.friend_datasets:
                df = self.RDataFrame(self.tree_name, self.get_path(inp[elem]),
                    allow_redefinition=self.allow_redefinition)
            # friend tree
            else:
                tchain = ROOT.TChain()
                for f in self.get_path(inp[elem]):
                    tchain.Add("{}/{}".format(f, self.tree_name))
                friend_tchain = ROOT.TChain()
                for f in self.get_path(inp[elem], 1):
                    friend_tchain.Add("{}/{}".format(f, self.tree_name))
                tchain.AddFriend(friend_tchain, "friend")
                df = self.RDataFrame(tchain, allow_redefinition=self.allow_redefinition)
        elif self.skip_merging:
            inp_to_consider = inp[elem]["root"].path
            df = self.RDataFrame(self.tree_name, inp_to_consider,
                allow_redefinition=self.allow_redefinition)
        else:
            inp_to_consider = inp[elem].targets[self.branch].path
            df = self.RDataFrame(self.tree_name, inp_to_consider,
                allow_redefinition=self.allow_redefinition)

        empty_file = False
        tf = ROOT.TFile.Open(inp_to_consider)
        tree = tf.Get(self.tree_name)
        if not tree: # no tree inside the file
            raise RuntimeError(f"FeatureDump : Input file '{inp_to_consider}' has no TTree named"
                               f" '{self.tree_name}'. Try removing the file and running the task again.")
        nentries = tree.GetEntries()
        if nentries == 0: # tree with 0 entries
            empty_file = True
        tf.Close()

        if not empty_file:
            # Define selections
            selection = "1"
            dataset_selection = self.config.get_object_expression(
                self.dataset.get_aux("selection", "1"), self.dataset.process.isMC)

            if self.skip_processing:
                selection = self.config.get_object_expression(
                    self.category, self.dataset.process.isMC)

            if dataset_selection and dataset_selection != "1":
                if selection != "1":
                    selection = join_root_selection(dataset_selection, selection, op="and")
                else:
                    selection = dataset_selection

            if self.region_name != law.NO_STR:
                region_selection = self.config.get_object_expression(
                    self.config.regions.get(self.region_name).selection,
                    self.dataset.process.isMC)
                if selection != "1":
                    selection = join_root_selection(region_selection, selection, op="and")
                else:
                    selection = region_selection

            # Apply selection
            if selection != "1":
                df = df.Define("selection", selection).Filter("selection")

            # Snapshot the needed branches to a pandas DataFrame
            pdf = df.AsNumpy(list(self.feature_names))
            pdf = pd.DataFrame(pdf)

            # Add year value
            pdf["year"] = self.run_period_name

            # Rename luminosityBlock -> lumi if needed
            if "luminosityBlock" in self.feature_names:
                pdf = pdf.rename(columns={"luminosityBlock": "lumi"})

            # Convert to list of dictionaries
            records = pdf.to_dict(orient="records")

            # Save output
            with open(create_file_dir(self.output().path), "w+") as f:
                json.dump(records, f, indent=4)

        else: # empty input file case
            # Save empty list
            with open(create_file_dir(self.output().path), "w+") as f:
                json.dump([], f, indent=4)


class FeatureDumpWrapper(DatasetCategoryWrapperTask):
    """
    Wrapper for FeatureDump task
    """
    def atomic_requires(self, dataset, category):
        return FeatureDump.req(self, dataset_name=dataset.name, category_name=category.name)


class MergeFeatureDump(DatasetTaskWithCategory, law.tasks.ForestMerge):
    """
    Merge the output from the FeatureDump task in order to have
    a single output file for each channel/category/dataset.
    """

    merge_factor = 16

    def __init__(self, *args, **kwargs):
        super(MergeFeatureDump, self).__init__(*args, **kwargs)

    def merge_workflow_requires(self):
        return FeatureDump.vreq(self, _prefer_cli=["workflow"])

    def merge_requires(self, start_leaf, end_leaf):
        return FeatureDump.vreq(self, workflow="local",
            branches=((start_leaf, end_leaf),), _exclude={"branch"})

    def trace_merge_inputs(self, inputs):
        return [inp for inp in inputs["collection"].targets.values()]

    def merge_output(self):
        postfix = "__" + self.region.name
        return self.local_target(f"data{postfix}.json")

    def merge(self, inputs, output):
        # Output content
        outputs = []

        # Merge inputs
        for inp in inputs:
            try:
                if "json" in inp.path:
                    values = inp.load(formatter="json")
            except:
                raise ValueError(f"MergeFeatureDump : error loading input file {inp}")

            # Add inputs
            outputs += values

        # Write output
        output.parent.touch()
        output.dump(outputs, indent=4, formatter="json")


class MergeFeatureDumpWrapper(DatasetCategoryWrapperTask):
    """
    Wrapper task to run the MergeFeatureDump task over several datasets in parallel.
    """
    def atomic_requires(self, dataset, category):
        return MergeFeatureDump.req(self, dataset_name=dataset.name, category_name=category.name)

class MultiConfigFeaturePlot(FeaturePlot, MultiConfigProcessGroupNameTask):
    recompute_qcd = luigi.BoolParameter(default=False, description="whether to compute the QCD shape "
        "using all configs, default: False (extracted per config)")
=======

class MultiConfigFeaturePlot(FeaturePlot, MultiConfigProcessGroupNameTask):
>>>>>>> 3a8529d (First implementation of MultiConfigFeaturePlot)

    def __init__(self, *args, **kwargs):
        super(MultiConfigFeaturePlot, self).__init__(*args, **kwargs)

        # when combining configs, it doesn't make a lot of sense to restrict to an era or period
        # this can be included later if it's found useful
        assert not self.run_period and not self.run_era

    # extracting the category information from the first config
    def get_category(self, category_name: str):
        return super(MultiConfigFeaturePlot, self).get_category(
            category_name,
            config=self.load_config(self.config_names[0]))

    # extracting the region information from the first config
    def get_region(self, region_name: str):
        return super(MultiConfigFeaturePlot, self).get_region(
            region_name,
            config=self.load_config(self.config_names[0]))

    # extracting the qcd regions from the first config
    def get_qcd_regions(self):
        return super(MultiConfigFeaturePlot, self).get_qcd_regions(
            config=self.load_config(self.config_names[0]))

    # extracting the features to run from the first config
    def _find_features(self, names, tags, config=None):
        return super(MultiConfigFeaturePlot, self)._find_features(names, tags,
            self.load_config(self.config_names[0]))

    def requires(self):
<<<<<<< HEAD
        reqs = {}
        feature_plot_reqs = FeaturePlot.requires(self)
        reqs["histos"] = {
            config_name: FeaturePlot.vreq(self, config_name=config_name, save_root=True,
                stack=True, avoid_normalization=False, normalize_signals=False,
                do_qcd=self.do_qcd and not self.recompute_qcd)
            for config_name in self.config_names
        }
        if self.do_qcd and self.recompute_qcd:
            reqs["qcd"] = feature_plot_reqs["qcd"]
        return reqs
=======
        return {
            config_name: FeaturePlot.vreq(self, config_name=config_name, save_root=True,
                stack=True, avoid_normalization=False, normalize_signals=False)
            for config_name in self.config_names
        }
>>>>>>> 3a8529d (First implementation of MultiConfigFeaturePlot)

    def get_labels_to_draw(self, signal_hists, data_hists, bkg_histo, draw_hists, maximum, label_scaling):
        print("Running get_labels_to_draw from MultiConfigFeaturePlot")

        # get text to plot inside the figure
        inner_text = self.config.get_inner_text_for_plotting(self.category, self.region)

        if self.normalize_signals and self.stack and signal_hists and bkg_histo:
            scale_text = []
            for hist in signal_hists:
                scale = hist.cmt_scale
                if scale != 1.:
                    if scale < 100:
                        scale = "{:.1f}".format(scale)
                    elif scale < 10000:
                        scale = "{}".format(int(round(scale)))
                    else:
                        scale = "{:.2e}".format(scale).replace("+0", "").replace("+", "")
                    scale_text.append("{} x{}".format(hist.process_label, scale))
            inner_text.append("#scale[0.75]{{{}}}".format(",  ".join(scale_text)))

        if (maximum > 1e4 or maximum < 1e-4) and not self.log_y:
            upper_left_offset = 0.05
        else:
            upper_left_offset = 0.0

        # Given that we are scaling the input histograms to lumi to maintain the proportions,
        # lumi will be included in the labels, regardless of the final histograms being or not
        # normalized to unity
        lumi_per_ecm = {}
        for config_name in self.config_names:
            config = self.load_config(config_name)
            lumi = (config.lumi_fb if not type(config.lumi_fb) == dict
                else sum(sum(period_dict.values()) for period_dict in self.config.lumi_fb.values()))
            if not type(config.lumi_fb) == dict:
                if config.ecm not in lumi_per_ecm:
                    lumi_per_ecm[config.ecm] = 0
                lumi_per_ecm[config.ecm] += lumi

        upper_right = " + ".join([f"{get_lumi_string(lumi)} ({ecm} TeV)"
            for ecm, lumi in lumi_per_ecm.items()])

        try:
            m = max([hist.GetMaximum() for hist in draw_hists]) if not self.log_y else None
        except AttributeError:  # TEfficiency doesn't have GetMaximum()
            m = None

        return get_labels(
            upper_left=self.config.upper_left_text,
            upper_left_offset=upper_left_offset,
            upper_right=upper_right,
            scaling=label_scaling,
            inner_text=inner_text,
            max=m
        )

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        """
        Splits processes into data, signal and background. Creates histograms from each process
        loading them from the input files. Scales the histograms and applies the correct format
        to them.
        """

        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

<<<<<<< HEAD
        if self.do_qcd and not self.recompute_qcd:
            # adding qcd process to the list of processes so that
            # the corresponding histograms are extracted from the root files
            self.processes_datasets[Process("qcd", "QCD", color=(255, 87, 215))] = []

        self.data_names = [p.name for p in self.processes_datasets.keys()
            if p.isData or p.get_aux("isFakeData", False)]
        self.background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal and not p.get_aux("isFakeData", False)]

=======
>>>>>>> 3a8529d (First implementation of MultiConfigFeaturePlot)
        processes = list(self.processes_datasets.keys())
        #if self.do_qcd_bis:
        #    processes.append(self.config.get(self.qcd_process_name))
        nprocesses = len(processes)
        if self.fixed_colors:
            colors = list(range(2, 2 + len(self.processes_datasets.keys())))

        for ifeat, feature in enumerate(self.features):
            self.histos = {"background": [], "signal": [], "data": [], "all": []}

            binning_args, y_axis_adendum = self.get_binning(feature, ifeat)
            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
            hist_title = "; %s; %s" % (x_title, y_title)

            systs_directions = [("central", "")]
            if self.plot_systematics:
                self.histos["bkg_histo_syst"] = ROOT.TH1D(
                    randomize("syst"), hist_title, *binning_args)
            if self.store_systematics:
                self.histos["shape"] = {}
                shape_systematics = self.get_systs(feature, True)
                systs_directions += list(itertools.product(shape_systematics, directions))

            for (syst, d) in systs_directions:
                feature_name = feature.name if syst == "central" else "%s_%s_%s" % (
                    feature.name, syst, d)
                if syst != "central":
                    self.histos["shape"]["%s_%s" % (syst, d)] = []
                for iproc, process in enumerate(self.processes_datasets.keys()):
                    if syst != "central" and process.isData:
                        continue
                    if self.do_sideband and not process.isData and not process.isSignal:
                        continue
                    process_histo = ROOT.TH1D(randomize(process.name), hist_title, *binning_args)
                    process_histo.process_label = str(process.label)
                    process_histo.cmt_process_name = process.name
                    process_histo.Sumw2()

                    # loop over configs
                    for inputs in self.input()["histos"].values():
                        tf = ROOT.TFile.Open(inputs["root"].targets[feature.name].path)
                        histo = copy(tf.Get("histograms/" + process.name).Clone())
                        if not "TObject" in str(type(histo)):
                            process_histo.Add(histo)
                        del histo
                        tf.Close()
                        # FIXME: include here treatment of norm systematics,
                        # as they may vary per config (e.g. different years)

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
                        if self.fixed_colors:
                            color = colors[iproc]
                        elif type(process.color) == tuple:
                            color = ROOT.TColor.GetColor(*process.color)
                        else:
                            color = process.color

                        if process.isSignal:
                            self.setup_signal_hist(process_histo, color)
                            self.histos["signal"].append(process_histo)
                        elif process.isData or process.get_aux("isFakeData", False):
                            self.setup_data_hist(process_histo, color)
                            self.histos["data"].append(process_histo)
                        else:
                            self.setup_background_hist(process_histo, color)
                            self.histos["background"].append(process_histo)
                        if not (process.isData or process.get_aux("isFakeData", False)): #or not self.hide_data:
                           self.histos["all"].append(process_histo)
                    else:
                        self.histos["shape"]["%s_%s" % (syst, d)].append(process_histo)

                    del process_histo

            # FIXME: include binning optimization

            self.plot(
                feature,
                compute_qcd=self.do_qcd and self.recompute_qcd
            )
