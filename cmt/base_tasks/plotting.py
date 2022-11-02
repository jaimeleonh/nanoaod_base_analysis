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
import functools
from collections import OrderedDict

import law
import luigi
import plotlib.root as r
from cmt.util import hist_to_array, hist_to_graph, get_graph_maximum, update_graph_values

from ctypes import c_double


from analysis_tools import ObjectCollection
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection, randomize
)
from plotting_tools.root import get_labels, Canvas, RatioCanvas
from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, ConfigTaskWithCategory,
    RDFModuleTask, InputData
)

from cmt.base_tasks.preprocessing import (
    Categorization, MergeCategorization, MergeCategorizationStats, EventCounterDAS
)

cmt_style = r.styles.copy("default", "cmt_style")
cmt_style.style.ErrorX = 0
cmt_style.x_axis.TitleOffset = 1.22
cmt_style.y_axis.TitleOffset = 1.48
cmt_style.legend.TextSize = 20
cmt_style.style.legend_dy = 0.035
cmt_style.style.legend_y2 = 0.93

EMPTY = -1.e5

ROOT = import_root()

class BasePlotTask(ConfigTaskWithCategory):
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

    :param n_bins: NOT YET IMPLEMENTED. Custom number of bins for plotting,
        defaults to the value configured by the feature when empty.
    :type n_bins: int

    :param systematics: NOT YET IMPLEMENTED. List of custom systematics to be considered.
    :type systematics: csv list

    :param shape_region: shape region used for QCD computation.
    :type shape_region: str from choice list

    :param remove_horns: NOT YET IMPLEMENTED. Whether to remove the eta horns present in 2017
    :type remove_horns: bool
    """

    feature_names = law.CSVParameter(default=(), description="names of features to plot, uses all "
        "features when empty, default: ()")
    feature_tags = law.CSVParameter(default=(), description="list of tags for filtering features "
        "selected via feature_names, default: ()")
    skip_feature_names = law.CSVParameter(default=(), description="names or name pattern of "
        "features to skip, default: ()")
    skip_feature_tags = law.CSVParameter(default=("multiclass_dnn",), description="list of tags of "
        "features to skip, default: (multiclass_dnn,)")
    apply_weights = luigi.BoolParameter(default=True, description="whether to apply "
        "mc weights to histograms, default: True")
    n_bins = luigi.IntParameter(default=law.NO_INT, description="custom number of bins for "
        "plotting, defaults to the value configured by the feature when empty, default: empty")
    systematics = law.CSVParameter(default=(), description="list of systematics, default: empty")
    shape_region = luigi.ChoiceParameter(default="os_inviso", choices=("os_inviso", "ss_iso"),
        significant=False, description="shape region default: os_inviso")
    remove_horns = luigi.BoolParameter(default=False, description="whether to remove horns "
        "from distributions, default: False")

    tree_name = "Events"

    def __init__(self, *args, **kwargs):
        super(BasePlotTask, self).__init__(*args, **kwargs)
        # select features
        self.features = self.get_features()

    def get_features(self):
        features = []
        for feature in self.config.features:
            if self.feature_names and not law.util.multi_match(feature.name, self.feature_names):
                continue
            if self.feature_tags and not feature.has_tag(self.feature_tags):
                continue
            if self.skip_feature_names and \
                    law.util.multi_match(feature.name, self.skip_feature_names):
                continue
            if self.skip_feature_tags and feature.has_tag(self.skip_feature_tags):
                continue
            features.append(feature)
        return features

    def get_binning(self, feature):
        y_axis_adendum = (" / %s %s" % (
            (feature.binning[2] - feature.binning[1]) / feature.binning[0],
                feature.get_aux("units")) if feature.get_aux("units") else "")
        return feature.binning, y_axis_adendum

    def get_feature_systematics(self, feature):
        return feature.systematics

    def get_systs(self, feature, isMC):
        systs = []
        if not isMC:
            return systs
        for syst in self.get_feature_systematics(feature):
            systs.append(syst)
        return systs

    def get_output_postfix(self):
        postfix = ""
        if self.region:
            postfix += "__" + self.region.name
        if not self.apply_weights:
            postfix += "__noweights"
        return postfix


class PrePlot(DatasetTaskWithCategory, BasePlotTask, law.LocalWorkflow, HTCondorWorkflow,
        RDFModuleTask):
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
        default="")

    def create_branch_map(self):
        """
        :return: number of files after merging (usually 1) unless skip_processing == True
        :rtype: int
        """
        if self.skip_processing or self.skip_merging:
            return len(self.dataset.get_files(
                os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config.name), add_prefix=False))
        return self.n_files_after_merging

    def workflow_requires(self):
        """
        """
        if self.skip_merging:
            return {"data": Categorization.vreq(self, workflow="local")}
        if self.skip_processing:
            return {"data": InputData.req(self)}
        return {"data": MergeCategorization.vreq(self, workflow="local")}

    def requires(self):
        """
        Each branch requires one input file
        """
        if self.skip_merging:
            return Categorization.vreq(self, workflow="local", branch=self.branch)
        if self.skip_processing:
            return InputData.req(self, file_index=self.branch)
        return MergeCategorization.vreq(self, workflow="local", branch=self.branch)

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
        if self.config.processes.get(self.dataset.process.name).isData or not self.apply_weights:
            return "1"
        else:
            return self.config.get_weights_expression(
                self.config.weights[category], syst_name, syst_direction)
        return self.config.weights.default

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules
        and produces a set of plots per each feature, one for the nominal value
        and others (if available) for all systematics.
        """
        isMC = self.dataset.process.isMC
        directions = ["up", "down"]

        # prepare inputs and outputs
        if self.skip_processing:
            inp = self.input().path
        elif self.skip_merging:
            inp = self.input()["data"].path
        else:
            inp = self.input().targets[self.branch].path
        outp = self.output().path

        df = ROOT.RDataFrame(self.tree_name, inp)

        modules = self.get_feature_modules(self.preplot_modules_file)
        if len(modules) > 0:
            for module in modules:
                df, _ = module.run(df)

        selection = "1"
        dataset_selection = self.dataset.get_aux("selection")

        if self.skip_processing:
            selection = self.config.get_object_expression(self.category, self.dataset.process.isMC)

        if dataset_selection and dataset_selection != "1":
            if selection != "1":
                selection = join_root_selection(dataset_selection, selection, op="and")
            else:
                selection = dataset_selection

        if self.region_name != law.NO_STR:
            region_selection = self.config.regions.get(self.region_name).selection
            if selection != "1":
                selection = join_root_selection(region_selection, selection, op="and")
            else:
                selection = region_selection
        if selection != "1":
            df = df.Define("selection", selection).Filter("selection")

        tf = ROOT.TFile.Open(inp)
        tree = tf.Get(self.tree_name)
        nentries = tree.GetEntries()
        histos = []
        for feature in self.features:
            binning_args, y_axis_adendum = self.get_binning(feature)
            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = "Events" + y_axis_adendum
            title = "; %s; %s" % (x_title, y_title)
            systs = self.get_systs(feature, isMC) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], isMC)
            systs_directions = [("central", "")]
            if isMC:
                systs_directions += list(itertools.product(systs, directions))

            # apply selection if needed
            if feature.selection:
                feat_df = df.Define("feat_selection", feature.selection).Filter("feat_selection")
            else:
                feat_df = df

            # loop over systematics and up/down variations
            for syst_name, direction in systs_directions:
                # define tag just for histograms's name
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
                if nentries > 0:
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

        out = ROOT.TFile.Open(create_file_dir(outp), "RECREATE")
        for histo in histos:
            histo = histo.Clone()
            histo.Sumw2()
            histo.Write()
        out.Close()


class FeaturePlot(BasePlotTask, DatasetWrapperTask):
    """
    Performs the actual histogram plotting: loads the histograms obtained in the PrePlot tasks,
    rescales them if needed and plots and saves them.

    Example command:

    ``law run FeaturePlot --version test --category-name etau --config-name ul_2018 \
--process-group-name etau --feature-names Htt_svfit_mass,lep1_pt,bjet1_pt,lep1_eta,bjet1_eta \
--workers 20 --PrePlot-workflow local --stack --hide-data False --do-qcd --region-name etau_os_iso\
--dataset-names tt_dl,tt_sl,dy_high,wjets,data_etau_a,data_etau_b,data_etau_c,data_etau_d \
--MergeCategorizationStats-version test_old --PrePlot-workflow htcondor``

    :param stack: whether to show all backgrounds stacked (True) or normalized to 1 (False)
    :type stack: bool

    :param do_qcd: whether to estimate QCD using the ABCD method
    :type do_qcd: bool

    :param qcd_wp: working point to use for QCD estimation
    :type qcd_wp: str from choice list

    :param qcd_signal_region_wp: region to use as signal region for QCD estimation
    :type qcd_signal_region_wp: str

    :param shape_region: region to use as shape region for QCD estimation
    :type shape_region: str from choice list

    :param qcd_sym_shape: whether to symmetrise the shape coming from both possible shape regions
    :type qcd_sym_shape: bool

    :param qcd_category_name: category name used for the same sign regions in QCD estimation
    :type qcd_category_name: str

    :param hide_data: whether to show (False) or hide (True) the data histograms
    :type hide_data: bool

    :param normalize_signals: (NOT YET IMPLEMENTED) whether to normalize signals to the
        total background yield (True) or not (False)
    :type normalize_signals: bool

    :param blinded: (NOT YET IMPLEMENTED) whether to blind data in specified regions
    :type blinded: bool

    :param save_png: whether to save plots in png
    :type save_png: bool

    :param save_pdf: whether to save plots in pdf
    :type save_pdf: bool

    :param save_root: whether to write plots in a root file
    :type save_root: bool

    :param save_yields: (NOT YET IMPLEMENTED) whether to save histogram yields in a json file
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

    """
    stack = luigi.BoolParameter(default=False, description="when set, stack backgrounds, weight "
        "them with dataset and category weights, and normalize afterwards, default: False")
    do_qcd = luigi.BoolParameter(default=False, description="whether to compute the QCD shape, "
        "default: False")
    qcd_wp = luigi.ChoiceParameter(default=law.NO_STR,
        choices=(law.NO_STR, "vvvl_vvl", "vvl_vl", "vl_l", "l_m"), significant=False,
        description="working points to use for qcd computation, default: empty (vvvl - m)")
    qcd_signal_region_wp = luigi.Parameter(default="os_iso", description="signal region wp, "
        "default: os_iso")
    shape_region = luigi.ChoiceParameter(default="os_inviso", choices=("os_inviso", "ss_iso"),
        significant=True, description="shape region default: os_inviso")
    qcd_sym_shape = luigi.BoolParameter(default=False, description="symmetrize QCD shape, "
        "default: False")
    qcd_category_name = luigi.Parameter(default="default", description="category use "
        "for qcd regions ss_iso and ss_inviso, default=default (same as category)")
    hide_data = luigi.BoolParameter(default=True, description="hide data events, default: True")
    normalize_signals = luigi.BoolParameter(default=True, description="whether to normalize "
        "signals to the total bkg yield, default: True")
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
    process_group_name = luigi.Parameter(default="default", description="the name of the process "
        "grouping, only encoded into output paths when changed, default: default")
    bins_in_x_axis = luigi.BoolParameter(default=False, description="whether to show in the x axis "
        "bin numbers instead of the feature value, default: False")
    plot_systematics = luigi.BoolParameter(default=True, description="whether plot systematics, "
        "default: True")
    fixed_colors = luigi.BoolParameter(default=False, description="whether to use fixed colors "
        "for plotting, default: False")
    log_y = luigi.BoolParameter(default=False, description="set logarithmic scale for Y axis, "
        "default: False")
    # # optimization parameters
    # bin_opt_version = luigi.Parameter(default=law.NO_STR, description="version of the binning "
        # "optimization task to use, not used when empty, default: empty")
    # n_start_bins = luigi.IntParameter(default=10, description="number of bins to be used "
        # "as initial value for scans, default: 10")
    # optimization_method = luigi.ChoiceParameter(default="flat_s",
        # choices=("flat_s", "flat_b", "flat_sb", "flat_s_b", "const", "flat_mpp", "flat_mpp_sb"),
        # significant=False, description="optimization method to be used, default: flat signal")
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

    def __init__(self, *args, **kwargs):
        super(FeaturePlot, self).__init__(*args, **kwargs)

        # select processes and datasets
        assert self.process_group_name in self.config.process_group_names
        self.processes_datasets = {}
        self.datasets_to_run = []

        def get_processes(dataset=None, process=None):
            processes = ObjectCollection()
            if dataset and not process:
                process = self.config.processes.get(dataset.process.name)
            processes.append(process)
            if process.parent_process:
                processes += get_processes(process=self.config.processes.get(
                    process.parent_process))
            return processes

        for dataset in self.datasets:
            processes = get_processes(dataset=dataset)
            filtered_processes = ObjectCollection()
            for process in processes:
                if process.name in self.config.process_group_names[self.process_group_name]:
                    filtered_processes.append(process)
            if len(filtered_processes) > 1:
                raise Exception("%s process group name includes not orthogonal processes %s"
                    % (self.process_group_name, ", ".join(filtered_processes.names)))
            elif len(filtered_processes) == 1:
                process = filtered_processes[0]
                if process not in self.processes_datasets:
                    self.processes_datasets[process] = []
                self.processes_datasets[process].append(dataset)
                self.datasets_to_run.append(dataset)

        # get QCD regions when requested
        self.qcd_regions = None
        if self.do_qcd:
            wp = self.qcd_wp if self.qcd_wp != law.NO_STR else ""
            self.qcd_regions = self.config.get_qcd_regions(region=self.region, category=self.category,
                wp=wp, shape_region=self.shape_region, signal_region_wp=self.qcd_signal_region_wp,
                sym=self.qcd_sym_shape)

            # complain when no data is present
            if not any(dataset.process.isData for dataset in self.datasets):
                raise Exception("no real dataset passed for QCD estimation")

    def requires(self):
        """
        All requirements needed:

            * Histograms coming from the PrePlot task.

            * Number of total events coming from the MergeCategorizationStats task \
              (to normalize MC histograms).

            * If estimating QCD, FeaturePlot for the three additional QCD regions needed.

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
            if dataset.process.isData or not self.apply_weights:
                continue
            if dataset.get_aux("secondary_dataset", None):
                reqs["stats"][dataset.name] = MergeCategorizationStats.vreq(self,
                    dataset_name=dataset.get_aux("secondary_dataset"))
            else:
                reqs["stats"][dataset.name] = MergeCategorizationStats.vreq(self,
                    dataset_name=dataset.name)

        if self.do_qcd:
            reqs["qcd"] = {
                key: self.req(self, region_name=region.name, blinded=False, hide_data=False,
                    do_qcd=False, stack=True, save_root=True, save_pdf=True, save_yields=False,
                    remove_horns=False,_exclude=["feature_tags", "shape_region",
                    "qcd_category_name", "qcd_sym_shape", "qcd_signal_region_wp"])
                for key, region in self.qcd_regions.items()
            }
            if self.qcd_category_name != "default":
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

        return reqs

    def get_output_postfix(self, feature, key="pdf"):
        """
        :return: string to be included in the output filenames
        :rtype: str
        """
        postfix = super(FeaturePlot, self).get_output_postfix()
        if self.process_group_name != self.default_process_group_name:
            postfix += "__pg_" + self.process_group_name
        if self.do_qcd:
            postfix += "__qcd"
        if self.hide_data:
            postfix += "__nodata"
        elif self.blinded and key not in ("root", "yields"):
            postfix += "__blinded"
        if self.stack and key not in ("root", "yields"):
            postfix += "__stack"
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
                    prefix, feature.name, self.get_output_postfix(feature, key), ext)))
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
        hist.hist_type = "signal"
        hist.legend_style = "l"
        hist.SetLineColor(color)

    def setup_background_hist(self, hist, color):
        """
        Method to apply background format to an histogram
        """
        hist.hist_type = "background"
        if self.stack:
            hist.SetLineColor(ROOT.kBlack)
            hist.SetLineWidth(1)
            hist.SetFillColor(color)
            hist.legend_style = "f"
        else:
            hist.SetLineColor(color)
            hist.legend_style = "l"

    def setup_data_hist(self, hist, color):
        """
        Method to apply data format to an histogram
        """
        hist.legend_style = "lp"
        hist.hist_type = "data"
        hist.SetMarkerStyle(20)
        hist.SetMarkerColor(color)
        hist.SetLineColor(color)
        hist.SetBinErrorOption((ROOT.TH1.kPoisson if self.stack else ROOT.TH1.kNormal))

    def get_systematics(self):
        """
        Method to extract all normalization systematics from the KLUB files.
        It considers the processes given by the process_group_name and their parents.
        """
        # systematics
        systematics = {}
        if self.plot_systematics:
            all_signal_names = []
            all_background_names = []
            for p in self.config.processes:
                if p.isSignal:
                    all_signal_names.append(p.get_aux("llr_name")
                        if p.get_aux("llr_name", None) else p.name)
                elif not p.isData:
                    all_background_names.append(p.get_aux("llr_name")
                        if p.get_aux("llr_name", None) else p.name)

            from cmt.analysis.systReader import systReader
            syst_folder = "files/systematics/"
            syst = systReader(self.retrieve_file(syst_folder + "systematics_{}.cfg".format(
                self.config.year)), all_signal_names, all_background_names, None)
            syst.writeOutput(False)
            syst.verbose(False)

            channel = self.config.get_channel_from_region(self.region)
            if(channel == "mutau"):
                syst.addSystFile(self.retrieve_file(syst_folder
                    + "systematics_mutau_%s.cfg" % self.config.year))
            elif(channel == "etau"):
                syst.addSystFile(self.retrieve_file(syst_folder
                    + "systematics_etau_%s.cfg" % self.config.year))
            syst.addSystFile(self.retrieve_file(syst_folder + "syst_th.cfg"))
            syst.writeSystematics()
            for isy, syst_name in enumerate(syst.SystNames):
                if "CMS_scale_t" in syst.SystNames[isy] or "CMS_scale_j" in syst.SystNames[isy]:
                    continue
                for dataset in self.datasets:
                    process = dataset.process
                    while True:
                        process_name = (process.get_aux("llr_name")
                            if process.get_aux("llr_name", None) else p.name)
                        if process_name in syst.SystProcesses[isy]:
                            iproc = syst.SystProcesses[isy].index(process_name)
                            systVal = syst.SystValues[isy][iproc]
                            if dataset.name not in systematics:
                                systematics[dataset.name] = []
                            systematics[dataset.name].append((syst_name, eval(systVal) - 1))
                            break
                        elif process.parent_process:
                            process=self.config.processes.get(process.parent_process)
                        else:
                            break
            for dataset_name in systematics:
                systematics[dataset_name] = math.sqrt(sum([x[1] * x[1]
                    for x in systematics[dataset_name]]))
        return systematics

    def plot(self, feature):
        """
        Performs the actual plotting.
        """
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
            for ibin in range(1, qcd_hist.GetNbinsX() + 1):
                if qcd_hist.GetBinContent(ibin) < bin_limit:
                    qcd_hist.SetBinContent(ibin, 1.e-6)

            return qcd_hist

        def get_integral_and_error(hist):
            error = c_double(0.)
            integral = hist.IntegralAndError(0, hist.GetNbinsX() + 1, error)
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

        binning_args, y_axis_adendum = self.get_binning(feature)
        x_title = (str(feature.get_aux("x_title"))
            + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
        y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
        hist_title = "; %s; %s" % (x_title, y_title)

        # qcd shape files
        qcd_shape_files = None
        if self.do_qcd:
            qcd_shape_files = {}
            for key, region in self.qcd_regions.items():
                if self.qcd_category_name != "default":
                    my_feature = (feature.name if "shape" in key or key == "os_inviso"
                        else self.qcd_feature)
                else:
                    my_feature = feature.name
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
            qcd_hist.cmt_scale = 1.
            qcd_hist.cmt_process_name = "qcd"
            qcd_hist.process_label = "QCD"
            qcd_hist.SetTitle("QCD")
            self.setup_background_hist(qcd_hist, ROOT.kYellow)
            background_hists.append(qcd_hist)
            all_hists.append(qcd_hist)

        if not self.hide_data:
            all_hists += data_hists

        bkg_histo = None
        data_histo = None
        if not self.stack:
            for hist in all_hists:
                scale = 1. / (hist.Integral() or 1.)
                hist.Scale(scale)
                hist.scale = scale
            draw_hists = all_hists[::-1]
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
            draw_hists = [background_stack] + signal_hists[::-1]
            if not self.hide_data:
                draw_hists.extend(data_hists[::-1])
                for hist in data_hists:
                    if not data_histo:
                        data_histo = hist.Clone()
                    else:
                        data_histo.Add(hist.Clone())

        entries = [(hist, hist.process_label, hist.legend_style) for hist in all_hists]
        n_entries = len(entries)
        if n_entries <= 4:
            n_cols = 1
        elif n_entries <= 8:
            n_cols = 2
        else:
            n_cols = 3
        col_width = 0.125
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

        dummy_hist = ROOT.TH1F(randomize("dummy"), hist_title, *binning_args)
        # Draw
        if self.hide_data or len(data_hists) == 0 or not self.stack:
            c = Canvas()
            if self.log_y:
                c.SetLogy()
        else:
            c = RatioCanvas()
            dummy_hist.GetXaxis().SetLabelOffset(100)
            dummy_hist.GetXaxis().SetTitleOffset(100)
            c.get_pad(1).cd()
            if self.log_y:
                c.get_pad(1).SetLogy()

        # r.setup_hist(dummy_hist, pad=c.get_pad(1))
        r.setup_hist(dummy_hist)
        dummy_hist.GetYaxis().SetMaxDigits(4)
        maximum = max([hist.GetMaximum() for hist in draw_hists])
        if self.log_y:
            dummy_hist.SetMaximum(100 * maximum)
            dummy_hist.SetMinimum(0.0011)
        else:
            dummy_hist.SetMaximum(1.1 * maximum)
            dummy_hist.SetMinimum(0.001)

        draw_labels = get_labels(upper_right="")

        dummy_hist.Draw()
        for ih, hist in enumerate(draw_hists):
            option = "HIST,SAME" if hist.hist_type != "data" else "PEZ,SAME"
            hist.Draw(option)

        for label in draw_labels:
            label.Draw("same")
        legend.Draw("same")
        if not (self.hide_data or len(data_hists) == 0 or len(background_hists) == 0 or not self.stack):
            dummy_ratio_hist = ROOT.TH1F(randomize("dummy"), hist_title, *binning_args)
            r.setup_hist(dummy_ratio_hist, pad=c.get_pad(2),
                props={"Minimum": 0.25, "Maximum": 1.75})
            r.setup_y_axis(dummy_ratio_hist.GetYaxis(), pad=c.get_pad(2),
                props={"Ndivisions": 3})
            dummy_ratio_hist.GetYaxis().SetTitle("Data / MC")
            dummy_ratio_hist.GetXaxis().SetTitleOffset(3)
            dummy_ratio_hist.GetYaxis().SetTitleOffset(1.22)

            data_graph = hist_to_graph(data_histo, remove_zeros=False, errors=True,
                asymm=True, overflow=False, underflow=False,
                attrs=["cmt_process_name", "cmt_hist_type", "cmt_legend_style"])
            bkg_graph = hist_to_graph(bkg_histo, remove_zeros=False, errors=True,
                asymm=True, overflow=False, underflow=False,
                attrs=["cmt_process_name", "cmt_hist_type", "cmt_legend_style"])

            ratio_graph = ROOT.TGraphAsymmErrors(binning_args[0])
            mc_unc_graph = ROOT.TGraphErrors(binning_args[0])
            r.setup_graph(ratio_graph, props={"MarkerStyle": 20, "MarkerSize": 0.5})
            r.setup_graph(mc_unc_graph, props={"FillStyle": 3004, "LineColor": 0,
                "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kGray + 2})
            if self.plot_systematics:
                syst_graph = hist_to_graph(bkg_histo_syst, remove_zeros=False, errors=True,
                    asymm=True, overflow=False, underflow=False,
                    attrs=["cmt_process_name", "cmt_hist_type", "cmt_legend_style"])
                syst_unc_graph = ROOT.TGraphErrors(binning_args[0])
                r.setup_graph(syst_unc_graph, props={"FillStyle": 3005, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kRed + 2})
                all_unc_graph = ROOT.TGraphErrors(binning_args[0])
                r.setup_graph(all_unc_graph, props={"FillStyle": 3007, "LineColor": 0,
                    "MarkerColor": 0, "MarkerSize": 0., "FillColor": ROOT.kBlue + 2})

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
            dummy_ratio_hist.Draw()
            mc_unc_graph.Draw("2,SAME")
            if not self.hide_data:
                ratio_graph.Draw("PEZ,SAME")
            if self.plot_systematics:
                # syst_unc_graph.Draw("2,SAME")
                all_unc_graph.Draw("2,SAME")

            lines = []
            for y in [0.5, 1.0, 1.5]:
                l = ROOT.TLine(binning_args[1], y, binning_args[2], y)
                r.setup_line(l, props={"NDC": False, "LineStyle": 3, "LineWidth": 1,
                    "LineColor": 1})
                lines.append(l)
            for line in lines:
                line.Draw("same")

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

            hist_dir = f.mkdir("histograms")
            hist_dir.cd()
            for hist in all_hists:
                hist.Write(hist.cmt_process_name)

            if self.plot_systematics:
                for syst_dir, shape_hists in self.histos["shape"].items():
                    for hist in shape_hists:
                        hist.Write("%s_%s" % (hist.cmt_process_name, syst_dir))
            f.Close()

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        """
        Splits processes into data, signal and background. Creates histograms from each process
        loading them from the input files. Scales the histograms and applies the correct format
        to them.
        """
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()

        lumi = self.config.lumi_pb

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        self.signal_names = [p.name for p in self.processes_datasets.keys() if p.isSignal]
        self.background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal]

        if self.plot_systematics:
            systematics = self.get_systematics()

        if self.fixed_colors:
            colors = list(range(2, 2 + len(self.processes_datasets.keys())))

        nevents = {}
        for iproc, (process, datasets) in enumerate(self.processes_datasets.items()):
            if not process.isData and self.apply_weights:
                for dataset in datasets:
                    inp = inputs["stats"][dataset.name]
                    with open(inp.path) as f:
                        stats = json.load(f)
                        # nevents += stats["nevents"]
                        nevents[dataset.name] = stats["nweightedevents"]

        for feature in self.features:
            self.histos = {"background": [], "signal": [], "data": [], "all": []}

            binning_args, y_axis_adendum = self.get_binning(feature)
            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
            hist_title = "; %s; %s" % (x_title, y_title)

            systs_directions = [("central", "")]
            if self.plot_systematics:
                self.histos["bkg_histo_syst"] = ROOT.TH1D(
                    randomize("syst"), hist_title, *binning_args)
                self.histos["shape"] = {}
                shape_systematics = self.get_systs(feature, True) \
                    + self.config.get_weights_systematics(self.config.weights[self.category.name], True)

                directions = ["up", "down"]
                systs_directions += list(itertools.product(shape_systematics, directions))

            for (syst, d) in systs_directions:
                feature_name = feature.name if syst == "central" else "%s_%s_%s" % (
                    feature.name, syst, d)
                if syst != "central":
                    self.histos["shape"]["%s_%s" % (syst, d)] = []
                for iproc, (process, datasets) in enumerate(self.processes_datasets.items()):
                    if syst != "central" and process.isData:
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
                                histo = copy(rootfile.Get(feature_name))
                                rootfile.Close()
                                dataset_histo.Add(histo)
                            if not process.isData and self.apply_weights:
                                if nevents[dataset.name] != 0:
                                    dataset_histo.Scale(dataset.xs * lumi / nevents[dataset.name])
                                    scaling = dataset.get_aux("scaling", None)
                                    if scaling:
                                        print("Scaling {} histo by {} +- {}".format(
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
                                dataset_histo_syst.SetBinError(ibin,
                                    float(dataset_histo.GetBinContent(ibin))\
                                        * systematics[dataset.name]
                                )
                            self.histos["bkg_histo_syst"].Add(dataset_histo_syst)

                    yield_error = c_double(0.)
                    process_histo.cmt_yield = process_histo.IntegralAndError(0,
                        process_histo.GetNbinsX() + 1, yield_error)
                    process_histo.cmt_yield_error = yield_error.value

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
                        elif process.isData:
                            self.setup_data_hist(process_histo, color)
                            self.histos["data"].append(process_histo)
                        else:
                            self.setup_background_hist(process_histo, color)
                            self.histos["background"].append(process_histo)
                        if not process.isData: #or not self.hide_data:
                           self.histos["all"].append(process_histo)
                    else:
                        self.histos["shape"]["%s_%s" % (syst, d)].append(process_histo)
            self.plot(feature)
