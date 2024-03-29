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


from analysis_tools import ObjectCollection
from analysis_tools.utils import (
    import_root, create_file_dir, join_root_selection, randomize, randomize
)
from plotting_tools.root import get_labels, Canvas, RatioCanvas
from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, ConfigTaskWithCategory
)

from cmt.base_tasks.preprocessing import Preprocess, MergeCategorization, MergeCategorizationStats

cmt_style = r.styles.copy("default", "cmt_style")
cmt_style.style.ErrorX = 0
cmt_style.x_axis.TitleOffset = 1.22
cmt_style.y_axis.TitleOffset = 1.48
cmt_style.legend.TextSize = 20
cmt_style.style.legend_dy = 0.035
cmt_style.style.legend_y2 = 0.93

EMPTY = -1.e5

class BasePlotTask(ConfigTaskWithCategory):
    base_category_name = luigi.Parameter(default="base", description="the name of the "
        "base category with the initial selection, default: base")
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

    def get_systematics(self, feature):
        return feature.systematics

    def get_systs(self, feature, isMC):
        systs = []
        if not isMC:
            return systs
        for syst in self.get_systematics(feature):
            systs.append(syst)
        return systs

    def get_output_postfix(self):
        postfix = ""
        if self.region:
            postfix += "__" + self.region.name
        if not self.apply_weights:
            postfix += "__noweights"
        return postfix


class PrePlot(DatasetTaskWithCategory, BasePlotTask, law.LocalWorkflow, HTCondorWorkflow):

    def create_branch_map(self):
        return self.n_files_after_merging

    def workflow_requires(self):
        return {"data": MergeCategorization.vreq(self, workflow="local")}

    def requires(self):
        return MergeCategorization.vreq(self, workflow="local", branch=self.branch)

    def output(self):
        return self.local_target("data{}_{}.root".format(
            self.get_output_postfix(), self.branch))

    def get_weight(self, category, **kwargs):
        if self.config.processes.get(self.dataset.process.name).isData:
            return "1"
        else:
            #print self.config.join_selection_channels(self.config.weights.channels_mult)
            # return self.config.weights.channels_mult["tautau"]
            if category in self.config.weights.channels:
            # for channel in self.config.channels:
                return " * ".join(self.config.weights.channels[category])
        return self.config.weights.default

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        ROOT = import_root()

        isMC = self.dataset.process.isMC
        directions = ["up", "down"]

        # prepare inputs and outputs
        inp = self.input().targets[self.branch].path
        outp = self.output().path

        df = ROOT.RDataFrame(self.tree_name, inp)

        if self.region_name != law.NO_STR:
            sel = self.config.regions.get(self.region_name).selection
            df = df.Filter(sel)

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

            systs = ["central"] + self.get_systs(feature, isMC)

            systs_directions = [("central", "")]
            if isMC:
                systs_directions += list(itertools.product(systs, directions))

            for syst_name, direction in systs_directions:
                if syst_name != "central":
                    syst = self.config.systematics.get(syst_name)
                else:
                    syst = self.config.systematics.get(feature.central)

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
                        df.Define(
                            "weight", "{}".format(self.get_weight(self.category.name))
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
                    dataset.process.parent_process))
            return processes

        for dataset in self.datasets:
            processes = get_processes(dataset=dataset)
            filtered_processes = ObjectCollection()
            for process in processes:
                if process.name in self.config.process_group_names[self.process_group_name]:
                    filtered_processes.append(process)
            # print dataset.name, [process.name for process in processes]
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
        reqs = {}
        reqs["data"] = OrderedDict(
            ((dataset.name, category.name), PrePlot.vreq(self,
                dataset_name=dataset.name, category_name=self.get_data_category(category).name))
            for dataset, category in itertools.product(
                self.datasets_to_run, self.expand_category())
        )
        reqs["stats"] = OrderedDict(
            (dataset.name, MergeCategorizationStats.vreq(self, dataset_name=dataset.name))
            for dataset in self.datasets_to_run if not dataset.process.isData
        )

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
        return ConfigTaskWithCategory.complete(self)

    @law.decorator.notify
    @law.decorator.safe_output
    def run(self):
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        # create root tchains for inputs
        inputs = self.input()

        lumi = self.config.lumi_pb

        def setup_signal_hist(hist, color):
            hist.hist_type = "signal"
            hist.legend_style = "l"
            hist.SetLineColor(color)

        def setup_background_hist(hist, color):
            hist.hist_type = "background"
            if self.stack:
                hist.SetLineColor(ROOT.kBlack)
                hist.SetLineWidth(1)
                hist.SetFillColor(color)
                hist.legend_style = "f"
            else:
                hist.SetLineColor(color)
                hist.legend_style = "l"

        def setup_data_hist(hist, color):
            hist.legend_style = "lp"
            hist.hist_type = "data"
            hist.SetMarkerStyle(20)
            hist.SetMarkerColor(color)
            hist.SetLineColor(color)
            hist.SetBinErrorOption((ROOT.TH1.kPoisson if self.stack else ROOT.TH1.kNormal))

        data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        signal_names = [p.name for p in self.processes_datasets.keys() if p.isSignal]
        background_names = [p.name for p in self.processes_datasets.keys()
            if not p.isData and not p.isSignal]

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
            syst_folder = os.environ["CMT_BASE"] + "/cmt/files/systematics/"
            syst = systReader(syst_folder + "systematics_{}.cfg".format(self.config.year),
                all_signal_names, all_background_names, None)
            syst.writeOutput(False)
            syst.verbose(False)

            channel = self.config.get_channel_from_region(self.region)
            if(channel == "mutau"):
                syst.addSystFile(syst_folder + "systematics_mutau_%s.cfg" % self.config.year)
            elif(channel == "etau"):
                syst.addSystFile(syst_folder + "systematics_etau_%s.cfg" % self.config.year)
            syst.addSystFile(syst_folder + "syst_th.cfg")
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
                            process=self.config.processes.get(dataset.process.parent_process)
                        else:
                            break
            for dataset_name in systematics:
                systematics[dataset_name] = math.sqrt(sum([x[1] * x[1]
                    for x in systematics[dataset_name]]))

        # helper to extract the qcd shape in a region
        def get_qcd(region, files, bin_limit=0.):
            d_hist = files[region].Get("histograms/" + data_names[0])
            if not d_hist:
                raise Exception("data histogram '{}' not found for region '{}' in tfile {}".format(
                    data_names[0], region, files[region]))

            b_hists = []
            for b_name in background_names:
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
            error = ROOT.Double()
            integral = hist.IntegralAndError(0, hist.GetNbinsX() + 1, error)
            error = float(error)
            compatible = True if integral <= 0 else False
            # compatible = True if integral - error <= 0 else False
            return integral, error, compatible

        for feature in self.features:
            binning_args, y_axis_adendum = self.get_binning(feature)

            x_title = (str(feature.get_aux("x_title"))
                + (" [%s]" % feature.get_aux("units") if feature.get_aux("units") else ""))
            y_title = ("Events" if self.stack else "Normalized Events") + y_axis_adendum
            hist_title = "; %s; %s" % (x_title, y_title)
            background_hists = []
            signal_hists = []
            data_hists = []
            all_hists = []
            colors = []            
            if self.plot_systematics:
                bkg_histo_syst = ROOT.TH1D("syst", hist_title, *binning_args)
            for process, datasets in self.processes_datasets.items():
                feature_name = feature.name  # FIXME: What about systs?
                process_histo = ROOT.TH1D(str(process.label), hist_title, *binning_args)
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
                    if not process.isData:
                        nevents = 0
                        inp = inputs["stats"][dataset.name]
                        with open(inp.path) as f:
                            stats = json.load(f)
                            # nevents += stats["nevents"]
                            nevents += stats["nweightedevents"]
                        if nevents != 0:
                            dataset_histo.Scale(dataset.xs * lumi / nevents)
                    process_histo.Add(dataset_histo)
                    if self.plot_systematics and not process.isData and not process.isSignal:
                        dataset_histo_syst = dataset_histo.Clone()
                        for ibin in range(1, dataset_histo_syst.GetNbinsX() + 1):
                            dataset_histo_syst.SetBinError(ibin,
                                float(dataset_histo.GetBinContent(ibin)) * systematics[dataset.name]
                            )
                        bkg_histo_syst.Add(dataset_histo_syst)

                yield_error = ROOT.Double()
                process_histo.cmt_yield = process_histo.IntegralAndError(0,
                    process_histo.GetNbinsX() + 1, yield_error)
                process_histo.cmt_yield_error = float(yield_error)
                if process.isSignal:
                    setup_signal_hist(process_histo, ROOT.TColor.GetColor(*process.color)) # FIXME include custom colors
                    signal_hists.append(process_histo)
                elif process.isData:
                    setup_data_hist(process_histo, ROOT.TColor.GetColor(*process.color))
                    data_hists.append(process_histo)
                else:
                    setup_background_hist(process_histo, ROOT.TColor.GetColor(*process.color))
                    background_hists.append(process_histo)
                if not process.isData: #or not self.hide_data:
                    all_hists.append(process_histo)

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
                    qcd_shape_files[key] = ROOT.TFile.Open(inputs["qcd"][key]["root"].targets[my_feature].path)

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
                yield_error = ROOT.Double()
                qcd_hist.cmt_yield = qcd_hist.IntegralAndError(
                    0, qcd_hist.GetNbinsX() + 1, yield_error)
                qcd_hist.cmt_yield_error = float(yield_error)
                qcd_hist.cmt_scale = 1.
                qcd_hist.cmt_process_name = "qcd"
                qcd_hist.process_label = "QCD"
                qcd_hist.SetTitle("QCD")
                setup_background_hist(qcd_hist, ROOT.kYellow)
                background_hists.append(qcd_hist)
                background_names.append("qcd")
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
                background_stack = ROOT.THStack(randomize("stack"), "stack")
                for hist in background_hists[::-1]:
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
            if self.hide_data or len(data_hists) == 0:
                c = Canvas()
            else:
                c = RatioCanvas()
                dummy_hist.GetXaxis().SetLabelOffset(100)
                dummy_hist.GetXaxis().SetTitleOffset(100)
                c.get_pad(1).cd()

            r.setup_hist(dummy_hist, pad=c.get_pad(1))
            dummy_hist.GetYaxis().SetMaxDigits(4)
            maximum = max([hist.GetMaximum() for hist in draw_hists])
            dummy_hist.SetMaximum(1.1 * maximum)
            dummy_hist.SetMinimum(0.001)  # FIXME in case of log scale
            draw_labels = get_labels(upper_right="")

            dummy_hist.Draw()
            for hist in draw_hists:
                option = "HIST,SAME" if hist.hist_type != "data" else "PEZ,SAME"
                hist.Draw(option)

            for label in draw_labels:
                label.Draw()
            legend.Draw()

            if not (self.hide_data or len(data_hists) == 0):
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
                r.setup_graph(ratio_graph)
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
                    x, d, b = ROOT.Double(), ROOT.Double(), ROOT.Double()
                    data_graph.GetPoint(i, x, d)
                    bkg_graph.GetPoint(i, x, b)
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
                f.Close()
