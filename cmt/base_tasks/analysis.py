# coding: utf-8

"""
Analysis tasks.
"""

__all__ = []

import os
from shutil import move
from copy import deepcopy as copy
import json
import math
import itertools
from collections import OrderedDict
import tabulate
import numpy as np

import law
import luigi

from analysis_tools.utils import (
    import_root, create_file_dir, randomize
)

from cmt.base_tasks.base import ConfigTaskWithCategory
from cmt.base_tasks.plotting import FeaturePlot

ROOT = import_root()

directions = ["up", "down"]


class CreateDatacards(FeaturePlot):
    """
    Task that creates datacards for its use inside the combine framework

    :param automcstats: Value used for autoMCStats inside the datacard, -1 to avoid using it.
    :type automcstats: int

    :param additional_lines: Additional lines to write at the end of the datacard.
    :type additional_lines: list of `str`

    :param fit_models: filename with fit models to use in the fit
    :type fit_models: str

    :param counting: whether the datacard should consider a counting experiment
    :type counting: bool
    """

    automcstats = luigi.IntParameter(default=10, description="value used for autoMCStats inside "
        "the datacard, -1 to avoid using it, default: 10")
    additional_lines = law.CSVParameter(default=(), description="addtional lines to write at the "
        "end of the datacard")
    propagate_syst_qcd = luigi.BoolParameter(default=False, description="whether to propagate"
        "systematics to qcd background, default: False")
    fit_models = luigi.Parameter(default="", description="filename with fit models to use "
        "in the fit, default: none (binned fit)")
    counting = luigi.BoolParameter(default=False, description="whether the datacard should consider "
        "a counting experiment, default: False")

    additional_scaling = luigi.DictParameter(description="dict with scalings to be "
        "applied to processes in the datacard, ONLY IMPLEMENTED FOR PARAMETRIC FITS, default: None")
    additional_scaling = {"dummy": 1}  # Temporary fix, the DictParameter fails when empty

    norm_syst_threshold = 0.01
    norm_syst_threshold_sym = 0.01

    def __init__(self, *args, **kwargs):
        super(CreateDatacards, self).__init__(*args, **kwargs)

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        if len(self.data_names) > 1:
            raise ValueError("Only 1 data process can be provided inside the process group")
        self.non_data_names = [p.name for p in self.processes_datasets.keys() if not p.isData]

        if self.do_qcd:
            self.non_data_names.append("qcd")

        if self.fit_models:
            import yaml
            from cmt.utils.yaml_utils import ordered_load
            with open(self.retrieve_file("config/{}.yaml".format(self.fit_models))) as f:
                self.models = ordered_load(f, yaml.SafeLoader)

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        if not self.fit_models and not self.counting:
            return FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False, normalize_signals=False)
        else:  # FIXME allow counting datacards starting from FeaturePlot
            reqs = {"fits": {}, "inspections": {}}
            self.model_processes = []
            for model, fit_params in self.models.items():
                self.model_processes.append(fit_params["process_name"])
                params = ", ".join([f"{param}='{value}'"
                    for param, value in fit_params.items() if param != "fit_parameters"])
                if "fit_parameters" in fit_params:
                    params += ", fit_parameters={" + ", ".join([f"'{param}': '{value}'"
                    for param, value in fit_params["fit_parameters"].items()]) + "}"
                reqs["fits"][fit_params["process_name"]] = eval(
                    f"Fit.vreq(self, {params}, _exclude=['include_fit'])")
                reqs["inspections"][fit_params["process_name"]] = eval(
                    f"InspectFitSyst.vreq(self, {params}, _exclude=['include_fit'])")

            # In order to have a workspace with data_obs, we replicate the first fit
            # (just in case the x_range is defined) for the available data process
            assert(self.data_names)
            fit_params = copy(list(self.models.values())[0])
            fit_params["process_name"] = self.data_names[0]
            params = ", ".join([f"{param}='{value}'"
                for param, value in fit_params.items() if param != "fit_parameters"])
            if "fit_parameters" in fit_params:
                params += ", fit_parameters={" + ", ".join([f"'{param}': '{value}'"
                for param, value in fit_params["fit_parameters"].items()]) + "}"
            reqs["fits"]["data_obs"] = eval(f"Fit.vreq(self, {params}, _exclude=['include_fit'])")
            return reqs

    def get_output_postfix(self, process_group_name=None):
        if process_group_name:
            self.process_group_name = process_group_name
        process_group_name = "" if self.process_group_name == "default" else "_{}".format(
            self.process_group_name)
        region_name = "" if not self.region else "_{}".format(self.region.name)
        return process_group_name + region_name

    def output(self):
        """
        Returns, per feature, one txt storing the datacard and its corresponding root file
        storing the histograms
        """
        keys = ["txt"]
        if not self.counting:
            keys.append("root")
        return {
            feature.name: {
                key: self.local_target("{}{}.{}".format(feature.name, self.get_output_postfix(),
                    key))
                for key in keys
            }
            for feature in self.features
        }
        return reqs

    def get_norm_systematics(self):
        if self.plot_systematics:
            return self.config.get_norm_systematics(self.processes_datasets, self.region)
        return {}

    def get_norm_systematics_from_inspect(self, feature_name):
        # structure: systematics[syst_name][process_name] = syst_value
        systematics = {}
        for name in self.non_data_names:
            path = self.input()["inspections"][name][feature_name]["json"].path
            with open(path) as f:
                d = json.load(f)
            for syst_name, values in d.items():
                up_value = values["integral"]["up"]
                down_value = values["integral"]["down"]
                if up_value == -999 or down_value == -999:
                    continue
                if abs(up_value) > self.norm_syst_threshold or \
                        abs(down_value) > self.norm_syst_threshold:
                    if syst_name not in systematics:
                        systematics[syst_name] = {}
                    # symmetric?
                    if abs(up_value + down_value) < self.norm_syst_threshold_sym:
                        systematics[syst_name][name] = "{:.2f}".format(1 + up_value)
                    else:
                        systematics[syst_name][name] = "{:.2f}/{:.2f}".format(1 + up_value,
                            1 + down_value)
        return systematics

    def write_datacard(self, yields, feature, norm_systematics, shape_systematics, *args):
        n_dashes = 50

        region_name = "" if not self.region else "_{}".format(self.region.name)
        bin_name = "{}{}".format(feature.name, region_name)

        table = []
        table.append(["bin", ""] + [bin_name for i in range(len(self.non_data_names))])
        table.append(["process", ""] + self.non_data_names)

        sig_counter = 0
        bkg_counter = 1
        line = ["process", ""]
        for p_name in self.non_data_names:
            try:
                if self.config.processes.get(p_name).isSignal:
                    line.append(sig_counter)
                    sig_counter -= 1
                else:
                    line.append(bkg_counter)
                    bkg_counter += 1
            except ValueError:  # qcd coming from do_qcd
                line.append(bkg_counter)
                bkg_counter += 1
        table.append(line)
        table.append(["rate", ""] + [yields[p_name] for p_name in self.non_data_names])

        # normalization systematics
        for norm_syst in norm_systematics:
            line = [norm_syst, "lnN"]
            for p_name in self.non_data_names:
                if p_name in norm_systematics[norm_syst]:
                    line.append(norm_systematics[norm_syst][p_name])
                else:
                    line.append("-")
            table.append(line)

        # shape systematics
        for shape_syst in shape_systematics:
            line = [shape_syst, "shape"]
            for p_name in self.non_data_names:
                if p_name in shape_systematics[shape_syst]:
                    line.append(1)
                else:
                    line.append("-")
            table.append(line)

        fancy_table = tabulate.tabulate(table, tablefmt="plain").split("\n")

        with open(create_file_dir(self.output()[feature.name]["txt"].path), "w+") as f:
            for letter in ["i", "j", "k"]:
                f.write("%smax  *\n" % letter)

            f.write(n_dashes * "-" + "\n")

            f.write("shapes  *  {0}  {1}  $PROCESS  $PROCESS_$SYSTEMATIC\n".format(bin_name,
                self.output()[feature.name]["root"].path))

            f.write(n_dashes * "-" + "\n")

            f.write("{:<11}  {}\n".format("bin", bin_name))
            f.write("observation  -1\n")

            f.write(n_dashes * "-" + "\n")
            for i in range(4):
                f.write(fancy_table[i] + "\n")

            f.write(n_dashes * "-" + "\n")
            for i in range(4, len(fancy_table)):
                f.write(fancy_table[i] + "\n")

            if self.automcstats != -1:
                f.write("*  autoMCStats  %s \n" % self.automcstats)

            for arg in args:
                f.write(arg + "\n")

    def write_shape_datacard(self, feature, norm_systematics, shape_systematics, *args):
        n_dashes = 50

        region_name = "" if not self.region else "_{}".format(self.region.name)
        bin_name = "{}{}".format(feature.name, region_name)

        shapes_table = []
        for name in self.non_data_names:
            shapes_table.append(["shapes", name, bin_name, self.output()[feature.name]["root"].path,
                "workspace_{0}:model_{0}".format(name)])
        for name in self.data_names:
            if name in self.model_processes:
                # We are extracting the background from the data, so let's label it as background
                # but let's be sure background is not in the process_group_name
                assert not "background" in self.processes_datasets.keys()
                shapes_table.append(["shapes", "background", bin_name, self.output()[feature.name]["root"].path,
                    "workspace_{0}:model_{0}".format(name)])

        # Include shape for the data
        shapes_table.append(["shapes", "data_obs", bin_name, self.output()[feature.name]["root"].path,
            "workspace_data_obs:data_obs"])

        table = []
        process_names = self.non_data_names
        for name in self.data_names:
            if name in self.model_processes:
                process_names.append("background")

        table.append(["bin", ""] + [bin_name for i in range(len(process_names))])
        table.append(["process", ""] + process_names)

        sig_counter = 0
        bkg_counter = 1
        line = ["process", ""]
        for p_name in process_names:
            try:
                if self.config.processes.get(p_name).isSignal:
                    line.append(sig_counter)
                    sig_counter -= 1
                else:
                    line.append(bkg_counter)
                    bkg_counter += 1
            except ValueError:  # qcd coming from do_qcd or background coming from data
                line.append(bkg_counter)
                bkg_counter += 1
        table.append(line)

        rate_line = ["rate", ""]
        for p_name in process_names:
            if p_name == "background" and self.data_names[0] in self.model_processes:
                # assuming it comes from data, may cause problems in certain setups
                rate_line.append(1)
            else:
                filename = self.input()["fits"][p_name][feature.name]["json"].path
                with open(filename) as f:
                    d = json.load(f)
                rate = d[""]["integral"]
                if self.additional_scaling.get(p_name, False):
                    rate *= float(self.additional_scaling.get(p_name))
                rate_line.append(rate)
        table.append(rate_line)

        # normalization systematics
        for norm_syst in norm_systematics:
            line = [norm_syst, "lnN"]
            for p_name in self.non_data_names:
                if p_name in norm_systematics[norm_syst]:
                    line.append(norm_systematics[norm_syst][p_name])
                else:
                    line.append("-")
            table.append(line)

        # # # shape systematics
        # # for shape_syst in shape_systematics:
            # # line = [shape_syst, "shape"]
            # # for p_name in self.non_data_names:
                # # if p_name in shape_systematics[shape_syst]:
                    # # line.append(1)
                # # else:
                    # # line.append("-")
            # # table.append(line)

        fancy_shapes_table = tabulate.tabulate(shapes_table, tablefmt="plain").split("\n")
        fancy_table = tabulate.tabulate(table, tablefmt="plain").split("\n")

        with open(create_file_dir(self.output()[feature.name]["txt"].path), "w+") as f:
            for letter in ["i", "j", "k"]:
                f.write("%smax  *\n" % letter)

            f.write(n_dashes * "-" + "\n")

            for line in fancy_shapes_table:
                f.write(line + "\n")

            f.write(n_dashes * "-" + "\n")

            f.write("{:<11}  {}\n".format("bin", bin_name))
            f.write("observation  -1\n")

            f.write(n_dashes * "-" + "\n")
            for i in range(4):
                f.write(fancy_table[i] + "\n")

            f.write(n_dashes * "-" + "\n")
            for i in range(4, len(fancy_table)):
                f.write(fancy_table[i] + "\n")

            # if self.automcstats != -1:
                # f.write("*  autoMCStats  %s \n" % self.automcstats)

            for arg in args:
                f.write(arg + "\n")

    def get_process_rate_for_counting(self, p_name, feature):
        if p_name == "background" and self.data_names[0] in self.model_processes:
            # assuming it comes from data, may cause problems in certain setups
            return 1
        else:
            filename = self.input()["fits"][p_name][feature.name]["json"].path
            with open(filename) as f:
                d = json.load(f)
            rate = d[""]["integral"]
            if self.additional_scaling.get(p_name, False):
                rate *= float(self.additional_scaling.get(p_name))
            return rate

    def write_counting_datacard(self, feature, norm_systematics, shape_systematics, *args):
        n_dashes = 50

        region_name = "" if not self.region else "_{}".format(self.region.name)
        bin_name = "{}{}".format(feature.name, region_name)

        table = []
        process_names = self.non_data_names
        for name in self.data_names:
            if name in self.model_processes:
                process_names.append("background")

        table.append(["bin", ""] + [bin_name for i in range(len(process_names))])
        table.append(["process", ""] + process_names)

        sig_counter = 0
        bkg_counter = 1
        line = ["process", ""]
        for p_name in process_names:
            try:
                if self.config.processes.get(p_name).isSignal:
                    line.append(sig_counter)
                    sig_counter -= 1
                else:
                    line.append(bkg_counter)
                    bkg_counter += 1
            except ValueError:  # qcd coming from do_qcd or background coming from data
                line.append(bkg_counter)
                bkg_counter += 1
        table.append(line)

        rate_line = ["rate", ""]
        for p_name in process_names:
            rate_line.append(self.get_process_rate_for_counting(p_name, feature))
        table.append(rate_line)

        # normalization systematics
        for norm_syst in norm_systematics:
            line = [norm_syst, "lnN"]
            for p_name in self.non_data_names:
                if p_name in norm_systematics[norm_syst]:
                    line.append(norm_systematics[norm_syst][p_name])
                else:
                    line.append("-")
            table.append(line)

        # # # shape systematics
        # # for shape_syst in shape_systematics:
            # # line = [shape_syst, "shape"]
            # # for p_name in self.non_data_names:
                # # if p_name in shape_systematics[shape_syst]:
                    # # line.append(1)
                # # else:
                    # # line.append("-")
            # # table.append(line)

        fancy_table = tabulate.tabulate(table, tablefmt="plain").split("\n")

        with open(create_file_dir(self.output()[feature.name]["txt"].path), "w+") as f:
            for letter in ["i", "j", "k"]:
                f.write("%smax  *\n" % letter)

            f.write(n_dashes * "-" + "\n")

            f.write("{:<11}  {}\n".format("bin", bin_name))
            filename = self.input()["fits"]["data_obs"][feature.name]["json"].path
            with open(filename) as f_data:
                d = json.load(f_data)
            rate = d[""]["integral"]
            f.write("observation  %s\n" % rate)

            f.write(n_dashes * "-" + "\n")
            for i in range(4):
                f.write(fancy_table[i] + "\n")

            f.write(n_dashes * "-" + "\n")
            for i in range(4, len(fancy_table)):
                f.write(fancy_table[i] + "\n")

            # if self.automcstats != -1:
                # f.write("*  autoMCStats  %s \n" % self.automcstats)

            for arg in args:
                f.write(arg + "\n")


    def run(self):
        """
        Splits the processes into data and non-data. Per feature, loads the input histograms, 
        creates the output histograms and the datacard inside the txt file.
        """
        inputs = self.input()

        norm_systematics = self.get_norm_systematics()

        for feature in self.features:
            systs_directions = [("central", "")]
            shape_syst_list = self.get_systs(feature, True)
            systs_directions += list(itertools.product(shape_syst_list, directions))

            # Convert the shape systematics list to a dict with the systs as keys and a list of 
            # the processes affected by them (all non-data processes except the qcd if computed
            # in the code)
            shape_systematics = {shape_syst: [p_name for p_name in self.non_data_names]
                for shape_syst in shape_syst_list}

            if not self.fit_models and not self.counting:  # binned fits
                histos = {}
                tf = ROOT.TFile.Open(inputs["root"].targets[feature.name].path)
                for name in self.data_names:
                    histos[name] = copy(tf.Get("histograms/" + name))
                for name in self.non_data_names:
                    for (syst, d) in systs_directions:
                        if syst == "central":
                            name_to_save = name
                            name_from_featureplot = name
                        elif self.do_qcd and name == "qcd":
                            continue
                        else:
                            name_to_save = "%s_%s%s" % (name, syst, d.capitalize())
                            name_from_featureplot = "%s_%s_%s" % (name, syst, d)
                        histos[name_to_save] = copy(tf.Get("histograms/" + name_from_featureplot))
                tf.Close()

                yields = {name_central: histos[name_central].Integral()
                    for name_central in self.non_data_names}

                self.write_datacard(yields, feature,
                    norm_systematics, shape_systematics, *self.additional_lines)

                tf = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "RECREATE")
                for name, histo in histos.items():
                    if "data" in name:
                        name = "data_obs"
                    histo.Write(name)

                tf.Close()
            elif not self.counting:  # unbinned fits
                tf = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "RECREATE")
                for model, fit_params in self.models.items():
                    model_tf = ROOT.TFile.Open(
                        inputs["fits"][fit_params["process_name"]][feature.name]["root"].path)
                    w = model_tf.Get("workspace_" + fit_params["process_name"])
                    tf.cd()
                    w.Write()
                    model_tf.Close()
                # data_obs
                model_tf = ROOT.TFile.Open(inputs["fits"]["data_obs"][feature.name]["root"].path)
                w = model_tf.Get("workspace_" + self.data_names[0])
                tf.cd()
                w.Write("workspace_data_obs")
                model_tf.Close()
                tf.Close()

                norm_systematics_feature = self.get_norm_systematics_from_inspect(feature.name)
                norm_systematics_feature.update(norm_systematics)

                self.write_shape_datacard(feature, norm_systematics_feature, shape_systematics,
                    *self.additional_lines)

            else:  # counting experiment
                norm_systematics_feature = self.get_norm_systematics_from_inspect(feature.name)
                norm_systematics_feature.update(norm_systematics)

                self.write_counting_datacard(feature, norm_systematics_feature, shape_systematics,
                    *self.additional_lines)


class Fit(FeaturePlot):
    """
    Task that run fits over FeaturePlot histograms

    """

    method = luigi.ChoiceParameter(choices=("voigtian", "polynomial", "exponential", "powerlaw"),
        default="voigtian", description="fitting method to consider, default: voigtian")
    process_name = luigi.Parameter(default="signal", description="process name to consider, "
        "default: signal")
    x_range = law.CSVParameter(default=("1.2", "1.5"), description="range of the x axis to consider "
        "in the fit, default: 1.2-1.5")
    blind_range = law.CSVParameter(default=("-1", "-1"), description="range of the x axis to blind "
        "in the fit, default: none")
    fit_parameters = luigi.DictParameter(default={}, description="Initial values for the parameters "
        "involved in the fit, defaults depend on the method. Should be included as "
        "--fit-parameters '{\"mean\": \"(20, -100, 100)\"}'")

    normalize_signals = False

    def __init__(self, *args, **kwargs):
        super(Fit, self).__init__(*args, **kwargs)

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        return FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False,
            normalize_signals=False, save_yields=True)

    def output(self):
        """
        Returns, per feature, one json file storing the fit results and one root file
        storing the workspace
        """

        x0 = str(self.x_range[0]).replace(".", "p")
        x1 = str(self.x_range[1]).replace(".", "p")
        x = f"{x0}_{x1}".replace(" ", "")
        blind = ("" if float(self.blind_range[0]) == -1. and float(self.blind_range[1]) == -1.
            else "__blinded")
        region_name = "" if not self.region_name else "__{}".format(self.region_name)

        return {
            feature.name: {
                key: self.local_target("{}__{}__{}__{}_{}{}.{}".format(
                    feature.name, self.process_name, self.method, x, blind, region_name, key
                ))
                for key in ["json", "root"]
            }
            for feature in self.features
        }

    def convert_parameters(self, d):
        for param, val in d.items():
            if isinstance(val, str):
                if "," not in val:
                    d[param] = tuple([float(val)])
                else:
                    d[param] = tuple(map(float, val.split(', ')))
            else:
                d[param] = val
        return d

    def run(self):
        inputs = self.input()

        assert self.process_name in self.config.process_group_names[self.process_group_name]

        isMC = self.config.processes.get(self.process_name).isMC
        for ifeat, feature in enumerate(self.features):
            systs_directions = [("central", "")]
            if isMC and self.store_systematics:
                systs = self.get_systs(feature, isMC)
                systs_directions += list(itertools.product(systs, directions))

            # fit range
            x_range = (float(self.x_range[0]), float(self.x_range[1]))
            x = ROOT.RooRealVar("x", "x", x_range[0], x_range[1])

            # blinded range
            blind = False
            if self.blind_range[0] != self.blind_range[1]:
                blind = True
                blind_range = (float(self.blind_range[0]), float(self.blind_range[1]))
                assert(blind_range[0] >= x_range[0] and blind_range[0] < blind_range[1] and
                    blind_range[1] <= x_range[1])
                x.setRange("loSB", x_range[0], blind_range[0])
                x.setRange("hiSB", blind_range[1], x_range[1])
                x.setRange("full", x_range[0], x_range[1])
                fit_range = "loSB,hiSB"
            l = ROOT.RooArgList(x)

            if blind:
                x_blind = ROOT.RooRealVar("x_blind", "x_blind", blind_range[0], blind_range[1])
                l_blind = ROOT.RooArgList(x_blind)

            d = {}
            for syst_name, direction in systs_directions:
                key = ""
                if syst_name != "central":
                    key = f"_{syst_name}_{direction}"
                tf = ROOT.TFile.Open(inputs["root"].targets[feature.name].path)
                try:
                    histo = copy(tf.Get("histograms/" + self.process_name + key))
                except:
                    raise ValueError(f"The histogram has not been created for {self.process_name}")

                data = ROOT.RooDataHist("data_obs", "data_obs", l, histo)
                if blind:
                    data_blind = ROOT.RooDataHist("data_obs_blind", "data_obs_blind", l_blind, histo)
                frame = x.frame(ROOT.RooFit.Title("Muon SV mass"))
                data.plotOn(frame)

                n_non_zero_bins = 0
                for i in range(1, histo.GetNbinsX()):
                    if histo.GetBinContent(i) > 0:
                        n_non_zero_bins += 1

                fit_parameters = {}
                if self.method == "voigtian":
                    fit_parameters["mean"] = self.fit_parameters.get("mean",
                        ((x_range[1] + x_range[0]) / 2, -100, 100))
                    fit_parameters["sigma"] = self.fit_parameters.get("sigma", (0.001, 0, 0.1))
                    fit_parameters["gamma"] = self.fit_parameters.get("gammma", (0.02, 0, 0.1))
                    fit_parameters = self.convert_parameters(fit_parameters)

                    mean = ROOT.RooRealVar('mean', 'Mean of Voigtian', *fit_parameters["mean"])
                    sigma = ROOT.RooRealVar('sigma', 'Sigma of Voigtian', *fit_parameters["sigma"])
                    gamma = ROOT.RooRealVar('gamma', 'Gamma of Voigtian', *fit_parameters["gamma"])
                    fun = ROOT.RooVoigtian("model_%s" % self.process_name,
                        "model_%s" % self.process_name, x, mean, gamma, sigma)

                elif self.method == "polynomial":
                    order = int(self.fit_parameters.get("order", 1))
                    for i in range(order):
                        fit_parameters[f"p{i}"] = self.fit_parameters.get(f"p{i}", (0, -1, 1))
                    fit_parameters = self.convert_parameters(fit_parameters)
                    params = []
                    for i in range(order):
                        if i == 0:
                            params.append(ROOT.RooRealVar('p0', 'p0', *fit_parameters[f"p0"]))
                        else:
                            params.append(ROOT.RooRealVar(f'p{i}', f'p{i}', *fit_parameters[f"p{i}"]))

                    fun = ROOT.RooPolynomial("model_%s" % self.process_name,
                        "model_%s" % self.process_name, x, ROOT.RooArgList(*params))

                elif self.method == "exponential":
                    # https://root.cern.ch/doc/master/classRooExponential.html
                    fit_parameters["c"] = self.fit_parameters.get("c", (0, -2, 2))
                    fit_parameters = self.convert_parameters(fit_parameters)
                    c = ROOT.RooRealVar('c', 'c', *fit_parameters["c"])
                    fun = ROOT.RooExponential("model_%s" % self.process_name,
                        "model_%s" % self.process_name, x, c)

                elif self.method == "powerlaw":
                    order = int(self.fit_parameters.get("order", 1))
                    for i in range(order):
                        fit_parameters[f"a{i}"] = self.fit_parameters.get(f"a{i}", (1, 0, 2))
                        fit_parameters[f"b{i}"] = self.fit_parameters.get(f"b{i}", (0, -2, 2))
                    fit_parameters = self.convert_parameters(fit_parameters)

                    params = [x]
                    for i in range(order):
                        params.append(ROOT.RooRealVar(f'a{i}', f'a{i}', *fit_parameters[f"a{i}"]))
                        params.append(ROOT.RooRealVar(f'b{i}', f'b{i}', *fit_parameters[f"b{i}"]))

                    fit_fun = " + ".join([f"@{i + 1} * TMath::Power(@0, @{i + 2})"
                        for i in range(0, order, 2)])

                    fun = ROOT.RooGenericPdf("model_%s" % self.process_name,
                        fit_fun, ROOT.RooArgList(*params))

                # fitting
                if not blind:
                    fun.fitTo(data, ROOT.RooFit.SumW2Error(True))
                else:
                    fun.fitTo(data, ROOT.RooFit.Range(fit_range), ROOT.RooFit.SumW2Error(True))

                npar = fun.getParameters(data).selectByAttrib("Constant",False).getSize()

                # filling output dict with fitting results
                if self.method == "voigtian":
                    gamma_value = gamma.getVal()
                    sigma_value = sigma.getVal()

                    G = 2 * sigma_value * np.sqrt(2 * np.log(2))
                    L = 2 * gamma_value
                    HWHM = (0.5346 * L + np.sqrt(0.2166 * L ** 2 + G ** 2)) / 2

                    d[key] = {
                        "mean": mean.getVal(),
                        "mean_error": mean.getError(),
                        "sigma": sigma.getVal(),
                        "sigma_error": sigma.getError(),
                        "gamma": gamma.getVal(),
                        "gamma_error": gamma.getError(),
                        "HWHM": HWHM,
                    }
                elif self.method == "polynomial":
                    param_values = [p.getVal() for p in params]
                    d[key] = dict(zip([f'p{i}' for i in range(order)], param_values))
                elif self.method == "exponential":
                    d[key] = {"c": c.getVal()}
                elif self.method == "powerlaw":
                    param_values = []
                    for i in range(order):
                        param_values.append((f"a{i}", params[1 + 2 * i].getVal()))
                        param_values.append((f"b{i}", params[1 + 2 * i + 1].getVal()))
                    d[key] = dict(param_values)

                fun.plotOn(frame)
                d[key]["chi2"] = frame.chiSquare()
                d[key]["npar"] = npar
                d[key]["chi2/ndf"] = frame.chiSquare(npar)
                d[key]["Number of non-zero bins"] = n_non_zero_bins
                d[key]["Full chi2"] = frame.chiSquare() * n_non_zero_bins
                d[key]["ndf"] = n_non_zero_bins - npar
                d[key]["integral"] = data.sumEntries()
                d[key]["fit_range"] = self.x_range
                d[key]["blind_range"] = "None" if not blind else blind_range
                num_entries = data.sumEntries() - (0 if not blind else data_blind.sumEntries())
                d[key]["integral"] = num_entries

                w_name = "workspace_" + self.process_name + key
                workspace = ROOT.RooWorkspace(w_name, w_name)
                getattr(workspace, "import")(fun)
                getattr(workspace, "import")(data)
                workspace.Print()
                f = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "UPDATE")
                f.cd()
                workspace.Write()
                f.Close()

            with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
                json.dump(d, f, indent=4)


class InspectFitSyst(Fit):
    def requires(self):
        """
        Needs as input the json file provided by the Fit task
        """
        return Fit.vreq(self)

    def output(self):
        """
        Returns, per feature, one json file storing the fit results and one root file
        storing the workspace
        """
        region_name = "" if not self.region else "__{}".format(self.region.name)
        return {
            feature.name: {
                "json": self.local_target("{}__{}__{}{}.json".format(
                    feature.name, self.process_name, self.method, region_name)),
                "txt": self.local_target("{}__{}__{}{}.txt".format(
                    feature.name, self.process_name, self.method, region_name))
            }
            for feature in self.features
        }

    def run(self):
        inputs = self.input()
        isMC = self.config.processes.get(self.process_name).isMC
        for ifeat, feature in enumerate(self.features):
            systs = self.get_unique_systs(self.get_systs(feature, isMC) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], isMC))

            params = ["integral"]
            if self.method == "voigtian":
                params += ["mean", "sigma", "gamma"]
            with open(inputs[feature.name]["json"].path) as f:
                d = json.load(f)

            table = []
            out = {}
            for syst in systs:
                line = [syst]
                out[syst] = {}
                for param in params:
                    out[syst][param] = {}
                    if d[""][param] == 0:
                        out[syst][param]["up"] = -999
                        out[syst][param]["down"] = -999
                    else:
                        out[syst][param]["up"] = (d[f"_{syst}_up"][param] - d[""][param]) / d[""][param]
                        out[syst][param]["down"] = (d[f"_{syst}_down"][param] - d[""][param]) /\
                            d[""][param]
                    line.append(out[syst][param]["up"])
                    line.append(out[syst][param]["down"])
                table.append(line)
            txt = tabulate.tabulate(table, headers=["syst name"] + list(
                itertools.product(params, directions)))
            print(txt)
            with open(create_file_dir(self.output()[feature.name]["txt"].path), "w+") as f:
                f.write(txt)
            with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
                json.dump(out, f, indent=4)


class CombineDatacards(CreateDatacards):
    category_names = law.CSVParameter(default=("base",), description="names of categories "
        "to run, default: (base,)")

    category_name = "base"

    def store_parts(self):
        parts = super(CombineDatacards, self).store_parts()
        parts["category_name"] = "cat_combined"
        return parts

    def requires(self):
        return {
            category_name: CreateDatacards.vreq(self, category_name=category_name,
                _exclude=["category_names"])
            for category_name in self.category_names
        }

    def output(self):
        return {
            feature.name: self.local_target("{}{}.txt".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        cmd = "combineCards.py "
        inputs = self.input()
        for feature in self.features:
            force_shape = False
            for category_name in self.category_names:
                cmd += f"{category_name}={inputs[category_name][feature.name]['txt'].path} "
                with open(inputs[category_name][feature.name]['txt'].path) as f:
                    text = f.read()
                if "shapes" in text:
                    force_shape = True
            if force_shape:
                cmd += "--force-shape "
            cmd += f"> {self.output()[feature.name].path}"
            create_file_dir(f"{self.output()[feature.name].path}")
            os.system(cmd)


class CreateWorkspace(CreateDatacards):
    category_names = law.CSVParameter(default=("base",), description="names of categories "
        "to run, default: (base,)")
    higgs_mass = luigi.IntParameter(default=125, description="Higgs mass to be used inside "
        "combine, default:125")

    def __init__(self, *args, **kwargs):
        super(CreateWorkspace, self).__init__(*args, **kwargs)
        self.is_combination = (len(self.category_names) > 1)

    def requires(self):
        if self.is_combination:
            return CombineDatacards.vreq(self)
        return CreateDatacards.vreq(self)

    def store_parts(self):
        parts = super(CreateWorkspace, self).store_parts()
        if not self.is_combination:
            parts["category_name"] = "cat_" + self.category_name
        return parts

    def output(self):
        return {
            feature.name: self.local_target("workspace_{}{}.root".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        inputs = self.input()
        for feature in self.features:
            if not self.is_combination:
                inp = inputs[feature.name]['txt'].path
            else:
                inp = inputs[feature.name].path
            cmd = "text2workspace.py {} -m {} -o {}".format(
                inp, self.higgs_mass, create_file_dir(self.output()[feature.name].path))
            os.system(cmd)


class RunCombine(CreateWorkspace):
    method = luigi.ChoiceParameter(choices=("limits",), default="limits",
        description="combine method to be considered, default: False")
    unblind = luigi.BoolParameter(default=False, description="whether to run combine unblinded, "
        "default: False")

    def requires(self):
        return CreateWorkspace.vreq(self)

    def output(self):
        return {
            feature.name: {
                key: self.local_target("results_{}{}.{}".format(
                    feature.name, self.get_output_postfix(), key))
                for key in ["txt", "root"]
            }
            for feature in self.features
        }

    def run(self):
        if self.method == "limits":
            out_file = "higgsCombine{}.AsymptoticLimits.mH{}.root"

        inputs = self.input()
        for feature in self.features:
            test_name = randomize("Test")
            cmd = "combine -M "
            if self.method == "limits":
                cmd += "AsymptoticLimits "
            cmd += f"--name {test_name} "
            if not self.unblind:  # not sure if this is only for AsymptoticLimits
                cmd += "--run blind "
            cmd += f"-m {self.higgs_mass} "
            cmd += inputs[feature.name].path
            cmd += f" > {create_file_dir(self.output()[feature.name]['txt'].path)}"
            os.system(cmd)
            move(out_file.format(test_name, self.higgs_mass),
                self.output()[feature.name]["root"].path)
