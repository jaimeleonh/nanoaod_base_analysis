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
from ctypes import c_double
import uproot

import law
import luigi

from analysis_tools.utils import (
    import_root, create_file_dir, randomize
)

from cmt.base_tasks.base import ConfigTaskWithCategory, HTCondorWorkflow, SGEWorkflow
from cmt.base_tasks.plotting import FeaturePlot

ROOT = import_root()

directions = ["up", "down"]


class FitBase():
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

    def get_x(self, x_range, blind_range=None, name="x"):
        # fit range
        x_range = (float(x_range[0]), float(x_range[1]))
        x = ROOT.RooRealVar(name, name, x_range[0], x_range[1])

        # blinded range
        blind = False
        if not blind_range:
            return x, False
        if blind_range[0] != blind_range[1]:
            blind = True
            blind_range = (float(self.blind_range[0]), float(self.blind_range[1]))
            assert(blind_range[0] >= x_range[0] and blind_range[0] < blind_range[1] and
                blind_range[1] <= x_range[1])
            x.setRange("loSB", x_range[0], blind_range[0])
            x.setRange("hiSB", blind_range[1], x_range[1])
            x.setRange("full", x_range[0], x_range[1])
            fit_range = "loSB,hiSB"
        return x, blind

    def get_fit(self, name, parameters, x, **kwargs):
        fit_parameters = {}
        params = OrderedDict()
        fit_name = kwargs.pop("fit_name", "model")

        if name == "voigtian":
            fit_parameters["mean"] = parameters.get("mean", (0, -100, 100))
            fit_parameters["gamma"] = parameters.get("gamma", (0.02, 0, 0.1))
            fit_parameters["sigma"] = parameters.get("sigma", (0.001, 0, 0.1))
            fit_parameters = self.convert_parameters(fit_parameters)

            params["mean"] = ROOT.RooRealVar('mean', 'Mean of Voigtian', *fit_parameters["mean"])
            params["gamma"] = ROOT.RooRealVar('gamma', 'Gamma of Voigtian', *fit_parameters["gamma"])
            params["sigma"] = ROOT.RooRealVar('sigma', 'Sigma of Voigtian', *fit_parameters["sigma"])
            fun = ROOT.RooVoigtian(fit_name, fit_name, x,
                params["mean"], params["gamma"], params["sigma"])

        elif name == "polynomial":
            order = int(parameters.get("order", 1))
            for i in range(order):
                fit_parameters[f"p{i}"] = parameters.get(f"p{i}", (0, -5, 5))
            fit_parameters = self.convert_parameters(fit_parameters)
            for i in range(order):
                params[f"p{i}"] = ROOT.RooRealVar(f'p{i}', f'p{i}', *fit_parameters[f"p{i}"])
            fun = ROOT.RooPolynomial(fit_name, fit_name, x, ROOT.RooArgList(*list(params.values())))

        elif name == "exponential":
            # https://root.cern.ch/doc/master/classRooExponential.html
            fit_parameters["c"] = parameters.get("c", (0, -2, 2))
            fit_parameters = self.convert_parameters(fit_parameters)
            params["c"] = ROOT.RooRealVar('c', 'c', *fit_parameters["c"])
            fun = ROOT.RooExponential(fit_name, fit_name, x, params["c"])

        elif name == "powerlaw":
            order = int(self.fit_parameters.get("order", 1))
            for i in range(order):
                fit_parameters[f"a{i}"] = parameters.get(f"a{i}", (1, 0, 2))
                fit_parameters[f"b{i}"] = parameters.get(f"b{i}", (0, -2, 2))
            fit_parameters = self.convert_parameters(fit_parameters)

            for i in range(order):
                params[f'a{i}'] = ROOT.RooRealVar(f'a{i}', f'a{i}', *fit_parameters[f"a{i}"])
                params[f'b{i}'] = ROOT.RooRealVar(f'b{i}', f'b{i}', *fit_parameters[f"b{i}"])

            fit_fun = " + ".join([f"@{i + 1} * TMath::Power(@0, @{i + 2})"
                for i in range(0, order, 2)])
            fun = ROOT.RooGenericPdf(fit_name, fit_fun, ROOT.RooArgList(*list(params.values())))

        return fun, params


class CombineBase(FeaturePlot, FitBase):
    """
    Base task for all combine-related tasks

    :param pois: Parameters-of-interest to be considered
    :type pois: list of `str`

    :param higgs_mass: Higgs mass to be considered inside combine
    :type higgs_mass: int

    :param fit_models: filename with fit models to use in the fit
    :type fit_models: str
    """

    pois = law.CSVParameter(default=("r",), description="parameters of interest to be considered, "
        "default: r")
    higgs_mass = luigi.IntParameter(default=125, description="Higgs mass to be used inside "
        "combine, default: 125")
    fit_models = luigi.Parameter(default="", description="filename with fit models to use "
        "in the fit, default: none (binned fit)")

    def get_output_postfix(self, **kwargs):
        self.process_group_name = kwargs.pop("process_group_name", self.process_group_name)
        process_group_name = "" if self.process_group_name == "default" else "_{}".format(
            self.process_group_name)
        region_name = "" if not self.region else "_{}".format(self.region.name)
        return process_group_name + region_name


class CombineCategoriesTask(CombineBase):
    category_names = law.CSVParameter(default=("base",), description="names of categories "
        "to run, default: (base,)")
    combine_categories = luigi.BoolParameter(default=False, description="whether to run on the "
        "combined datacard or per category, default: False (per category)")

    def get_output_postfix(self, **kwargs):
        postfix = super(CombineCategoriesTask, self).get_output_postfix()
        if not self.combine_categories:
            postfix += "_" + list(self.category_names)[self.branch]
        return postfix

    def store_parts(self):
        parts = super(CombineCategoriesTask, self).store_parts()
        # parts["category_name"] = "cat_combined"
        del parts["category_name"]
        return parts


class CreateDatacards(CombineBase):
    """
    Task that creates datacards for its use inside the combine framework

    :param automcstats: Value used for autoMCStats inside the datacard, -1 to avoid using it.
    :type automcstats: int

    :param additional_lines: Additional lines to write at the end of the datacard.
    :type additional_lines: list of `str`

    :param counting: whether the datacard should consider a counting experiment
    :type counting: bool
    """

    automcstats = luigi.IntParameter(default=10, description="value used for autoMCStats inside "
        "the datacard, -1 to avoid using it, default: 10")
    additional_lines = law.CSVParameter(default=(), description="addtional lines to write at the "
        "end of the datacard")
    propagate_syst_qcd = luigi.BoolParameter(default=False, description="whether to propagate"
        "systematics to qcd background, default: False")
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

    def get_shape_systematics_from_inspect(self, feature_name):
        def round_unc(num):
            exp = 0
            while True:
                if num * 10 ** exp > 1:
                    return round(num, exp + 1)
                exp += 1
        # structure: systematics[syst_name][process_name][parameter] = syst_value
        systematics = {}
        for name in self.non_data_names:
            path = self.input()["inspections"][name][feature_name]["json"].path
            with open(path) as f:
                d = json.load(f)
            for syst_name, values in d.items():
                for param in values:
                    if param == "integral":
                        continue
                    up_value = round_unc(abs(values[param]["up"]))
                    down_value = round_unc(abs(values[param]["down"]))
                    if up_value == -999 or down_value == -999:
                        continue
                    if abs(up_value) > self.norm_syst_threshold or \
                            abs(down_value) > self.norm_syst_threshold:
                        if name not in systematics:
                            systematics[name] = {}
                        if param not in systematics[name]:
                            systematics[name][param] = {}
                        systematics[name][param][syst_name] = max(up_value, down_value)
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

    def write_shape_datacard(self, feature, norm_systematics, shape_systematics,
            datacard_syst_params, *args):
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

        # shape systematics
        for syst_param in datacard_syst_params:
            table.append([syst_param, "param", 0.0, 1.0])

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
            weight = (0 if d[""]["integral_error"] == 0
                else rate / (d[""]["integral_error"] * d[""]["integral_error"]))
            if self.additional_scaling.get(p_name, False):
                rate *= float(self.additional_scaling.get(p_name))
                weight /= float(self.additional_scaling.get(p_name))
            return rate, weight

    def get_stat_unc_lines(self, feature, rates, process_names=None):
        def round_unc(num):
            exp = 0
            while True:
                if num * 10 ** exp > 1:
                    return round(num, exp + 1)
                exp += 1

        if not process_names:
            process_names = self.non_data_names
        table = []
        for p_name in process_names:
            # line = [f"{p_name}_norm", "gmN {:.2f}".format(rates[p_name][0] * rates[p_name][1])]
            line = [f"{p_name}_{self.category_name}_norm",
                "gmN {}".format(int(round(rates[p_name][0] * rates[p_name][1])))]
            append_line = False
            for name in self.non_data_names:
                if name == p_name or \
                        p_name in [p.name for p in self.config.get_children_from_process(name)]:
                    if rates[p_name][1] > 0.001:
                        append_line = True
                        # line.append("{:.4f}".format(1. / rates[p_name][1]))
                        line.append(round_unc(1. / rates[p_name][1]))
                else:
                    line.append("-")
            if append_line:
                table.append(line)
        return table

    def write_counting_datacard(self, feature, norm_systematics, shape_systematics, *args):
        n_dashes = 50

        region_name = "" if not self.region else "_{}".format(self.region.name)
        bin_name = "{}".format(region_name)

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
        rates = {}
        for p_name in process_names:
            rates[p_name] = self.get_process_rate_for_counting(p_name, feature)
            if not self.config.processes.get(p_name).isSignal and rates[p_name][0] < 0.001:
                rates[p_name] = (0.001, rates[p_name][1])
            rate_line.append(round(rates[p_name][0], 3))
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

        # statistical uncertainty
        stat_unc_lines = self.get_stat_unc_lines(feature, rates)
        table += stat_unc_lines

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
                shape_systematics_feature = self.get_shape_systematics_from_inspect(feature.name)
                # shape_systematics_feature = {}
                datacard_syst_params = []

                tf = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "RECREATE")
                for model, fit_params in self.models.items():
                    model_tf = ROOT.TFile.Open(
                        inputs["fits"][fit_params["process_name"]][feature.name]["root"].path)
                    w = model_tf.Get("workspace_" + fit_params["process_name"])
                    if fit_params["process_name"] not in shape_systematics_feature:
                        # directly extract the workspace from the inputs
                        tf.cd()
                        w.Write()
                        model_tf.Close()
                    else:
                        # create a new workspace with the dedicated systematics
                        x_range = fit_params.get("x_range", Fit.x_range._default)
                        if type(x_range) == str:
                            x_range = x_range.split(", ")
                        blind_range = fit_params.get("blind_range", Fit.blind_range._default)
                        method = fit_params.get("method")

                        x, blind = self.get_x(x_range, blind_range)
                        data = w.data("data_obs")
                        fit_parameters = fit_params.get("fit_parameters", {})
                        _, params = self.get_fit(method, fit_parameters, x)
                        # for param in params:
                            # param.setConstant(True)

                        # let's build the RooFormulaVar for each param (w/ or w/o systematics)
                        param_syst = OrderedDict()
                        systs = OrderedDict()
                        for param, value in params.items():
                            if param not in shape_systematics_feature[fit_params["process_name"]]:
                                # no systematics to add, only need to consider the actual parameter
                                param_syst[param] = value
                            else:
                                systs[param] = []
                                syst_values = []
                                for syst, syst_value in \
                                        shape_systematics_feature[fit_params["process_name"]][param].items():
                                    systs[param].append(ROOT.RooRealVar(f"d{param}_{syst}",
                                        f"d{param}_{syst}", 0, -5, 5))
                                    systs[param][-1].setConstant(True)
                                    syst_values.append(syst_value)
                                    datacard_syst_params.append(f"d{param}_{syst}")
                                param_syst[param] = ROOT.RooFormulaVar(
                                    f"{param}_syst", f"{param}_syst",
                                    "@0*" + "*".join([f"(1+{syst_values[i]}*@{i+1})"
                                        for i in range(len(syst_values))]),
                                    ROOT.RooArgList(value, *systs[param]))

                        # Create the new fitting function
                        if method == "voigtian":
                            fun = ROOT.RooVoigtian("model_%s" % fit_params["process_name"],
                                "model_%s" % fit_params["process_name"], x,
                                param_syst["mean"], param_syst["gamma"], param_syst["sigma"])
                        elif method == "polynomial":
                            fun = ROOT.RooPolynomial("model_%s" % fit_params["process_name"],
                                "model_%s" % fit_params["process_name"], x,
                                ROOT.RooArgList(*list(param_syst.values())))
                        elif method == "exponential":
                            fun = ROOT.RooExponential("model_%s" % fit_params["process_name"],
                                "model_%s" % fit_params["process_name"], x, param_syst["c"])
                        elif method == "powerlaw":
                            order = len(params.values())
                            fit_fun = " + ".join([f"@{i + 1} * TMath::Power(@0, @{i + 2})"
                                for i in range(0, order, 2)])
                            fun = ROOT.RooGenericPdf("model_%s" % self.process_name,
                                fit_fun, ROOT.RooArgList(*list(params.values())))

                        # Refit
                        if not blind:
                            fun.fitTo(data, ROOT.RooFit.SumW2Error(True))
                        else:
                            fun.fitTo(data, ROOT.RooFit.Range(
                                float(self.x_range[0]), float(self.x_range[1])),
                                ROOT.RooFit.SumW2Error(True))

                        for value in params.values():
                            value.setConstant(True)

                        # save the function inside the workspace
                        w_name = "workspace_syst"
                        workspace_syst = ROOT.RooWorkspace(w_name, w_name)
                        getattr(workspace_syst, "import")(fun)
                        getattr(workspace_syst, "import")(data)
                        tf.cd()
                        workspace_syst.Write("workspace_" + fit_params["process_name"])
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
                    datacard_syst_params, *self.additional_lines)

            else:  # counting experiment
                norm_systematics_feature = self.get_norm_systematics_from_inspect(feature.name)
                norm_systematics_feature.update(norm_systematics)

                self.write_counting_datacard(feature, norm_systematics_feature, shape_systematics,
                    *self.additional_lines)


class Fit(FeaturePlot, FitBase):
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
            x, blind = self.get_x(self.x_range, self.blind_range)
            l = ROOT.RooArgList(x)

            if blind:
                x_blind, _ = self.get_x(self.blind_range)
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

                frame = x.frame()
                data.plotOn(frame)

                n_non_zero_bins = 0
                for i in range(1, histo.GetNbinsX()):
                    if histo.GetBinContent(i) > 0:
                        n_non_zero_bins += 1

                # get function to fit and its parameters
                fun, params = self.get_fit(self.method, self.fit_parameters, x,
                    fit_name="model_" + self.process_name)

                # fitting
                if not blind:
                    fun.fitTo(data, ROOT.RooFit.SumW2Error(True))
                else:
                    fun.fitTo(data, ROOT.RooFit.Range(
                        float(self.x_range[0]), float(self.x_range[1])),
                        ROOT.RooFit.SumW2Error(True))

                npar = fun.getParameters(data).selectByAttrib("Constant", False).getSize()

                # filling output dict with fitting results
                d[key] = {}
                for param, value in params.items():
                    d[key][param] = value.getVal()
                    # setting the parameter as constant before saving it in the workspace
                    value.setConstant(True)  # needs further studying
                if self.method == "voigtian":
                    gamma_value = params["gamma"].getVal()
                    sigma_value = params["sigma"].getVal()
                    G = 2 * sigma_value * np.sqrt(2 * np.log(2))
                    L = 2 * gamma_value
                    d[key]["HWHM"] = (0.5346 * L + np.sqrt(0.2166 * L ** 2 + G ** 2)) / 2

                histo_new = data.createHistogram("histo_new", x)
                error = c_double(0.)
                integral = histo_new.IntegralAndError(
                    0, histo_new.GetNbinsX() + 1, error)
                if blind:
                    histo_blind = data_blind.createHistogram("histo_blind", x_blind)
                    error_blind = c_double(0.)
                    integral_blind = histo_blind.IntegralAndError(
                        0, histo_blind.GetNbinsX() + 1, error_blind)

                # Additional results to include in the output dict
                fun.plotOn(frame)
                d[key]["chi2"] = frame.chiSquare()
                d[key]["npar"] = npar
                d[key]["chi2/ndf"] = frame.chiSquare(npar)
                d[key]["Number of non-zero bins"] = n_non_zero_bins
                d[key]["Full chi2"] = frame.chiSquare() * n_non_zero_bins
                d[key]["ndf"] = n_non_zero_bins - npar
                d[key]["integral"] = data.sumEntries()
                d[key]["fit_range"] = self.x_range
                d[key]["blind_range"] = "None" if not blind else self.blind_range
                d[key]["sum_entries"] = data.sumEntries() - (
                    0 if not blind else data_blind.sumEntries())
                d[key]["integral"] = integral - (0 if not blind else integral_blind)
                d[key]["integral_error"] = error.value - (0 if not blind else error_blind.value)

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
            elif self.method == "exponential":
                params += ["c"]
            else:
                order = int(self.fit_parameters.get("order", 1))
                if self.method == "polynomial":
                    params += [f'p{i}' for i in range(order)]
                elif self.method == "powerlaw":
                    params += [f'a{i}' for i in range(order)] + [f'b{i}' for i in range(order)]

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


class CombineDatacards(CombineCategoriesTask):

    category_name = "base"
    combine_categories = True

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


class CreateWorkspace(CreateDatacards, CombineCategoriesTask,
        law.LocalWorkflow, HTCondorWorkflow, SGEWorkflow):

    category_name = "base"

    def create_branch_map(self):
        if self.combine_categories:
            return 1
        return len(self.category_names)

    def requires(self):
        if self.combine_categories:
            return CombineDatacards.vreq(self)
        return {
            category_name: CreateDatacards.vreq(self, category_name=category_name)
            for category_name in self.category_names
        }

    def workflow_requires(self):
        if self.combine_categories:
            return {"data": CombineDatacards.vreq(self)}
        return {
            "data": {
                category_name: CreateDatacards.vreq(self, category_name=category_name)
                for category_name in self.category_names
            }
        }

    def output(self):
        assert not self.combine_categories or (
            self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: self.local_target("workspace_{}{}.root".format(
                feature.name, self.get_output_postfix() ))
            for feature in self.features
        }

    def run(self):
        inputs = self.input()
        for feature in self.features:
            if not self.combine_categories:
                inp = inputs[list(self.category_names)[self.branch]][feature.name]['txt'].path
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

    def workflow_requires(self):
        return {"data": CreateWorkspace.vreq(self)}

    def requires(self):
        return CreateWorkspace.vreq(self)

    def output(self):
        assert not self.combine_categories or (
            self.combine_categories and len(self.category_names) > 1)
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


class PullsAndImpacts(RunCombine):
    # based on https://gitlab.cern.ch/hh/tools/inference/-/blob/master/dhi/tasks/pulls_impacts.py
    def get_selected_nuisances(self, nuisances):
        return nuisances

    def extract_nuisance_names(self, path):
        def prepare_prefit_var(var, pdf, epsilon=0.001):
            """
            Prepares a RooRealVar *var* for the extraction of its prefit values using the corresponding
            *pdf* object. Internally, this is done using a made-up fit with precision *epsilon* following a
            recipe in the CombineHarvester (see
            https://github.com/cms-analysis/CombineHarvester/blob/f1029e160701140ce3a1c1f44a991315fd272886/CombineTools/python/combine/utils.py#L87-L95).  # noqa
            *var* is returned.
            """

            if var and pdf:
                nll = ROOT.RooConstraintSum("NLL", "", ROOT.RooArgSet(pdf), ROOT.RooArgSet(var))
                minimizer = ROOT.RooMinimizer(nll)
                minimizer.setEps(epsilon)
                minimizer.setErrorLevel(0.5)
                minimizer.setPrintLevel(-1)
                minimizer.setVerbose(False)
                minimizer.minimize("Minuit2", "migrad")
                minimizer.minos(ROOT.RooArgSet(var))

            return var

        tf = ROOT.TFile.Open(path)
        w = tf.Get("w")
        config = w.genobj("ModelConfig")
        all_params = config.GetPdf().getParameters(config.GetObservables())
        params = {}
        for param in all_params:
            if not (isinstance(param, ROOT.RooRealVar) and not param.isConstant()
                    and param.GetName() not in self.pois):
                continue

            pdf = w.pdf(f"{param.GetName()}_Pdf")
            gobs = w.var(f"{param.GetName()}_In")
            if pdf and gobs:
                prepare_prefit_var(param, pdf)
                nom = param.getVal()
                prefit = [nom + param.getErrorLo(), nom, nom + param.getErrorHi()]
                if isinstance(pdf, ROOT.RooGaussian):
                    pdf_type = "Gaussian"
                elif isinstance(pdf, ROOT.RooPoisson):
                    pdf_type = "Poisson"
                elif isinstance(pdf, ROOT.RooBifurGauss):
                    pdf_type = "AsymmetricGaussian"
                else:
                    pdf_type = "Unrecognised"
            elif not pdf or isinstance(pdf, ROOT.RooUniform):
                pdf_type = "Unconstrained"
                nom = param.getVal()
                prefit = [nom, nom, nom]

            # get groups
            start = "group_"
            groups = [attr.replace(start, "")
                for attr in param.attributes() if attr.startswith(start)]

            # store it
            params[param.GetName()] = {
                "name": param.GetName(),
                "type": pdf_type,
                "groups": groups,
                "prefit": prefit,
            }

        tf.Close()
        return params

    @law.workflow_property(setter=False, empty_value=law.no_value, cache=True)
    def workspace_parameters(self):
        ws_input = CreateWorkspace.vreq(self, branch=0).output()[self.features[0].name]
        if not ws_input.exists():
            return law.no_value
        return self.extract_nuisance_names(ws_input.path)

    def __init__(self, *args, **kwargs):
        super(PullsAndImpacts, self).__init__(*args, **kwargs)

    def create_branch_map(self):
        self.nuisance_names = [self.pois[0]]
        params = self.get_selected_nuisances(self.workspace_parameters)
        if params:
            self.nuisance_names.extend(list(params.keys()))
            self.cache_branch_map = True
        return self.nuisance_names

    def workflow_requires(self):
        return {"data": CreateWorkspace.vreq(self, _exclude=["branches", "branch"])}

    def requires(self):
        return CreateWorkspace.vreq(self, _exclude=["branches", "branch"])

    def output(self):
        assert(self.combine_categories or self.category_names == 1)
        return {
            feature.name: {
                key: self.local_target("results_{}{}__{}.{}".format(
                feature.name, self.get_output_postfix(), self.branch_data, key))
                for key in ["log", "root", "json"]
            }
            for feature in self.features
        }

    def run(self):
        inp = self.input()["collection"].targets[0]
        for feature in self.features:
            w_path = inp[feature.name].path

            if self.branch == 0:
                cmd = ("combine -M MultiDimFit -n _initialFit --algo singles "
                    "--redefineSignalPOIs {poi} {toys} -m {mass} -d {workspace} > {log_file}")
                out_file = "higgsCombine_initialFit.MultiDimFit.mH{mass}.root"
            else:
                cmd = ("combine -M MultiDimFit -n _paramFit_Test_{param} --algo impact "
                    "--redefineSignalPOIs {poi} -P {param} --floatOtherPOIs 1 --saveInactivePOI 1 "
                    "{toys} -m {mass} -d {workspace} > {log_file}")
                out_file = "higgsCombine_paramFit_Test_{param}.MultiDimFit.mH{mass}.root"

            toys = "--toys -1"
            if self.unblind:
                toys = ""
            log_file = randomize("log")

            cmd = cmd.format(poi=self.pois[0], toys=toys, mass=self.higgs_mass,
                workspace=w_path, param=self.branch_data, log_file=log_file)
            print("Running " + cmd)
            os.system(cmd)

            move(log_file, create_file_dir(self.output()[feature.name]["log"].path))
            move(out_file.format(param=self.branch_data, mass=self.higgs_mass),
                create_file_dir(self.output()[feature.name]["root"].path))

            tree = uproot.open(self.output()[feature.name]["root"].path)["limit"]
            results = {
                self.branch_data: tree[self.branch_data].array().tolist()
            }
            if self.branch_data != self.pois[0]:
                results[self.pois[0]] = tree[self.pois[0]].array().tolist()
                results["prefit"] = self.workspace_parameters[self.branch_data]["prefit"]
                results["groups"] = self.workspace_parameters[self.branch_data]["groups"]
                results["type"] = self.workspace_parameters[self.branch_data]["type"]

            with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
                json.dump(results, f)


class MergePullsAndImpacts(CombineCategoriesTask):
    category_name = "base"

    def requires(self):
        return PullsAndImpacts.vreq(self)

    def output(self):
        assert(self.combine_categories or self.category_names == 1)
        return {
            feature.name: self.local_target("results_{}{}.json".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        inp = self.input()["collection"].targets
        nuisance_names = list(self.requires().get_branch_map().values())  # includes POI
        for feature in self.features:
            res = {"POIs": [], "params": []}
            with open(inp[0][feature.name]["json"].path) as f:
                d = json.load(f)
            res["POIs"].append({
                "name": nuisance_names[0],
                "fit": [d[self.pois[0]][1], d[self.pois[0]][0], d[self.pois[0]][2]]
            })

            for inuis, nuisance_name in enumerate(nuisance_names[1:]):
                res["params"].append({"name": nuisance_name})
                with open(inp[inuis + 1][feature.name]["json"].path) as f:
                    d = json.load(f)
                res["params"][-1]["fit"] = [
                    d[nuisance_name][1], d[nuisance_name][0], d[nuisance_name][2]
                ]
                res["params"][-1][self.pois[0]] = [
                    d[self.pois[0]][1], d[self.pois[0]][0], d[self.pois[0]][2]
                ]
                res["params"][-1]["impact_" + self.pois[0]] = max(
                    list(map(abs, (res["params"][-1][self.pois[0]][1] -
                        res["params"][-1][self.pois[0]][i] for i in [0, 2])))
                )
                res["params"][-1]["prefit"] = d["prefit"]
                res["params"][-1]["groups"] = d["groups"]
                res["params"][-1]["type"] = d["type"]

            with open(create_file_dir(self.output()[feature.name].path), "w+") as f:
                json.dump(res, f, indent=4)


class PlotPullsAndImpacts(MergePullsAndImpacts):
    def requires(self):
        return MergePullsAndImpacts.vreq(self)

    def output(self):
        assert(self.combine_categories or self.category_names == 1)
        return {
            feature.name: self.local_target("impacts_{}{}.pdf".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        inp = self.input()
        for feature in self.features:
            out = randomize("impacts")
            os.system("plotImpacts.py -i {} -o {}".format(
                inp[feature.name].path,
                out
            ))
            move(out + ".pdf", self.output()[feature.name].path)
