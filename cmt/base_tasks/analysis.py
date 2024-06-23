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

from cmt.base_tasks.base import (
    ConfigTaskWithCategory, HTCondorWorkflow, SGEWorkflow, SlurmWorkflow,
    DatasetWrapperTask, FitBase, ProcessGroupNameTask, QCDABCDTask
)
from cmt.base_tasks.plotting import BasePlotTask, FeaturePlot

ROOT = import_root()

directions = ["up", "down"]


class CombineBase(BasePlotTask, FitBase):
    """
    Base task for all combine-related tasks

    :param pois: Parameters-of-interest to be considered
    :type pois: list of `str`

    :param higgs_mass: Higgs mass to be considered inside combine
    :type higgs_mass: int

    :param fit_models: filename with fit models to use in the fit
    :type fit_models: str

    :param unblind: Whether to run combine unblinded.
    :type method: bool
    """

    pois = law.CSVParameter(default=("r",), description="parameters of interest to be considered, "
        "default: r")
    higgs_mass = luigi.IntParameter(default=125, description="Higgs mass to be used inside "
        "combine, default: 125")
    fit_models = luigi.Parameter(default="", description="filename with fit models to use "
        "in the fit, default: none (binned fit)")
    unblind = luigi.BoolParameter(default=False, description="whether to run combine unblinded, "
        "default: False")

    def get_output_postfix(self, **kwargs):
        postfix = super(CombineBase, self).get_output_postfix()
        default_pgn = getattr(self, "process_group_name", "default")
        process_group_name = kwargs.get("process_group_name", default_pgn)
        process_group_name = "" if process_group_name == "default" else "_{}".format(
            process_group_name)
        region_name = "" if not self.region else "_{}".format(self.region.name)
        return process_group_name + region_name


class CombineCategoriesTask(CombineBase):
    """
    Base task for all combine-related tasks with multiple categories

    :param category_names: names of categories to run
    :type category_names: list of `str`

    :param combine_categories: whether to run on the combined datacard or per category
    :type combine_categories: bool

    """
    category_names = law.CSVParameter(default=("base",), description="names of categories "
        "to run, default: (base,)")
    combine_categories = luigi.BoolParameter(default=False, description="whether to run on the "
        "combined datacard or per category, default: False (per category)")

    def get_output_postfix(self, **kwargs):
        postfix = super(CombineCategoriesTask, self).get_output_postfix(**kwargs)
        if not self.combine_categories:
            category_names = kwargs.get("category_names", self.category_names)
            postfix += "_" + list(category_names)[self.branch]
        return postfix


class CreateDatacards(CombineBase, FeaturePlot):
    """
    Task that creates datacards for its use inside the combine framework

    :param automcstats: Value used for autoMCStats inside the datacard, -1 to avoid using it.
    :type automcstats: int

    :param additional_lines: Additional lines to write at the end of the datacard.
    :type additional_lines: list of `str`

    :param propagate_syst_qcd: Whether to propagate systematics to estimated qcd background.
    :type propagate_syst_qcd: bool

    :param counting: whether the datacard should consider a counting experiment
    :type counting: bool
    """

    automcstats = luigi.IntParameter(default=10, description="value used for autoMCStats inside "
        "the datacard, -1 to avoid using it, default: 10")
    additional_lines = law.CSVParameter(default=(), description="addtional lines to write at the "
        "end of the datacard")
    propagate_syst_qcd = luigi.BoolParameter(default=False, description="whether to propagate"
        "systematics to estimated qcd background, default: False")
    counting = luigi.BoolParameter(default=False, description="whether the datacard should consider "
        "a counting experiment, default: False")
    refit_signal_with_syst = luigi.BoolParameter(default=True, description="whether to refit the "
        "signal histograms after modifying the fit parameters with systematics, default: True")

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
            return FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False,
                normalize_signals=False)
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
                    f"Fit.vreq(self, {params}, _exclude=['include_fit', 'save_pdf', 'save_png'])")
                reqs["inspections"][fit_params["process_name"]] = eval(
                    f"InspectFitSyst.vreq(self, {params}, "
                        "_exclude=['include_fit', 'save_pdf', 'save_png'])")

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
        keys = ["txt", "json"]
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
            if num == 0:
                return num
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

    def add_processes_line(self, process_names):
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
            except ValueError:
                # qcd coming from do_qcd or background coming from data
                if "qcd" in p_name or "data" in p_name:
                    line.append(bkg_counter)
                    bkg_counter += 1
                else:  # signal coming from a grid
                    line.append(sig_counter)
                    sig_counter -= 1
        return line

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
        table.append(self.add_processes_line(self.non_data_names))
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

    def get_rate_from_process_in_fit(self, feature, p_name):
        filename = self.input()["fits"][p_name][feature.name]["json"].path
        with open(filename) as f:
            d = json.load(f)
        rate = d[""]["integral"]
        if self.additional_scaling.get(p_name, False):
            rate *= float(self.additional_scaling.get(p_name))
        return rate

    def write_shape_datacard(self, feature, norm_systematics, shape_systematics,
            datacard_syst_params, datacard_env_cats, *args):
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
        table.append(self.add_processes_line(process_names))

        rate_line = ["rate", ""]
        yields = {}
        for p_name in process_names:
            if p_name == "background" and self.data_names[0] in self.model_processes:
                # assuming it comes from data, may cause problems in certain setups
                rate_line.append(1)
            else:
                yields[p_name] = self.get_rate_from_process_in_fit(feature, p_name)
                rate_line.append(yields[p_name])
        table.append(rate_line)
        with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
            json.dump(yields, f, indent=4)

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

        # datacard envelope categories
        for cat in datacard_env_cats:
            table.append([cat, "discrete"])

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
            num_ev = round(rate * weight)
            if self.additional_scaling.get(p_name, False):
                rate *= float(self.additional_scaling.get(p_name))
                weight /= float(self.additional_scaling.get(p_name))
            # round rate and modify the weight accordingly
            weight = (num_ev / rate if rate != 0. else 0.)
            return rate, weight

    def get_stat_unc_lines(self, feature, rates, process_names=None):
        def round_unc(num, ev, val):
            exp = 0
            while True:
                exp += 1
                if abs(round(num, exp) * ev - round(val, 3)) < 0.001:
                    return round(num, exp)

        if not process_names:
            process_names = self.non_data_names
        table = []
        for p_name in process_names:
            # line = [f"{p_name}_norm", "gmN {:.2f}".format(rates[p_name][0] * rates[p_name][1])]
            evs = int(round(rates[p_name][0] * rates[p_name][1]))
            # evs = rates[p_name][0] * rates[p_name][1]
            line = [f"{p_name}_{self.category_name}_norm", "gmN {}".format(evs)]
            append_line = False
            for name in self.non_data_names:
                if name == p_name or \
                        p_name in [p.name for p in self.config.get_children_from_process(name)]:
                    if rates[p_name][1] > 0.001:
                        append_line = True
                        # line.append("{:.4f}".format(1. / rates[p_name][1]))
                        line.append(round_unc(1. / rates[p_name][1], evs, rates[p_name][0]))
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
        table.append(self.add_processes_line(process_names))

        rate_line = ["rate", ""]
        yields = {}
        for p_name in process_names:
            yields[p_name] = self.get_process_rate_for_counting(p_name, feature)
            rate_line.append(round(yields[p_name][0], 3))
        table.append(rate_line)
        with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
            json.dump(yields, f, indent=4)

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

    def update_parameters_for_fitting_signal(self, feature, fit_params, fit_parameters,
            workspace_is_available):
        if workspace_is_available:
            with open(self.input()["fits"][fit_params["process_name"]][feature.name]["json"].path
                    ) as f:
                d = json.load(f)
            fit_parameters.update(d[""])
        return fit_parameters

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
                with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
                    json.dump(yields, f, indent=4)

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
                datacard_syst_params = {}  # list of systematics to be included later in the datacard
                datacard_env_cats = []  # list of categories for envelope fits to be included later in the datacard

                tf = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "RECREATE")
                for model, fit_params in self.models.items():
                    # Parametric models can be obtained in two ways
                    # 1) From the Fit class, which already creates a workspace w/o systs
                    # 2) For a grid, where a workspace is not created
                    # In the second case, we always need to create a new workspace.
                    # In the first one, if no systematics need to be applied, one can use the same
                    # workspace created in the Fit class.
                    is_signal = False
                    try:
                        is_signal = self.config.processes.get(fit_params["process_name"]).isSignal
                    except ValueError:
                        is_signal = True and "data" not in fit_params["process_name"]
                        # the latter houldn't be needed, but just in case
                    workspace_is_available = True
                    try:
                        model_tf = ROOT.TFile.Open(
                            inputs["fits"][fit_params["process_name"]][feature.name]["root"].path)
                        w = model_tf.Get("workspace_" + fit_params["process_name"])
                    except:
                        workspace_is_available = False
                    if fit_params["process_name"] not in shape_systematics_feature and \
                            workspace_is_available:
                        # directly extract the workspace from the inputs
                        tf.cd()
                        w.Write()
                        model_tf.Close()
                        # if it's an envelope function, we also need to
                        # store the category in the datacard
                        if fit_params.get("method") == "envelope":
                            datacard_env_cats.append(
                                f"pdf_index_{self.category_name}_" + fit_params["process_name"])
                    else:
                        # create a new workspace with the dedicated systematics
                        x_range = fit_params.get("x_range", Fit.x_range._default)
                        if type(x_range) == str:
                            x_range = x_range.split(", ")
                        blind_range = fit_params.get("blind_range", Fit.blind_range._default)
                        method = fit_params.get("method")
                        if method != "envelope":
                            functions = [method]
                        else:
                            functions = [e.strip() for e in fit_params.get("functions").split(",")]

                        x, blind = self.get_x(x_range, blind_range)
                        fit_parameters = fit_params.get("fit_parameters", {})
                        if is_signal and not self.refit_signal_with_syst:
                            fit_parameters = self.update_parameters_for_fitting_signal(
                                feature, fit_params, fit_parameters, workspace_is_available)

                        # Loop over the different functions (1 if not an envelope)
                        params = {}  # parameters in the fit
                        param_syst = {}  # parameters after shifting
                        systs = {}  # RooRealVar with the systematics
                        funs = []  # functions used for fitting (size 1 if not an envelope)
                        for function in functions:
                            _, params[function] = self.get_fit(function, fit_parameters, x)
                            # for param in params[function]:
                                # param.setConstant(True)
                            param_syst[function] = OrderedDict()
                            systs[function] = OrderedDict()

                            for param, value in params[function].items():
                                if param not in shape_systematics_feature[fit_params["process_name"]]:
                                    # no systematics to add, only need to consider the actual parameter
                                    param_syst[function][param] = value
                                else:
                                    systs[function][param] = []
                                    syst_values = []
                                    for syst, syst_value in \
                                            shape_systematics_feature[fit_params["process_name"]][param].items():

                                        if syst not in datacard_syst_params:
                                            datacard_syst_params[syst] = ROOT.RooRealVar(syst,
                                                syst, 0, -5, 5)
                                            datacard_syst_params[syst].setConstant(True)
                                        systs[function][param].append(datacard_syst_params[syst])
                                        syst_values.append(syst_value)

                                    param_syst[function][param] = ROOT.RooFormulaVar(
                                        f"{param}_syst", f"{param}_syst",
                                        "@0*" + "*".join([f"(1+{syst_values[i]}*@{i+1})"
                                            for i in range(len(syst_values))]),
                                        ROOT.RooArgList(value, *systs[function][param]))

                            # Create the new fitting function
                            if method != "envelope":
                                fit_name = "model_" + fit_params["process_name"]
                            else:
                                fit_name = f"model_{function.strip()}" + fit_params["process_name"]

                            if function == "voigtian":
                                fun = ROOT.RooVoigtian(fit_name, fit_name, x,
                                    param_syst[function]["mean"],
                                    param_syst[function]["gamma"],
                                    param_syst[function]["sigma"])
                            elif function == "polynomial":
                                fun = ROOT.RooPolynomial(fit_name, fit_name, x,
                                    ROOT.RooArgList(*list(param_syst[function].values())))
                            elif function == "exponential":
                                fun = ROOT.RooExponential(fit_name, fit_name, x,
                                    param_syst[function]["c"])
                            elif function == "powerlaw":
                                order = len(params[function].values())
                                fit_fun = " + ".join([f"@{i + 1} * TMath::Power(@0, @{i + 2})"
                                    for i in range(0, order, 2)])
                                fun = ROOT.RooGenericPdf(fit_name, fit_fun,
                                    ROOT.RooArgList(*([x] + list(param_syst[function].values()))))

                            if workspace_is_available:
                                data = w.data("data_obs")

                            # Refit
                            if (not is_signal or self.refit_signal_with_syst) and workspace_is_available:
                                if not blind:
                                    fun.fitTo(data, ROOT.RooFit.SumW2Error(True))
                                else:
                                    fun.fitTo(data, ROOT.RooFit.Range(
                                        float(self.x_range[0]), float(self.x_range[1])),
                                        ROOT.RooFit.SumW2Error(True))

                            for value in params[function].values():
                                value.setConstant(True)

                            funs.append(fun)

                        # save the function inside the workspace
                        w_name = "workspace_syst"
                        workspace_syst = ROOT.RooWorkspace(w_name, w_name)

                        if method == "envelope":
                            models = ROOT.RooArgList()
                            for fun in funs:
                                models.add(fun)
                            cat_name = f"pdf_index_{self.category_name}_" + fit_params["process_name"]
                            cat = ROOT.RooCategory(cat_name, "")
                            datacard_env_cats.append(cat_name)
                            multi_fun = ROOT.RooMultiPdf("model_" + fit_params["process_name"], "",
                                cat, models)
                            getattr(workspace_syst, "import")(cat)
                            getattr(workspace_syst, "import")(multi_fun)
                        else:
                            getattr(workspace_syst, "import")(fun)

                        if workspace_is_available:
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
                    datacard_syst_params.keys(), datacard_env_cats, *self.additional_lines)

            else:  # counting experiment
                norm_systematics_feature = self.get_norm_systematics_from_inspect(feature.name)
                norm_systematics_feature.update(norm_systematics)

                self.write_counting_datacard(feature, norm_systematics_feature, shape_systematics,
                    *self.additional_lines)


class Fit(FeaturePlot, FitBase):
    """
    Task that run fits over FeaturePlot histograms

    :param method: Fitting method to consider. To choose between `voigtian`, `polynomial`,
        `exponential`, `powerlaw`.
    :type method: str

    :param process_name: Process name to consider.
    :type process_name: str

    :param x_range: Range of the x axis to consider in the fitting.
    :type x_range: Two comma-separated `float`.

    :param blind_range: Range of the x axis to blind in the fitting.
    :type blind_range: Two comma-separated `float`.

    :param fit_parameters: Initial values for the parameters involved in the fit.
    :type fit_parameters: `str` representing a dict (e.g. '{\"mean\": \"(20, -100, 100)\"}').

    :param functions: Functions to be considered inside the envelope.
    :type functions: csv string.

    :param save_pdf: whether to save plots in pdf
    :type save_pdf: bool

    :param save_png: whether to save plots in png
    :type save_png: bool

    """

    method = luigi.ChoiceParameter(
        choices=("voigtian", "gaussian", "polynomial", "exponential", "powerlaw", "envelope"),
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
    functions = law.CSVParameter(default={}, description="functions to be considered inside "
        "the envelope, default: None")
    # save_pdf = luigi.BoolParameter(default=False, description="whether to save created histograms "
        # "in pdf, default: False")
    # save_png = luigi.BoolParameter(default=False, description="whether to save created histograms "
        # "in png, default: False")

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

        keys = ["json", "root"]
        if self.save_png:
            keys.append("png")
        if self.save_pdf:
            keys.append("pdf")

        return {
            feature.name: {
                key: self.local_target("{}__{}__{}__{}_{}{}.{}".format(
                    feature.name, self.process_name, self.method, x, blind, region_name, key
                ))
                for key in keys
            }
            for feature in self.features
        }

    def get_input(self):
        return self.input()

    def run(self):
        """
        Obtains the fits per feature and systematic.
        """
        inputs = self.get_input()

        # assert self.process_name in self.config.process_group_names[self.process_group_name]

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
                    if histo.GetBinContent(i) > 0 and \
                            histo.GetBinCenter(i) >= float(self.x_range[0]) and \
                            histo.GetBinCenter(i) <= float(self.x_range[1]):
                        n_non_zero_bins += 1

                funs = []
                # get function to fit and its parameters
                if self.method != "envelope":
                    fun, params = self.get_fit(self.method, self.fit_parameters, x,
                        fit_name="model_" + self.process_name)
                    funs.append(fun)
                else:
                    params = {}
                    for function in self.functions:
                        aux_fun, aux_params = self.get_fit(function.strip(), self.fit_parameters, x,
                            fit_name=f"model_{function.strip()}_{self.process_name}")
                        funs.append(aux_fun)
                        params.update(aux_params)

                # fitting
                for fun in funs:
                    if not blind:
                        fun.fitTo(data, ROOT.RooFit.SumW2Error(True))
                    else:
                        fun.fitTo(data, ROOT.RooFit.Range(
                            float(self.x_range[0]), float(self.x_range[1])),
                            ROOT.RooFit.SumW2Error(True))

                # filling output dict with fitting results
                d[key] = {}
                for param, value in params.items():
                    d[key][param] = value.getVal()
                    d[key][param + "_error"] = value.getError()
                    # setting the parameter as constant before saving it in the workspace
                    value.setConstant(True)  # needs further studying

                if self.method == "voigtian":
                    gamma_value = params["gamma"].getVal()
                    sigma_value = params["sigma"].getVal()
                    G = 2 * sigma_value * np.sqrt(2 * np.log(2))
                    L = 2 * gamma_value
                    d[key]["HWHM"] = (0.5346 * L + np.sqrt(0.2166 * L ** 2 + G ** 2)) / 2

                if self.method != "envelope":
                    npar = fun.getParameters(data).selectByAttrib("Constant", False).getSize()
                    d[key]["npar"] = npar
                    d[key]["chi2/ndf"] = frame.chiSquare(npar)
                    d[key]["ndf"] = n_non_zero_bins - npar
                else:
                    models = ROOT.RooArgList()
                    for fun in funs:
                        models.add(fun)
                    cat = ROOT.RooCategory(f"pdf_index_{self.category_name}_{self.process_name}", "")
                    multi_fun = ROOT.RooMultiPdf("model_" + self.process_name, "", cat, models)

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
                d[key]["Number of non-zero bins"] = n_non_zero_bins
                d[key]["Full chi2"] = frame.chiSquare() * n_non_zero_bins
                d[key]["integral"] = data.sumEntries()
                d[key]["fit_range"] = self.x_range
                d[key]["blind_range"] = "None" if not blind else self.blind_range
                d[key]["sum_entries"] = data.sumEntries() - (
                    0 if not blind else data_blind.sumEntries())
                d[key]["integral"] = integral - (0 if not blind else integral_blind)
                d[key]["integral_error"] = error.value - (0 if not blind else error_blind.value)

                w_name = "workspace_" + self.process_name + key
                workspace = ROOT.RooWorkspace(w_name, w_name)
                if self.method != "envelope":
                    getattr(workspace, "import")(fun)
                else:
                    getattr(workspace, "import")(multi_fun)
                    getattr(workspace, "import")(cat)
                getattr(workspace, "import")(data)
                workspace.Print()
                f = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "UPDATE")
                f.cd()
                workspace.Write()
                f.Close()

                if self.save_png or self.save_pdf:
                    c = ROOT.TCanvas()
                    frame.Draw()
                    if self.save_pdf:
                        c.SaveAs(create_file_dir(self.output()[feature.name]["pdf"].path))
                    if self.save_png:
                        c.SaveAs(create_file_dir(self.output()[feature.name]["png"].path))

            with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
                json.dump(d, f, indent=4)


class InspectFitSyst(Fit):
    """
    Task that extracts systematic effects on the fits.

    """

    def requires(self):
        """
        Needs as input the json file provided by the Fit task
        """
        return Fit.vreq(self)

    def output(self):
        """
        Returns, per feature, one json file and one txt file storing the inspection results
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
        def get_params(method):
            if method == "voigtian":
                return ["mean", "sigma", "gamma"]
            elif method == "exponential":
                return ["c"]
            elif method == "polynomial":
                order = int(self.fit_parameters.get("polynomial_order", 1))
                return [f'p{i}' for i in range(order)]
            elif method == "powerlaw":
                order = int(self.fit_parameters.get("powerlaw_order", 1))
                return [f'a{i}' for i in range(order)] + [f'b{i}' for i in range(order)]

        inputs = self.input()
        isMC = self.config.processes.get(self.process_name).isMC
        for ifeat, feature in enumerate(self.features):
            systs = self.get_unique_systs(self.get_systs(feature, isMC) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], isMC))

            params = ["integral"]
            if self.method != "envelope":
                params.extend(get_params(self.method))
            else:
                for method in self.functions:
                    params.extend(get_params(method.strip()))

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


class CombineDatacards(ProcessGroupNameTask, CombineCategoriesTask):
    """
    Task that combines datacards coming from :class:`.CreateDatacards`.

    """

    combine_categories = True

    def requires(self):
        """
        Needs as input the datacards provided by the CreateDatacards task for each category
        """
        return {
            category_name: CreateDatacards.vreq(self, category_name=category_name,
                _exclude=["category_names"])
            for category_name in self.category_names
        }

    def output(self):
        """
        Outputs a txt file with the merged datacard
        """
        return {
            feature.name: self.local_target("{}{}.txt".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        """
        Combines all datacards using combine's combineCards.py
        """
        inputs = self.input()
        for feature in self.features:
            cmd = "combineCards.py "
            force_shape = False
            for category_name in self.category_names:
                # First, check if this category has all signals or all backgrounds with 0 expectation
                null_signal = True
                null_bkg = True
                with open(inputs[category_name][feature.name]['json'].path) as f:
                    yields = json.load(f)
                for process_name, value in yields.items():
                    if value != 0:
                        try:
                            if self.config.processes.get(process_name).isSignal:
                                null_signal = False
                            elif not self.config.processes.get(process_name).isData:
                                null_bkg = False
                        except:  # signal not included as a process
                            null_signal = False
                if null_signal or null_bkg:
                    continue

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


class CreateWorkspace(ProcessGroupNameTask, CombineCategoriesTask,
        law.LocalWorkflow, HTCondorWorkflow, SGEWorkflow, SlurmWorkflow):
    """
    Task that creates the Combine workspace from the datacards obtained from
    :class:`.CreateDatacards` or :class:`.CombineDatacards`.

    """

    def create_branch_map(self):
        """
        Returns one branch if categories are combined or one branch per category
        """
        if self.combine_categories:
            return [None]
        return len(self.category_names)

    def requires(self):
        """
        Requires the datacard coming from CombineDatacards or one datacard per category obtained by
        CreateDatacards.
        """
        if self.combine_categories:
            return CombineDatacards.vreq(self)
        return {
            category_name: CreateDatacards.vreq(self, category_name=category_name)
            for category_name in self.category_names
        }

    def workflow_requires(self):
        """
        Requires the datacard coming from CombineDatacards or one datacard per category obtained by
        CreateDatacards.
        """
        if self.combine_categories:
            return {"data": CombineDatacards.vreq(self)}
        return {
            "data": {
                category_name: CreateDatacards.vreq(self, category_name=category_name)
                for category_name in self.category_names
            }
        }

    def output(self):
        """
        Outputs one root file with the workspace if categories are combined or one per category.
        """
        assert not self.combine_categories or (
            self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: self.local_target("workspace_{}{}.root".format(
                feature.name, self.get_output_postfix() ))
            for feature in self.features
        }

    def run(self):
        """
        Obtains the workspace for each provided datacard.
        """
        inputs = self.input()
        for feature in self.features:
            if not self.combine_categories:
                inp = inputs[list(self.category_names)[self.branch]][feature.name]['txt'].path
            else:
                inp = inputs[feature.name].path
            cmd = "text2workspace.py {} -m {} -o {}".format(
                inp, self.higgs_mass, create_file_dir(self.output()[feature.name].path))
            os.system(cmd)


class ValidateDatacards(CreateWorkspace):
    """
    Task that runs the ValidateDatacards.py script from the CombineHarvester package
    Uses the datacards coming from :class:`.CreateDatacards` or :class:`.CombineDatacards`.

    NOTE: Needs the full CombineHarvester package, so at the moment it only works in CMSSW_11_3_4.

    """

    def output(self):
        """
        Outputs one root file with the workspace if categories are combined or one per category.
        """
        assert not self.combine_categories or (
            self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                key: self.local_target("results_{}{}.{}".format(
                    feature.name, self.get_output_postfix(), key))
                for key in ["json", "log"]
            }
            for feature in self.features
        }

    def run(self):
        """
        Obtains the workspace for each provided datacard.
        """
        inputs = self.input()
        for feature in self.features:
            if not self.combine_categories:
                inp = inputs[list(self.category_names)[self.branch]][feature.name]['txt'].path
            else:
                inp = inputs[feature.name].path
            filename = randomize("datacard")
            os.system(f"cp {inp} {filename}.txt")
            cmd = "ValidateDatacards.py {}.txt --jsonFile {} > {}".format(
                filename,
                create_file_dir(self.output()[feature.name]["json"].path),
                create_file_dir(self.output()[feature.name]["log"].path)
            )
            os.system(cmd)


class RunCombine(CreateWorkspace):
    """
    Task that runs the combine tool over the workspace created by
    :class:`.CreateWorkspace`.

    :param method: Combine method to consider. Only `limits` (AsymptoticLimits) is implemented for
        now.
    :type method: str

    """

    method = luigi.ChoiceParameter(choices=("limits",), default="limits",
        description="combine method to be considered, default: False")

    def workflow_requires(self):
        """
        Requires the workspace coming from CreateWorkspace.
        """
        return {"data": CreateWorkspace.vreq(self)}

    def requires(self):
        """
        Requires the workspace coming from CreateWorkspace.
        """
        return CreateWorkspace.vreq(self)

    def output(self):
        """
        Outputs one txt file and one root file in total or one of each per category with the results
        of running combine
        """
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
        """
        Runs combine over the provided workspaces.
        """
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


class BasePullsAndImpacts(ProcessGroupNameTask, CombineCategoriesTask):

    robust = luigi.BoolParameter(default=False, description="whether to run pulls and impacts with robust fit, "
        "default: False")

    def get_output_postfix(self, **kwargs):
        postfix = super(CombineCategoriesTask, self).get_output_postfix(**kwargs)
        if self.robust:
            postfix += "__robustfit"
        return postfix


class PullsAndImpacts(BasePullsAndImpacts, law.LocalWorkflow, HTCondorWorkflow, 
                    SGEWorkflow, SlurmWorkflow):
    """
    Task that obtains the pulls and impacts over the workspace created by :class:`.CreateWorkspace`.
    Based on https://gitlab.cern.ch/hh/tools/inference/-/blob/master/dhi/tasks/pulls_impacts.py

    """

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
        """
        Returns the nuisance parameters considered in the fit
        """
        self.nuisance_names = [self.pois[0]]
        params = self.get_selected_nuisances(self.workspace_parameters)
        if params:
            self.nuisance_names.extend(list(params.keys()))
            self.cache_branch_map = True
        return self.nuisance_names

    def workflow_requires(self):
        """
        Requires the workspace coming from CreateWorkspace.
        """
        return {"data": CreateWorkspace.vreq(self, _exclude=["branches", "branch"])}

    def requires(self):
        """
        Requires the workspace coming from CreateWorkspace.
        """
        return CreateWorkspace.vreq(self, _exclude=["branches", "branch"])

    def output(self):
        """
        Outputs one log, root file, and json per nuisance paramter.
        """
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
        """
        Runs the combine commands needed for pulls and impacts computations.
        """
        inp = self.input()["collection"].targets[0]
        robust_fit = "--robustFit 1 " if self.robust else ""
        for feature in self.features:
            w_path = inp[feature.name].path

            if self.branch == 0:
                cmd = ("combine -M MultiDimFit -n _initialFit --algo singles "
                    "--redefineSignalPOIs {poi} {toys} -m {mass} -d {workspace} {robust_fit}>"
                    " {log_file}")
                out_file = "higgsCombine_initialFit.MultiDimFit.mH{mass}.root"
            else:
                cmd = ("combine -M MultiDimFit -n _paramFit_Test_{param} --algo impact "
                    "--redefineSignalPOIs {poi} -P {param} --floatOtherPOIs 1 --saveInactivePOI 1 "
                    "{toys} -m {mass} -d {workspace} {robust_fit}> {log_file}")
                out_file = "higgsCombine_paramFit_Test_{param}.MultiDimFit.mH{mass}.root"

            toys = "--toys -1"
            if self.unblind:
                toys = ""
            log_file = randomize("log")

            cmd = cmd.format(poi=self.pois[0], toys=toys, mass=self.higgs_mass,
                workspace=w_path, param=self.branch_data, robust_fit=robust_fit, log_file=log_file)
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


class MergePullsAndImpacts(BasePullsAndImpacts):
    """
    Task that merges the pulls and impacts for each systematic obtained by :class:`.PullsAndImpacts`.

    """

    def requires(self):
        """
        Requires the json files coming from PullsAndImpacts.
        """
        return PullsAndImpacts.vreq(self)

    def output(self):
        """
        Outputs one json file (in CombineHarvester format) with the results for all
        nuisance parameters.
        """
        assert(self.combine_categories or self.category_names == 1)
        return {
            feature.name: self.local_target("results_{}{}.json".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        """
        Merges the results from the PullsAndImpacts task into one json file
        """
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
    """
    Task that plots pulls and impacts coming from :class:`.MergePullsAndImpacts`.

    """

    def requires(self):
        """
        Requires the json file with all pulls and impacts obtained by MergePullsAndImpacts.
        """
        return MergePullsAndImpacts.vreq(self)

    def output(self):
        """
        Outputs one pdf with the pulls and impacts obtained by CombineHarvester.
        """
        assert(self.combine_categories or self.category_names == 1)
        return {
            feature.name: self.local_target("impacts_{}{}.pdf".format(
                feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        """
        Plots the pulls and impacts using CombineHarvester's plotImpacts.py
        """
        inp = self.input()
        for feature in self.features:
            out = randomize("impacts")
            os.system("plotImpacts.py -i {} -o {}".format(
                inp[feature.name].path,
                out
            ))
            move(out + ".pdf", create_file_dir(self.output()[feature.name].path))
