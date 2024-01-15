# coding: utf-8

"""
Analysis tasks.
"""

__all__ = []

import os
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
    """

    automcstats = luigi.IntParameter(default=10, description="value used for autoMCStats inside "
        "the datacard, -1 to avoid using it, default: 10")
    additional_lines = law.CSVParameter(default=(), description="addtional lines to write at the "
        "end of the datacard")
    propagate_syst_qcd = luigi.BoolParameter(default=False, description="whether to propagate"
        "systematics to qcd background, default: False")
    fit_models = luigi.Parameter(default="", description="filename with fit models to use "
        "in the fit, default: none (binned fit)")

    def __init__(self, *args, **kwargs):
        super(CreateDatacards, self).__init__(self, *args, **kwargs)

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        if len(self.data_names) > 1:
            raise ValueError("Only 1 data process can be provided inside the process group")
        self.non_data_names = [p.name for p in self.processes_datasets.keys() if not p.isData]

        if self.do_qcd:
            self.non_data_names.append("qcd")

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        if not self.fit_models:
            return FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False)
        else:
            import yaml
            from cmt.utils.yaml_utils import ordered_load
            with open(self.retrieve_file("config/{}.yaml".format(self.fit_models))) as f:
                self.models = ordered_load(f, yaml.SafeLoader)

            reqs = {}
            self.model_processes = []
            for model, fit_params in self.models.items():
                self.model_processes.append(fit_params["process_name"])
                params = ", ".join([f"{param}='{value}'"
                    for param, value in fit_params.items() if param != "fit_parameters"])
                if "fit_parameters" in fit_params:
                    params += ", fit_parameters={" + ", ".join([f"'{param}': '{value}'"
                    for param, value in fit_params["fit_parameters"].items()]) + "}"
                reqs[fit_params["process_name"]] = eval(
                    f"Fit.vreq(self, {params}, _exclude=['include_fit'])")

            # In order to have a workspace with data_obs, we replicate the first fit
            # (just in case the x_range is defined) for the available data process
            assert(self.data_names)
            fit_params = list(self.models.values())[0]
            fit_params["process_name"] = self.data_names[0]
            params = ", ".join([f"{param}='{value}'"
                for param, value in fit_params.items() if param != "fit_parameters"])
            if "fit_parameters" in fit_params:
                params += ", fit_parameters={" + ", ".join([f"'{param}': '{value}'"
                for param, value in fit_params["fit_parameters"].items()]) + "}"
            reqs["data_obs"] = eval(f"Fit.vreq(self, {params}, _exclude=['include_fit'])")
            return reqs

    def output(self):
        """
        Returns, per feature, one txt storing the datacard and its corresponding root file
        storing the histograms
        """
        region_name = "" if not self.region else "_{}".format(self.region.name)
        return {
            feature.name: {
                "txt": self.local_target("{}{}.txt".format(feature.name, region_name)),
                "root": self.local_target("{}{}.root".format(feature.name, region_name))
            }
            for feature in self.features
        }

    def get_norm_systematics(self):
        if self.plot_systematics:
            return self.config.get_norm_systematics(self.processes_datasets, self.region)
        return {}

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

            f.write("shapes  *  {0}  {0}.root  $PROCESS  $PROCESS_$SYSTEMATIC\n".format(bin_name))

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
            shapes_table.append(["shapes", name, bin_name, bin_name + ".root",
                "workspace_{0}:model_{0}".format(name)])
        for name in self.data_names:
            if name in self.model_processes:
                # We are extracting the background from the data, so let's label it as background
                # but let's be sure background is not in the process_group_name
                assert not "background" in self.processes_datasets.keys()
                shapes_table.append(["shapes", "background", bin_name, bin_name + ".root",
                    "workspace_{0}:model_{0}".format(name)])

        # Include shape for the data
        shapes_table.append(["shapes", "data_obs", bin_name, bin_name + ".root",
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
            if p_name == "background":
                # assuming it comes from data, may cause problems in certain setups
                rate_line.append(1)
            else:
                filename = self.input()[p_name][feature.name]["json"].path
                with open(filename) as f:
                    d = json.load(f)
                rate_line.append(d["integral"])
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
        for shape_syst in shape_systematics:
            line = [shape_syst, "shape"]
            for p_name in self.non_data_names:
                if p_name in shape_systematics[shape_syst]:
                    line.append(1)
                else:
                    line.append("-")
            table.append(line)

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


    def run(self):
        """
        Splits the processes into data and non-data. Per feature, loads the input histograms, 
        creates the output histograms and the datacard inside the txt file.
        """
        inputs = self.input()

        norm_systematics = self.get_norm_systematics()

        for feature in self.features:
            systs_directions = [("central", "")]
            shape_syst_list = self.get_unique_systs(self.get_systs(feature, True) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], True))
            systs_directions += list(itertools.product(shape_syst_list, directions))

            # Convert the shape systematics list to a dict with the systs as keys and a list of 
            # the processes affected by them (all non-data processes except the qcd if computed
            # in the code)
            shape_systematics = {shape_syst: [p_name for p_name in self.non_data_names]
                for shape_syst in shape_syst_list}

            if not self.fit_models:  # unbinned fits
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
            else:  # binned fits
                tf = ROOT.TFile.Open(create_file_dir(self.output()[feature.name]["root"].path),
                    "RECREATE")
                for model, fit_params in self.models.items():
                    model_tf = ROOT.TFile.Open(
                        inputs[fit_params["process_name"]][feature.name]["root"].path)
                    w = model_tf.Get("workspace_" + fit_params["process_name"])
                    tf.cd()
                    w.Write()
                    model_tf.Close()
                # data_obs
                model_tf = ROOT.TFile.Open(inputs["data_obs"][feature.name]["root"].path)
                w = model_tf.Get("workspace_" + self.data_names[0])
                tf.cd()
                w.Write("workspace_data_obs")
                model_tf.Close()
                tf.Close()

                self.write_shape_datacard(feature, norm_systematics, shape_systematics,
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
        assert self.process_name in self.config.process_group_names[self.process_group_name]

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        return FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False)

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
                "root": self.local_target("{}__{}__{}{}.root".format(
                    feature.name, self.process_name, self.method, region_name))
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
        isMC = self.config.processes.get(self.process_name).isMC
        for ifeat, feature in enumerate(self.features):
            systs = self.get_unique_systs(self.get_systs(feature, isMC) \
                + self.config.get_weights_systematics(self.config.weights[self.category.name], isMC))
            systs_directions = [("central", "")]
            if isMC and self.store_systematics:
                systs_directions += list(itertools.product(systs, directions))

            # fit range
            x_range = (float(self.x_range[0]), float(self.x_range[1]))
            x = ROOT.RooRealVar("x", "x", x_range[0], x_range[1])
            l = ROOT.RooArgList(x)

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
                    fun = ROOT.RooPolynomial("model_%s" % self.process_name,
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
                    out[syst][param]["up"] = (d[f"_{syst}_up"][param] - d[""][param]) / d[""][param]
                    out[syst][param]["down"] = (d[f"_{syst}_down"][param] - d[""][param]) /\
                        d[""][param]
                    line.append((d[f"_{syst}_up"][param] - d[""][param]) / d[""][param])
                    line.append((d[f"_{syst}_down"][param] - d[""][param]) / d[""][param])
                table.append(line)
            txt = tabulate.tabulate(table, headers=["syst name"] + list(
                itertools.product(params, directions)))
            print(txt)
            with open(create_file_dir(self.output()[feature.name]["txt"].path), "w+") as f:
                f.write(txt)
            with open(create_file_dir(self.output()[feature.name]["json"].path), "w+") as f:
                json.dump(out, f, indent=4)
