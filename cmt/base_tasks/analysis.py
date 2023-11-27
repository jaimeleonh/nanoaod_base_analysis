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

import law
import luigi

from analysis_tools.utils import (
    import_root, create_file_dir, randomize
)

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

    def requires(self):
        """
        Needs as input the root file provided by the FeaturePlot task
        """
        return FeaturePlot.vreq(self, save_root=True, stack=True, hide_data=False)

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
                    line.append(signal_counter)
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

    def run(self):
        """
        Splits the processes into data and non-data. Per feature, loads the input histograms, 
        creates the output histograms and the datacard inside the txt file.
        """
        inputs = self.input()

        self.data_names = [p.name for p in self.processes_datasets.keys() if p.isData]
        if len(self.data_names) > 1:
            raise ValueError("Only 1 data process can be provided inside the process group")
        self.non_data_names = [p.name for p in self.processes_datasets.keys() if not p.isData]

        if self.do_qcd:
            self.non_data_names.append("qcd")

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
