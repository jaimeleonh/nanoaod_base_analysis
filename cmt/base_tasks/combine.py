# coding: utf-8

"""
Tasks related to the Combine package.
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
from cmt.base_tasks.analysis import (
    ProcessGroupNameTask, CombineCategoriesTask, CreateWorkspace, RunCombine
)

directions = ["up", "down"]


class SLBase(ProcessGroupNameTask, CombineCategoriesTask):
    use_control_regions = luigi.BoolParameter(default=False, description="whether to use control "
        "regions available in the datacard, default: False")


class CreateWorkspaceSL(SLBase, CreateWorkspace):
    def __init__(self, *args, **kwargs):
        super(CreateWorkspaceSL, self).__init__(*args, **kwargs)
        self.requires().force_shape = True

    def run(self):
        """
        Obtains the workspace for each provided datacard.
        """
        inputs = self.input()
        channel_masks = "" if not self.use_control_regions else "--channel-masks"
        for feature in self.features:
            if not self.combine_categories:
                inp = inputs[list(self.category_names)[self.branch]][feature.name]['txt'].path
            else:
                inp = inputs[feature.name].path
            cmd = ("text2workspace.py --X-allow-no-signal --X-allow-no-background "
                "{} {} -m {} -o {}".format(
                    channel_masks, inp, self.higgs_mass,
                    create_file_dir(self.output()[feature.name]["root"].path)))
            error_code = os.system(cmd)
            with open(create_file_dir(self.output()[feature.name]["log"].path), "w+") as f:
                if error_code == 0:
                    f.write("Success")
                else:
                    f.write("Error")


class SimplifiedLikelihood(SLBase, RunCombine):
    ntoys = luigi.IntParameter(default=2000, description="Number of toys to consider in the "
        "covariance")

    def workflow_requires(self):
        """
        Requires the workspace coming from CreateWorkspace.
        """
        return {"data": CreateWorkspaceSL.vreq(self)}

    def requires(self):
        """
        Requires the workspace coming from CreateWorkspace.
        """
        return CreateWorkspaceSL.vreq(self)

    def output(self):
        """
        Outputs one root file storing the covariance and another one storing the signal yields
        for r = 1
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                key: self.local_target("results_{}{}_{}.root".format(
                    feature.name, self.get_output_postfix(), key))
                for key in ["cov", "signal", "full_like"]
            }
            for feature in self.features
        }

    def run(self):
        """
        Runs combine over the provided workspaces.
        """

        inputs = self.input()
        for feature in self.features:
            test_name = randomize("Test")
            cmd = (f"combine {inputs[feature.name]['root'].path} -M FitDiagnostics --saveShapes "
                f"--saveWithUnc --numToysForShape {self.ntoys} --saveOverall --preFitValue 0 "
                f"-n {test_name}")

            os.system(cmd)
            move(f"fitDiagnostics{test_name}.root",
                create_file_dir(self.output()[feature.name]["cov"].path))

            test_name = randomize("Test")
            cmd = (f"combine {inputs[feature.name]['root'].path} -M FitDiagnostics --saveShapes "
                f"--saveWithUnc --numToysForShape 1 --saveOverall --preFitValue 1 "
                f"-n {test_name}")
            print(f"Running {cmd}...")
            os.system(cmd)
            move(f"fitDiagnostics{test_name}.root",
                create_file_dir(self.output()[feature.name]["signal"].path))

            test_name = randomize("Test")
            cmd = (f"combine {inputs[feature.name]['root'].path} -M MultiDimFit "
                f"--rMin -0.5 --rMax 2 --algo grid -m {self.higgs_mass} "
                f"-n {test_name}")

            os.system(cmd)
            move(f"higgsCombine{test_name}.MultiDimFit.mH{self.higgs_mass}.root",
                create_file_dir(self.output()[feature.name]["full_like"].path))


class MakeSLInputs(SimplifiedLikelihood):
    def workflow_requires(self):
        """
        Requires the root files produced by SimplifiedLikelihood.
        """
        return {"data": SimplifiedLikelihood.vreq(self)}

    def requires(self):
        """
        Requires the root files produced by SimplifiedLikelihood.
        """
        return SimplifiedLikelihood.vreq(self)

    def output(self):
        """
        Outputs root files storing the simplified likelihood inputs
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                key: self.local_target("results_{}{}_{}.root".format(
                    feature.name, self.get_output_postfix(), key))
                for key in ["bkg", "signal"]
            }
            for feature in self.features
        }

    def run(self):
        """
        Runs combine over the provided workspaces.
        """

        script_path = os.path.expandvars("${CMT_BASE}/cmt/utils/makeLHInputs.py")
        for feature in self.features:
            inp = self.input()[feature.name]
            out = self.output()[feature.name]

            cmd = (f"python3 {script_path} -i {inp['cov'].path} -o {create_file_dir(out['bkg'].path)}")
            os.system(cmd)

            cmd = (f"python3 {script_path} -i {inp['signal'].path} -o {create_file_dir(out['signal'].path)}")
            os.system(cmd)


class ConvertSLInputs(SimplifiedLikelihood):
    def workflow_requires(self):
        """
        Requires the root files produced by SimplifiedLikelihood.
        """
        return {"data": MakeSLInputs.vreq(self)}

    def requires(self):
        """
        Requires the root files produced by SimplifiedLikelihood.
        """
        return MakeSLInputs.vreq(self)

    def output(self):
        """
        Outputs root files storing the simplified likelihood inputs
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name:
                self.local_target("model_{}{}.py".format(feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        """
        Transforms root file into SL python model
        """

        script_path = os.path.expandvars(
            "${CMSSW_BASE}/src/HiggsAnalysis/CombinedLimit/test/simplifiedLikelihoods/"
            "convertSLRootToPython.py")

        for feature in self.features:
            inp = self.input()[feature.name]
            out = create_file_dir(self.output()[feature.name].path)

            cmd = (f"python3 {script_path} -O {out} "
                f"-s {inp['signal'].path}:shapes_prefit/total_signal "
                f"-b {inp['bkg'].path}:shapes_prefit/total_M1 "
                f"-d {inp['bkg'].path}:shapes_prefit/total_data "
                f"-c {inp['bkg'].path}:shapes_prefit/total_M2 "
                f"-t {inp['bkg'].path}:shapes_prefit/total_M3")
            print(cmd)
            os.system(cmd)


class PlotSimplifiedLikelihood(SimplifiedLikelihood):
    def workflow_requires(self):
        """
        Requires the python model of the SL and the root file storing the full likelihood.
        """
        return {
            "data": {
                "model": ConvertSLInputs.vreq(self),
                "full_like": SimplifiedLikelihood.vreq(self)
            }
        }

    def requires(self):
        """
        Requires the python model of the SL and the root file storing the full likelihood.
        """
        return {
            "model": ConvertSLInputs.vreq(self),
            "full_like": SimplifiedLikelihood.vreq(self)
        }

    def output(self):
        """
        Outputs pdf file showing SL vs full L comparison
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name:
                self.local_target("plot_{}{}.pdf".format(feature.name, self.get_output_postfix()))
            for feature in self.features
        }

    def run(self):
        import simplike as sl

        for feature in self.features:
            exec(open(self.input()["model"][feature.name].path).read(), globals())
            slp1 = sl.SLParams(background, covariance, obs=data, sig=signal)

            import ROOT
            fi = ROOT.TFile.Open(self.input()["full_like"][feature.name]["full_like"].path)
            tr = fi.Get("limit")

            points = []
            for i in range(tr.GetEntries()):
              tr.GetEntry(i)
              points.append([tr.r, 2 * tr.deltaNLL])
            points.sort()

            mus2 = [pt[0] for pt in points]
            tmus2 = [pt[1] for pt in points]

            import numpy as np
            npoints = 50
            mus1 = np.arange(-0.5, 2, (2 + 0.5)/npoints)
            tmus1 = [slp1.tmu(mu) for mu in mus1]

            from matplotlib import pyplot as plt
            plt.plot(mus1, tmus1, label='Simplified likelihood')
            plt.plot(mus2, tmus2, label='Full likelihood')
            plt.legend()
            plt.xlabel("$\mu$")
            plt.ylabel("$-2\Delta \ln L$")

            plt.savefig(create_file_dir(self.output()[feature.name].path))


class GOFProduction(RunCombine):
    ntoys = luigi.IntParameter(default=200, description="Number of toys to consider in the "
        "covariance, default: 200")
    seed = luigi.IntParameter(default=123456, description="Random seed to be considered, "
        "default: 123456")
    algo = luigi.ChoiceParameter(default="saturated", choices=("saturated", "KS", "AD"),
        significant=False, description="algorithm to be used in the GOF computation, "
        "default: saturated")

    def output(self):
        """
        Outputs one root file for the data results and another with the MC toy results
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                key: self.local_target("results_{}{}_{}.root".format(
                    feature.name, self.get_output_postfix(), key))
                for key in ["data", "mc"]
            }
            for feature in self.features
        }

    def run(self):
        """
        Runs combine over the provided workspaces.
        """

        inputs = self.input()
        for feature in self.features:
            test_name = randomize("Test")
            cmd = (f"combine -M GoodnessOfFit {inputs[feature.name]['root'].path} --algo={self.algo}"
                f" -n {test_name} -m {self.higgs_mass}")
            os.system(cmd)
            move(f"higgsCombine{test_name}.GoodnessOfFit.mH{self.higgs_mass}.root",
                create_file_dir(self.output()[feature.name]["data"].path))
            cmd = (f"combine -M GoodnessOfFit {inputs[feature.name]['root'].path} --algo={self.algo}"
                f" -n {test_name} -m {self.higgs_mass} -t {self.ntoys} -s {self.seed}")
            os.system(cmd)
            move(f"higgsCombine{test_name}.GoodnessOfFit.mH{self.higgs_mass}.{self.seed}.root",
                create_file_dir(self.output()[feature.name]["mc"].path))


class GOFPlot(GOFProduction):
    def workflow_requires(self):
        """
        Requires the root files produced by GOFProduction.
        """
        return {"data": GOFProduction.vreq(self)}

    def requires(self):
        """
        Requires the root files produced by GOFProduction.
        """
        return GOFProduction.vreq(self)

    def output(self):
        """
        Outputs json, pdf and png files with the GOF distributions
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                key: self.local_target("results_{}{}.{}".format(
                    feature.name, self.get_output_postfix(), key))
                for key in ["json", "pdf", "png"]
            }
            for feature in self.features
        }

    def run(self):
        """
        Runs combineTool.py and plotGof.py to create the GOF distribution plots
        """

        for feature in self.features:
            inp = self.input()[feature.name]
            out = self.output()[feature.name]

            cmd = (f"combineTool.py -M CollectGoodnessOfFit --input {inp['data'].path} "
                f"{inp['mc'].path} -m {self.higgs_mass} -o {create_file_dir(out['json'].path)}")
            os.system(cmd)

            test_name = randomize("Test")
            cmd = (f"plotGof.py {out['json'].path} --statistic {self.algo} "
                f"--mass {float(self.higgs_mass)} -o {test_name}")
            os.system(cmd)
            move(f"{test_name}.pdf", create_file_dir(out["pdf"].path))
            move(f"{test_name}.png", create_file_dir(out["png"].path))


class PrePostFitProduction(RunCombine):

    def output(self):
        """
        Outputs one root file with the results from FitDiagnostics
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                "fit_diagnostics": self.local_target("results_{}{}__fit_diagnostics.root".format(
                    feature.name, self.get_output_postfix())),
                "combine_output": self.local_target("results_{}{}__combine_output.root".format(
                    feature.name, self.get_output_postfix())),
            }
            for feature in self.features
        }

    def run(self):
        """
        Runs combine over the provided workspaces.
        """

        inputs = self.input()
        for feature in self.features:
            test_name = randomize("Test")
            cmd = (f"combine -M FitDiagnostics {inputs[feature.name]['root'].path} "
                f"-m {self.higgs_mass} --saveShapes --saveWithUncertainties -n {test_name}")
            os.system(cmd)
            move(f"higgsCombine{test_name}.FitDiagnostics.mH{self.higgs_mass}.root",
                create_file_dir(self.output()[feature.name]["combine_output"].path))
            move(f"fitDiagnostics{test_name}.root",
                create_file_dir(self.output()[feature.name]["fit_diagnostics"].path))


class PrePostFitPlotting(RunCombine):
    stages = ["prefit", "postfit"]

    def create_branch_map(self):
        """
        Returns one branch per category
        """
        return len(self.category_names)

    def workflow_requires(self):
        """
        Requires the root files produced by PrePosFitProduction.
        """
        if self.combine_categories:
            return {"data": PrePostFitProduction.vreq(self, branch=0)}
        else:
            return {"data": PrePostFitProduction.vreq(self)}

    def requires(self):
        """
        Requires the root files produced by PrePosFitProduction.
        """
        if self.combine_categories:
            return PrePostFitProduction.vreq(self, branch=0)
        else:
            return PrePostFitProduction.vreq(self)

    def output(self):
        """
        Outputs pdf and png files with the PreFit and PostFit distributions
        """
        # assert not self.combine_categories or (
            # self.combine_categories and len(self.category_names) > 1)
        return {
            feature.name: {
                stage: {
                    key: self.local_target("{}_distribution_{}{}.{}".format(
                        stage, feature.name, self.get_output_postfix(
                            category_names=self.category_names), key))
                    for key in ["pdf", "png"]
                } for stage in self.stages
            }
            for feature in self.features
        }

    def run(self):
        """
        Plots prefit and postfit distribution plots
        """

        from plotting_tools.root import get_labels, Canvas
        ROOT = import_root()
        ROOT.gStyle.SetOptStat(0)

        self.category = self.config.categories.get(self.category_names[self.branch])

        for feature in self.features:
            inp = self.input()[feature.name]
            out = self.output()[feature.name]
            d_name = (f"{feature.name}_{self.region_name}"
                if not self.combine_categories else self.category_names[self.branch])

            for stage, name in zip(self.stages, ["shapes_prefit", "shapes_fit_s"]):
                c = Canvas()
                tf = ROOT.TFile.Open(inp["fit_diagnostics"].path)
                h_bkg = tf.Get(f"{name}/{d_name}/total_background")
                h_sig = tf.Get(f"{name}/{d_name}/total")
                h_dat = tf.Get(f"{name}/{d_name}/data")

                try:
                    h_bkg.SetFillColor(ROOT.TColor.GetColor(100, 192, 232))
                except AttributeError:
                    print(f"Category {self.category.name} does not exist in the root file, "
                        "probably it was not considered in the combination of cards. "
                        "Dummy files are created as output.")
                    os.system(f"touch {create_file_dir(out[stage]['pdf'].path)}")
                    os.system(f"touch {create_file_dir(out[stage]['png'].path)}")
                    continue

                h_dat.Scale(1./1500.)
                h_bkg.Scale(1./1500.)
                h_sig.Scale(1./1500.)

                m_sig = h_sig.GetMaximum()
                m_dat = max([h_dat.GetPointY(i) + h_dat.GetErrorYhigh(i)
                    for i in range(1, h_dat.GetN())])

                h_bkg.SetTitle("")
                h_bkg.GetYaxis().SetRangeUser(0, 1.1 * max(m_sig, m_dat))
                h_sig.SetTitle("")
                h_sig.GetYaxis().SetRangeUser(0, 1.1 * max(m_sig, m_dat))

                h_bkg.Draw("HIST")

                h_err = h_bkg.Clone()
                h_err.SetFillColorAlpha(12, 0.3)  # Set grey colour (12) and alpha (0.3)
                h_err.SetMarkerSize(0)
                h_err.Draw("E2SAME")

                h_sig.SetLineColor(ROOT.kRed)
                h_sig.Draw("HISTSAME")

                h_dat.Draw("PSAME")

                h_bkg.SetMaximum(h_bkg.GetMaximum() * 1.4)

                legend = ROOT.TLegend(0.60, 0.70, 0.88, 0.88)
                legend.SetBorderSize(0)
                legend.AddEntry(h_bkg, "Background", "F")
                legend.AddEntry(h_sig, "Signal + Background", "L")
                legend.AddEntry(h_err, "Background uncertainty", "F")
                legend.Draw()

                upper_right="{}, {:.1f} ".format(
                    self.config.year,
                    self.config.lumi_fb,
                ) + "fb^{-1} " + "({} TeV)".format(self.config.ecm)  # Fix for eras

                inner_text = self.config.get_inner_text_for_plotting(self.category, self.region)

                draw_labels = get_labels(
                    upper_left=self.config.upper_left_text,
                    upper_right=upper_right,
                    inner_text=inner_text
                )
                for label in draw_labels:
                    label.Draw("same")

                c.SaveAs(create_file_dir(out[stage]["pdf"].path))
                c.SaveAs(create_file_dir(out[stage]["png"].path))
