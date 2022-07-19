# coding: utf-8

"""
Tasks used for the phy6cool mini-dissertation.
"""

__all__ = []


import abc
import contextlib
import itertools
from collections import OrderedDict, defaultdict
import os
import json

import law
import luigi

from analysis_tools.utils import join_root_selection as jrs
from analysis_tools.utils import import_root, create_file_dir

from cmt.base_tasks.base import ( 
    DatasetTaskWithCategory, DatasetWrapperTask, HTCondorWorkflow, InputData,
    ConfigTaskWithCategory, SplittedTask, DatasetTask, RDFModuleTask
)

class NtupleMaker(DatasetTaskWithCategory, law.LocalWorkflow, HTCondorWorkflow):
    default_store = "$CMT_STORE_EOS_CATEGORIZATION"
    default_wlcg_fs = "wlcg_fs_categorization"

    tree_name = "Events"

    def create_branch_map(self):
        """
        :return: number of files for the selected dataset
        :rtype: int
        """
        return len(self.dataset.get_files(
            os.path.expandvars("$CMT_TMP_DIR/%s/" % self.config.name), add_prefix=False))

    def workflow_requires(self):
        """
        """
        return {"data": InputData.req(self)}

    def requires(self):
        """
        Each branch requires one input file
        """
        return InputData.req(self, file_index=self.branch)

    def output(self):
        """
        :return: One file per input file
        :rtype: `.root`
        """
        return  self.local_target("data_%s.root" % self.branch)

    @law.decorator.notify
    @law.decorator.localize(input=False)
    def run(self):
        """
        Creates one RDataFrame per input file, runs the desired RDFModules
        and counts the number of events w/ and w/o additional weights
        """

        from shutil import copy
        ROOT = import_root()
        # ROOT.EnableImplicitMT()

        ROOT.gInterpreter.Declare("""
            using Vfloat = const ROOT::RVec<float>&;
            Double_t Phi_mpi_pi(Double_t x) {
                while (x >= 3.14159) x -= (2 * 3.14159);
                while (x < -3.14159) x += (2 * 3.14159);
                return x;
            }
            #include "TMath.h"
            using Vfloat = const ROOT::RVec<float>&;      
            ROOT::RVec<bool> maskDeltaR(Vfloat eta1, Vfloat phi1, Vfloat eta2, Vfloat phi2, float th_dr) {
                ROOT::RVec<bool> mask;
                for (size_t i = 0; i < eta1.size(); i++){
                    bool bigger_deltar = true;
                    for (size_t j = 0; j < eta2.size(); j++){
                        Double_t deta = eta1[i] - eta2[j];
                        Double_t dphi = Phi_mpi_pi(phi1[i] - phi2[j]);
                        Double_t dr = TMath::Sqrt(deta * deta + dphi * dphi);
                        if (dr < th_dr) bigger_deltar = false;
                    }
                    mask.push_back(bigger_deltar);
                }
                return mask;
            }
        """)

        # prepare inputs and outputs
        inp = self.input().path
        print(inp)
        outp = self.output()
        df = ROOT.RDataFrame(self.tree_name, inp)

        hmodel = ("nevents", "nevents", 1, 1, 2)
        histo_noweight = df.Define("nevents", "1.").Histo1D(hmodel, "nevents")

        branches = []
        trigger_list = ["HLT_IsoMu24", "HLT_IsoMu27",
            "HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1",
            "HLT_Ele32_WPTight_Gsf", "HLT_Ele35_WPTight_Gsf",
            "HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1",
            "HLT_DoubleMediumChargedIsoPFTauHPS35_Trk1_eta2p1_Reg",
            "HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1"]

        df = df.Define("pass_trigger", " || ".join(["%s == 1" % elem for elem in trigger_list]))
        # branches.append("pass_trigger")
        df = df.Filter("pass_trigger == 1")

        muon_sel = "Muon_dxy <= 0.045 && Muon_dz <= 0.2 && Muon_tightId == 1"
        tau_sel = ("Tau_idDeepTau2017v2p1VSmu >= 1 && Tau_idDeepTau2017v2p1VSe >= 3 "
            "&& Tau_idDeepTau2017v2p1VSjet >= 1 && Tau_dz <= 0.2 && !(Tau_decayMode != 0 "
            "&& Tau_decayMode != 1 && Tau_decayMode != 10 && Tau_decayMode != 11)")
        electron_sel = "Electron_dxy <= 0.045 && Electron_dz <= 0.2 && Electron_mvaFall17V2Iso_WP80 == 1"
        jet_sel = ("Jet_pt >= 20 && (Jet_puId >= 4 || Jet_pt >= 50) && Jet_jetId >= 2 && "
        # jet_sel = ("(Jet_puId >= 4 || Jet_pt >= 50) && Jet_jetId >= 2 && "
            "maskDeltaR(Jet_eta, Jet_phi, Muon_eta, Muon_phi, 0.5) && "
            "maskDeltaR(Jet_eta, Jet_phi, Electron_eta, Electron_phi, 0.5) && "
            "maskDeltaR(Jet_eta, Jet_phi, Tau_eta, Tau_phi, 0.5)")

        for variable in ["MET_pt", "MET_phi"]:
            # df = df.Define(variable.lower(), "%s" % (variable))
            branches.append(variable)

        for variable in ["Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass", "Muon_charge", "Muon_pfRelIso04_all"]:
            df = df.Define(variable.lower(), "%s[%s]" % (variable, muon_sel))
            branches.append(variable.lower())
        df = df.Define("nmuon", "muon_pt.size()")
        branches.append("nmuon")

        for variable in ["Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass", "Tau_charge"]:
            df = df.Define(variable.lower(), "%s[%s]" % (variable, tau_sel))
            branches.append(variable.lower())
        df = df.Define("ntau", "tau_pt.size()")
        branches.append("ntau")

        for variable in ["Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass", "Electron_charge"]:
            df = df.Define(variable.lower(), "%s[%s]" % (variable, electron_sel))
            branches.append(variable.lower())
        df = df.Define("nelectron", "electron_pt.size()")
        branches.append("nelectron")

        for variable in ["Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass", "Jet_btagDeepFlavB"]:
            df = df.Define(variable.lower(), "%s[%s]" % (variable, jet_sel))
            branches.append(variable.lower())
        df = df.Define("njet", "jet_pt.size()")
        branches.append("njet")

        df = df.Filter("(nmuon == 1 && nelectron == 0) || (nmuon == 0 && nelectron == 1) || (ntau >= 2 && nmuon == 0 && nelectron == 0)")

        branch_list = ROOT.vector('string')()
        for branch_name in branches:
            branch_list.push_back(branch_name)
        df.Snapshot(self.tree_name, create_file_dir(outp.path), branch_list)

        tf = ROOT.TFile.Open(outp.path, "update")
        histo_noweight.GetXaxis().SetBinLabel(1, "nevents (bin 1)")
        histo_noweight.Write()
        tf.Close()

