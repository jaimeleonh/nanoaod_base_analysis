import os
from copy import deepcopy as copy

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from analysis_tools.utils import import_root
from cmt.modules.tau_utils import LeptonTauPair, TriggerChecker, lepton_veto
from cmt.modules.baseModules import JetLepMetModule

ROOT = import_root()

class Htt_trigSFProducer(JetLepMetModule):
    def __init__(self, isMC, year, *args, **kwargs):
        super(Htt_trigSFProducer, self).__init__(*args, **kwargs)
        self.isMC = isMC
        self.year = year
        
        if year == 2016:
            self.mutau_pt_th1 = 23.
            self.mutau_pt_th2 = 25.
        elif year in [2017, 2018]:
            self.mutau_pt_th1 = 25.
            self.mutau_pt_th2 = 32.
            self.etau_pt_th1 = 33.
            self.etau_pt_th2 = 35.

        base = "{}/{}/src/HTT-utilities/LepEffInterface".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gSystem.Load("libHTT-utilitiesLepEffInterface.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/ScaleFactor.h".format(base))

        base_tau = "{}/{}/src/TauAnalysisTools/TauTriggerSFs".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gSystem.Load("libTauAnalysisToolsTauTriggerSFs.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/SFProvider.h".format(base_tau))
        # ROOT.gROOT.ProcessLine(".L /afs/cern.ch/work/j/jleonhol/private/nanoaod_base_analysis/data/cmssw/CMSSW_11_1_0_pre4/src/TauAnalysisTools/TauTriggerSFs/interface/SFProvider.h")
        if self.isMC:
            self.eTrgSF = ROOT.ScaleFactor()
            self.eTauTrgSF = ROOT.ScaleFactor()
            self.muTrgSF = ROOT.ScaleFactor()
            self.muTauTrgSF = ROOT.ScaleFactor()

            if self.year == 2016:
                self.eTrgSF.init_ScaleFactor(
                    "{}/data/Electron/Run2016/Electron_Run2016_legacy_Ele25.root".format(base))
                self.eTauTrgSF = None  # No ele cross triggers in 2016
                self.muTrgSF.init_ScaleFactor(
                    "{}/data/Muon/Run2016/Muon_Run2016_legacy_IsoMu22.root".format(base))
                self.muTauTrgSF.init_ScaleFactor(
                    "{}/data/Muon/Run2016/Muon_Mu19leg_2016BtoH_eff.root".format(base))
                self.tauTrgSF_ditau = ROOT.SFProvider(
                    "{}/data/2016_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "ditau", "Medium")
                self.tauTrgSF_mutau = ROOT.SFProvider(
                    "{}/data/2016_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "mutau", "Medium")
                self.tauTrgSF_etau = None
                self.tauTrgSF_vbf = None
                self.jetTrgSF_vbf = None
            elif self.year == 2017:
                self.eTrgSF.init_ScaleFactor(
                    "{}/data/Electron/Run2017/Electron_Ele32orEle35_fix.root".format(base))
                self.eTauTrgSF.init_ScaleFactor(
                    "{}/data/Electron/Run2017/Electron_EleTau_Ele24_fix.root".format(base))
                self.muTrgSF.init_ScaleFactor(
                    "{}/data/Muon/Run2017/Muon_IsoMu24orIsoMu27.root".format(base))
                self.muTauTrgSF.init_ScaleFactor(
                    "{}/data/Muon/Run2017/Muon_MuTau_IsoMu20.root".format(base))
                self.tauTrgSF_ditau = ROOT.tau_trigger.SFProvider(
                    "{}/data/2017_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "ditau", "Medium")
                self.tauTrgSF_mutau = ROOT.tau_trigger.SFProvider(
                    "{}/data/2017_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "mutau", "Medium")
                self.tauTrgSF_etau = ROOT.tau_trigger.SFProvider(
                    "{}/data/2017_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "etau", "Medium")
                self.tauTrgSF_vbf = ROOT.tau_trigger.SFProvider(
                    "{}/data/2017_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "ditauvbf", "Medium")
                f = "{}/data/2017_VBFHTauTauTrigger_JetLegs.root".format(base_tau)
                tf = ROOT.TFile.Open(f)
                self.jetTrgSF_vbf = copy(tf.Get("SF_mjj_pT1_pT2"))
            elif self.year == 2018:
                self.eTrgSF.init_ScaleFactor(
                    "{}/data/Electron/Run2018/Electron_Run2018_Ele32orEle35.root".format(base))
                self.eTauTrgSF.init_ScaleFactor(
                    "{}/data/Electron/Run2018/Electron_Run2018_Ele24.root".format(base))
                self.muTrgSF.init_ScaleFactor(
                    "{}/data/Muon/Run2018/Muon_Run2018_IsoMu24orIsoMu27.root".format(base))
                self.muTauTrgSF.init_ScaleFactor(
                    "{}/data/Muon/Run2018/Muon_Run2018_IsoMu20.root".format(base))
                self.tauTrgSF_ditau = ROOT.tau_trigger.SFProvider(
                    "{}/data/2018_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "ditau", "Medium")
                self.tauTrgSF_mutau = ROOT.tau_trigger.SFProvider(
                    "{}/data/2018_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "mutau", "Medium")
                self.tauTrgSF_etau = ROOT.tau_trigger.SFProvider(
                    "{}/data/2018_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "etau", "Medium")
                self.tauTrgSF_vbf = ROOT.tau_trigger.SFProvider(
                    "{}/data/2018_tauTriggerEff_DeepTau2017v2p1.root".format(base_tau),
                    "ditauvbf", "Medium")
                f = "{}/data/2018_VBFHTauTauTrigger_JetLegs.root".format(base_tau)
                tf = ROOT.TFile.Open(f)
                self.jetTrgSF_vbf = copy(tf.Get("SF_mjj_pT1_pT2"))
        pass

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        self.out.branch('trigSF', 'F')
        self.out.branch('trigSF_single', 'F')
        self.out.branch('trigSF_cross', 'F')
        self.out.branch('trigSF_muUp', 'F')
        self.out.branch('trigSF_muDown', 'F')
        self.out.branch('trigSF_eleUp', 'F')
        self.out.branch('trigSF_eleDown', 'F')
        self.out.branch('trigSF_DM0Up', 'F')
        self.out.branch('trigSF_DM1Up', 'F')
        self.out.branch('trigSF_DM10Up', 'F')
        self.out.branch('trigSF_DM11Up', 'F')
        self.out.branch('trigSF_DM0Down', 'F')
        self.out.branch('trigSF_DM1Down', 'F')
        self.out.branch('trigSF_DM10Down', 'F')
        self.out.branch('trigSF_DM11Down', 'F')
        self.out.branch('trigSF_vbfjetUp', 'F')
        self.out.branch('trigSF_vbfjetDown', 'F')
        

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")

        dau1, dau2, dau1_tlv, dau2_tlv = self.get_daus(event, muons, electrons, taus)
        decaymodes = [0, 1, 10, 11]
        if event.pairType == 0 and self.isMC:
            # eta region covered both by cross-trigger and single lepton trigger
            if abs(dau2_tlv.Eta()) < 2.1:
                passSingle = (1 if dau1_tlv.Pt() >= self.mutau_pt_th1 else 0)
                passCross = (1 if dau2_tlv.Pt() >= self.mutau_pt_th2 else 0)

                # lepton trigger
                SFL_Data = self.muTrgSF.get_EfficiencyData(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFL_MC = self.muTrgSF.get_EfficiencyMC(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFL_Data_Err = self.muTrgSF.get_EfficiencyDataError(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFL_MC_Err = self.muTrgSF.get_EfficiencyMCError(dau1_tlv.Pt(), dau1_tlv.Eta())

                SFL_Data = [SFL_Data - 1. * SFL_Data_Err, SFL_Data, SFL_Data + 1. * SFL_Data_Err]
                SFL_MC = [SFL_MC - 1. * SFL_MC_Err, SFL_MC, SFL_MC + 1. * SFL_MC_Err]
                
                # cross trigger
                # mu leg
                SFl_Data = self.muTauTrgSF.get_EfficiencyData(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFl_MC = self.muTauTrgSF.get_EfficiencyMC(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFl_Data_Err = self.muTauTrgSF.get_EfficiencyDataError(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFl_MC_Err = self.muTauTrgSF.get_EfficiencyMCError(dau1_tlv.Pt(), dau1_tlv.Eta())

                SFl_Data = [SFl_Data - 1. * SFl_Data_Err, SFl_Data, SFl_Data + 1. * SFl_Data_Err]
                SFl_MC = [SFl_MC - 1. * SFl_MC_Err, SFl_MC, SFl_MC + 1. * SFl_MC_Err]

                # tau leg
                SFtau_Data = self.tauTrgSF_mutau.getEfficiencyData(dau2_tlv.Pt(), dau2.decayMode, 0)
                SFtau_MC = self.tauTrgSF_mutau.getEfficiencyMC(dau2_tlv.Pt(), dau2.decayMode, 0)

                # efficiencies            
                Eff_Data_mu =\
                    [passSingle * SFL_Data[i] - passCross * passSingle * min([SFl_Data[i], SFL_Data[i]])
                    * SFtau_Data + passCross * SFl_Data[i] * SFtau_Data for i in range(3)]
                Eff_MC_mu =\
                    [passSingle * SFL_MC[i] - passCross * passSingle * min([SFl_MC[i], SFL_MC[i]])
                    * SFtau_MC + passCross * SFl_MC[i] * SFtau_MC for i in range(3)]

                SFtau_Data_tauup = [SFtau_Data for i in range(len(decaymodes))]
                SFtau_Data_taudown = [SFtau_Data for i in range(len(decaymodes))]
                SFtau_MC_tauup = [SFtau_MC for i in range(len(decaymodes))]
                SFtau_MC_taudown = [SFtau_MC for i in range(len(decaymodes))]
                Eff_Data_tauup = [Eff_Data_mu for i in range(len(decaymodes))]
                Eff_Data_taudown = [Eff_Data_mu for i in range(len(decaymodes))]
                Eff_MC_tauup = [Eff_MC_mu for i in range(len(decaymodes))]
                Eff_MC_taudown = [Eff_MC_mu for i in range(len(decaymodes))]
                
                for idm, dm in enumerate(decaymodes):
                    if dm == int(dau2.decayMode):
                        SFtau_Data_tauup[idm] = self.tauTrgSF_mutau.getEfficiencyData(
                            dau2_tlv.Pt(), dau2.decayMode, 1)
                        SFtau_Data_taudown[idm] = self.tauTrgSF_mutau.getEfficiencyData(
                            dau2_tlv.Pt(), dau2.decayMode, -1)
                        SFtau_MC_tauup[idm] = self.tauTrgSF_mutau.getEfficiencyMC(
                            dau2_tlv.Pt(), dau2.decayMode, 1)
                        SFtau_MC_taudown[idm] = self.tauTrgSF_mutau.getEfficiencyMC(
                            dau2_tlv.Pt(), dau2.decayMode, -1)
                    
                Eff_Data_tauup = [
                    passSingle * SFL_Data[1] - passCross * passSingle * min([SFl_Data[1], SFL_Data[1]])
                        * SFtau_Data_tauup[1] + passCross * SFl_Data[1] * SFtau_Data_tauup[idm]
                    for idm in range(len(decaymodes))]
                Eff_Data_taudown = [
                    passSingle * SFL_Data[1] - passCross * passSingle * min([SFl_Data[1], SFL_Data[1]])
                        * SFtau_Data_taudown[1] + passCross * SFl_Data[1] * SFtau_Data_taudown[idm]
                    for idm in range(len(decaymodes))]
                Eff_MC_tauup = [
                    passSingle * SFL_MC[1] - passCross * passSingle * min([SFl_MC[1], SFL_MC[1]])
                        * SFtau_MC_tauup[idm] + passCross * SFl_MC[1] * SFtau_MC_tauup[idm]
                    for i in range(len(decaymodes))]
                Eff_MC_taudown = [
                    passSingle * SFL_MC[1] - passCross * passSingle * min([SFl_MC[1], SFL_MC[1]])
                        * SFtau_MC_taudown[idm] + passCross * SFl_MC[1] * SFtau_MC_taudown[idm]
                    for i in range(len(decaymodes))]

                trigSF_mu = [Eff_Data_mu[i] / Eff_MC_mu[i] for i in range(len(Eff_Data_mu))]
                trigSF_tauup = [Eff_Data_tauup[i] / Eff_MC_tauup[i] for i in range(len(Eff_Data_tauup))]
                trigSF_taudown = [Eff_Data_taudown[i] / Eff_MC_taudown[i] for i in range(len(Eff_Data_taudown))]
                # trig SF for analysis only with cross-trigger
                SFl = self.muTauTrgSF.get_ScaleFactor(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFtau = self.tauTrgSF_mutau.getSF(dau2_tlv.Pt(), dau2.decayMode, 0)
                trigSF_cross = SFl * SFtau
            
            else:
                SF = self.muTrgSF.get_ScaleFactor(dau1_tlv.Pt(), dau1_tlv.Eta())
                SF_error = self.muTrgSF.get_ScaleFactorError(dau1_tlv.Pt(), dau1_tlv.Eta())
                trigSF_mu = [SF - 1. * SF_error, SF, SF + 1. * SF_error]
                trigSF_cross = SF
                trigSF_tauup = [SF for i in range(len(decaymodes))]
                trigSF_taudown = [SF for i in range(len(decaymodes))]

            # trig SF for analysis only with single-e trigger
            trigSF_single = self.muTrgSF.get_ScaleFactor(dau1_tlv.Pt(), dau1_tlv.Eta())
            

            self.out.fillBranch('trigSF', trigSF_mu[1])
            self.out.fillBranch('trigSF_single', trigSF_single)
            self.out.fillBranch('trigSF_cross', trigSF_cross)
            self.out.fillBranch('trigSF_muUp', trigSF_mu[2])
            self.out.fillBranch('trigSF_muDown', trigSF_mu[0])
            self.out.fillBranch('trigSF_eleUp', trigSF_mu[1])
            self.out.fillBranch('trigSF_eleDown', trigSF_mu[1])
            self.out.fillBranch('trigSF_DM0Up', trigSF_tauup[0])
            self.out.fillBranch('trigSF_DM1Up', trigSF_tauup[1])
            self.out.fillBranch('trigSF_DM10Up', trigSF_tauup[2])
            self.out.fillBranch('trigSF_DM11Up', trigSF_tauup[3])
            self.out.fillBranch('trigSF_DM0Down', trigSF_taudown[0])
            self.out.fillBranch('trigSF_DM1Down', trigSF_taudown[1])
            self.out.fillBranch('trigSF_DM10Down', trigSF_taudown[2])
            self.out.fillBranch('trigSF_DM11Down', trigSF_taudown[3])
            self.out.fillBranch('trigSF_vbfjetUp', trigSF_mu[1])
            self.out.fillBranch('trigSF_vbfjetDown', trigSF_mu[1])
            return True

        elif event.pairType == 1 and self.isMC:
            # eta region covered both by cross-trigger and single lepton trigger
            # in 2016 there is no cross etau trigger
            if abs(dau2_tlv.Eta()) < 2.1 and self.year != 2016:
                passSingle = (1 if dau1_tlv.Pt() >= self.etau_pt_th1 else 0)
                passCross = (1 if dau2_tlv.Pt() >= self.etau_pt_th2 else 0)

                # lepton trigger
                SFL_Data = self.eTrgSF.get_EfficiencyData(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFL_MC = self.eTrgSF.get_EfficiencyMC(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFL_Data_Err = self.eTrgSF.get_EfficiencyDataError(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFL_MC_Err = self.eTrgSF.get_EfficiencyMCError(dau1_tlv.Pt(), dau1_tlv.Eta())

                SFL_Data = [SFL_Data - 1. * SFL_Data_Err, SFL_Data, SFL_Data + 1. * SFL_Data_Err]
                SFL_MC = [SFL_MC - 1. * SFL_MC_Err, SFL_MC, SFL_MC + 1. * SFL_MC_Err]
                
                # cross trigger
                # e leg
                SFl_Data = self.eTauTrgSF.get_EfficiencyData(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFl_MC = self.eTauTrgSF.get_EfficiencyMC(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFl_Data_Err = self.eTauTrgSF.get_EfficiencyDataError(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFl_MC_Err = self.eTauTrgSF.get_EfficiencyMCError(dau1_tlv.Pt(), dau1_tlv.Eta())

                SFl_Data = [SFl_Data - 1. * SFl_Data_Err, SFl_Data, SFl_Data + 1. * SFl_Data_Err]
                SFl_MC = [SFl_MC - 1. * SFl_MC_Err, SFl_MC, SFl_MC + 1. * SFl_MC_Err]

                # tau leg
                SFtau_Data = self.tauTrgSF_etau.getEfficiencyData(dau2_tlv.Pt(), dau2.decayMode, 0)
                SFtau_MC = self.tauTrgSF_etau.getEfficiencyMC(dau2_tlv.Pt(), dau2.decayMode, 0)

                # efficiencies            
                Eff_Data_e =\
                    [passSingle * SFL_Data[i]
                        - passCross * passSingle * min([SFl_Data[i], SFL_Data[i]]) * SFtau_Data
                        + passCross * SFl_Data[i] * SFtau_Data for i in range(3)]
                Eff_MC_e =\
                    [passSingle * SFL_MC[i]
                        - passCross * passSingle * min([SFl_MC[i], SFL_MC[i]]) * SFtau_MC
                        + passCross * SFl_MC[i] * SFtau_MC for i in range(3)]

                SFtau_Data_tauup = [SFtau_Data for i in range(len(decaymodes))]
                SFtau_Data_taudown = [SFtau_Data for i in range(len(decaymodes))]
                SFtau_MC_tauup = [SFtau_MC for i in range(len(decaymodes))]
                SFtau_MC_taudown = [SFtau_MC for i in range(len(decaymodes))]
                Eff_Data_tauup = [Eff_Data_e for i in range(len(decaymodes))]
                Eff_Data_taudown = [Eff_Data_e for i in range(len(decaymodes))]
                Eff_MC_tauup = [Eff_MC_e for i in range(len(decaymodes))]
                Eff_MC_taudown = [Eff_MC_e for i in range(len(decaymodes))]
                
                for idm, dm in enumerate(decaymodes):
                    if dm == int(dau2.decayMode):
                        SFtau_Data_tauup[idm] = self.tauTrgSF_etau.getEfficiencyData(
                            dau2_tlv.Pt(), dau2.decayMode, 1)
                        SFtau_Data_taudown[idm] = self.tauTrgSF_etau.getEfficiencyData(
                            dau2_tlv.Pt(), dau2.decayMode, -1)
                        SFtau_MC_tauup[idm] = self.tauTrgSF_etau.getEfficiencyMC(
                            dau2_tlv.Pt(), dau2.decayMode, 1)
                        SFtau_MC_taudown[idm] = self.tauTrgSF_etau.getEfficiencyMC(
                            dau2_tlv.Pt(), dau2.decayMode, -1)
                    
                Eff_Data_tauup = [
                    passSingle * SFL_Data[1]
                        - passCross * passSingle * min([SFl_Data[1], SFL_Data[1]]) * SFtau_Data_tauup[1]
                        + passCross * SFl_Data[1] * SFtau_Data_tauup[idm]
                    for idm in range(len(decaymodes))]
                Eff_Data_taudown = [
                    passSingle * SFL_Data[1]
                        - passCross * passSingle * min([SFl_Data[1], SFL_Data[1]]) * SFtau_Data_taudown[1]
                        + passCross * SFl_Data[1] * SFtau_Data_taudown[idm]
                    for idm in range(len(decaymodes))]
                Eff_MC_tauup = [
                    passSingle * SFL_MC[1]
                        - passCross * passSingle * min([SFl_MC[1], SFL_MC[1]]) * SFtau_MC_tauup[idm]
                        + passCross * SFl_MC[1] * SFtau_MC_tauup[idm]
                    for i in range(len(decaymodes))]
                Eff_MC_taudown = [
                    passSingle * SFL_MC[1]
                        - passCross * passSingle * min([SFl_MC[1], SFL_MC[1]]) * SFtau_MC_taudown[idm]
                        + passCross * SFl_MC[1] * SFtau_MC_taudown[idm]
                    for i in range(len(decaymodes))]

                trigSF_e = [Eff_Data_e[i] / Eff_MC_e[i] for i in range(len(Eff_Data_e))]
                trigSF_tauup = [Eff_Data_tauup[i] / Eff_MC_tauup[i] for i in range(len(Eff_Data_tauup))]
                trigSF_taudown = [Eff_Data_taudown[i] / Eff_MC_taudown[i]
                    for i in range(len(Eff_Data_taudown))]
                # trig SF for analysis only with cross-trigger
                SFl = self.eTauTrgSF.get_ScaleFactor(dau1_tlv.Pt(), dau1_tlv.Eta())
                SFtau = self.tauTrgSF_etau.getSF(dau2_tlv.Pt(), dau2.decayMode, 0)
                trigSF_cross = SFl * SFtau

            else:
                SF = self.eTrgSF.get_ScaleFactor(dau1_tlv.Pt(), dau1_tlv.Eta())
                SF_error = self.eTrgSF.get_ScaleFactorError(dau1_tlv.Pt(), dau1_tlv.Eta())
                trigSF_e = [SF - 1. * SF_error, SF, SF + 1. * SF_error]
                trigSF_cross = SF
                trigSF_tauup = [SF for i in range(len(decaymodes))]
                trigSF_taudown = [SF for i in range(len(decaymodes))]

            # trig SF for analysis only with single-e trigger
            trigSF_single = self.eTrgSF.get_ScaleFactor(dau1_tlv.Pt(), dau1_tlv.Eta())
            
            self.out.fillBranch('trigSF', trigSF_e[1])
            self.out.fillBranch('trigSF_single', trigSF_single)
            self.out.fillBranch('trigSF_cross', trigSF_cross)
            self.out.fillBranch('trigSF_muUp', trigSF_e[1])
            self.out.fillBranch('trigSF_muDown', trigSF_e[1])
            self.out.fillBranch('trigSF_eleUp', trigSF_e[2])
            self.out.fillBranch('trigSF_eleDown', trigSF_e[0])
            self.out.fillBranch('trigSF_DM0Up', trigSF_tauup[0])
            self.out.fillBranch('trigSF_DM1Up', trigSF_tauup[1])
            self.out.fillBranch('trigSF_DM10Up', trigSF_tauup[2])
            self.out.fillBranch('trigSF_DM11Up', trigSF_tauup[3])
            self.out.fillBranch('trigSF_DM0Down', trigSF_taudown[0])
            self.out.fillBranch('trigSF_DM1Down', trigSF_taudown[1])
            self.out.fillBranch('trigSF_DM10Down', trigSF_taudown[2])
            self.out.fillBranch('trigSF_DM11Down', trigSF_taudown[3])
            self.out.fillBranch('trigSF_vbfjetUp', trigSF_e[1])
            self.out.fillBranch('trigSF_vbfjetDown', trigSF_e[1])
            return True

        elif event.pairType == 2 and self.isMC and event.isVBFtrigger == 0:
            
            SF1 = self.tauTrgSF_ditau.getSF(dau1_tlv.Pt(), dau1.decayMode, 0)
            SF2 = self.tauTrgSF_ditau.getSF(dau2_tlv.Pt(), dau2.decayMode, 0)
            
            SF1_tauup = [SF1 for i in range(len(decaymodes))]
            SF1_taudown = [SF1 for i in range(len(decaymodes))]
            SF2_tauup = [SF2 for i in range(len(decaymodes))]
            SF2_taudown = [SF2 for i in range(len(decaymodes))]

            for idm, dm in enumerate(decaymodes):
                if dm == int(dau1.decayMode):
                    SF1_tauup[idm] = self.tauTrgSF_ditau.getSF(dau1_tlv.Pt(), dau1.decayMode, 1)
                    SF1_taudown[idm] = self.tauTrgSF_ditau.getSF(dau1_tlv.Pt(), dau1.decayMode, -1)
                if dm == int(dau2.decayMode):
                    SF2_tauup[idm] = self.tauTrgSF_ditau.getSF(dau2_tlv.Pt(), dau2.decayMode, 1)
                    SF2_taudown[idm] = self.tauTrgSF_ditau.getSF(dau2_tlv.Pt(), dau2.decayMode, -1)

            trigSF = SF1 * SF2
            trigSF_tauup = [SF1_tauup[i] * SF2_tauup[i] for i in range(len(SF1_tauup))]
            trigSF_taudown = [SF1_taudown[i] * SF2_taudown[i] for i in range(len(SF1_taudown))]
            
            self.out.fillBranch('trigSF', trigSF)
            self.out.fillBranch('trigSF_single', trigSF)
            self.out.fillBranch('trigSF_cross', trigSF)
            self.out.fillBranch('trigSF_muUp', trigSF)
            self.out.fillBranch('trigSF_muDown', trigSF)
            self.out.fillBranch('trigSF_eleUp', trigSF)
            self.out.fillBranch('trigSF_eleDown', trigSF)
            self.out.fillBranch('trigSF_DM0Up', trigSF_tauup[0])
            self.out.fillBranch('trigSF_DM1Up', trigSF_tauup[1])
            self.out.fillBranch('trigSF_DM10Up', trigSF_tauup[2])
            self.out.fillBranch('trigSF_DM11Up', trigSF_tauup[3])
            self.out.fillBranch('trigSF_DM0Down', trigSF_taudown[0])
            self.out.fillBranch('trigSF_DM1Down', trigSF_taudown[1])
            self.out.fillBranch('trigSF_DM10Down', trigSF_taudown[2])
            self.out.fillBranch('trigSF_DM11Down', trigSF_taudown[3])
            self.out.fillBranch('trigSF_vbfjetUp', trigSF)
            self.out.fillBranch('trigSF_vbfjetDown', trigSF)
            return True

        elif event.pairType == 2 and self.isMC and self.year != 2016:
            jets = Collection(event, "Jet")
            vbfjet1, vbfjet2, vbfjet1_tlv, vbfjet2_tlv = self.get_vbfjets(event, jets)
            
            
            if vbfjet1 and vbfjet2:
                vbfjj_mass = (vbfjet1_tlv + vbfjet2_tlv).M()
                if (vbfjet1_tlv.Pt() > 140 and vbfjet2_tlv.Pt() > 60 and vbfjj_mass > 800
                        and dau1_tlv.Pt() > 25 and dau2_tlv.Pt() > 25
                        and (dau1_tlv.Pt() <= 40 or dau2_tlv.Pt() <= 40)):
                    # jet leg sf
                    jetSF = getContentHisto3D(
                        self.jetTrgSF_vbf, vbfjj_mass, vbfjet1_tlv.Pt(), vbfjet2_tlv.Pt(), 0)
                    jetSFerror = getContentHisto3D(
                        self.jetTrgSF_vbf, vbfjj_mass, vbfjet1_tlv.Pt(), vbfjet2_tlv.Pt(), 1)
                    # tau leg sf
                    SF1 = self.tauTrgSF_vbf.getSF(dau1_tlv.Pt(), dau1.decayMode, 0)
                    SF2 = self.tauTrgSF_vbf.getSF(dau2_tlv.Pt(), dau2.decayMode, 0)

                    SF1_tauup = [SF1 for i in range(len(decaymodes))]
                    SF1_taudown = [SF1 for i in range(len(decaymodes))]
                    SF2_tauup = [SF2 for i in range(len(decaymodes))]
                    SF2_taudown = [SF2 for i in range(len(decaymodes))]

                    for idm, dm in enumerate(decaymodes):
                        if dm == int(dau1.decayMode):
                            SF1_tauup[idm] = self.tauTrgSF_vbf.getSF(dau1_tlv.Pt(), dau1.decayMode, 1)
                            SF1_taudown[idm] = self.tauTrgSF_vbf.getSF(dau1_tlv.Pt(), dau1.decayMode, -1)
                        if dm == int(dau2.decayMode):
                            SF2_tauup[idm] = self.tauTrgSF_vbf.getSF(dau2_tlv.Pt(), dau2.decayMode, 1)
                            SF2_taudown[idm] = self.tauTrgSF_vbf.getSF(dau2_tlv.Pt(), dau2.decayMode, -1)

                    trigSF_vbfjet = [
                        (jetSF - jetSFerror) * SF1 * SF2,
                        jetSF * SF1 * SF2,
                        (jetSF + jetSFerror) * SF1 * SF2
                    ]
                    
                    trigSF_tauup = [jetSF * SF1_tauup[i] * SF2_tauup[i] for i in range(len(SF1_tauup))]
                    trigSF_taudown = [jetSF * SF1_taudown[i] * SF2_taudown[i] for i in range(len(SF1_taudown))]
                    
                    self.out.fillBranch('trigSF', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_single', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_cross', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_muUp', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_muDown', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_eleUp', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_eleDown', trigSF_vbfjet[1])
                    self.out.fillBranch('trigSF_DM0Up', trigSF_tauup[0])
                    self.out.fillBranch('trigSF_DM1Up', trigSF_tauup[1])
                    self.out.fillBranch('trigSF_DM10Up', trigSF_tauup[2])
                    self.out.fillBranch('trigSF_DM11Up', trigSF_tauup[3])
                    self.out.fillBranch('trigSF_DM0Down', trigSF_taudown[0])
                    self.out.fillBranch('trigSF_DM1Down', trigSF_taudown[1])
                    self.out.fillBranch('trigSF_DM10Down', trigSF_taudown[2])
                    self.out.fillBranch('trigSF_DM11Down', trigSF_taudown[3])
                    self.out.fillBranch('trigSF_vbfjetUp', trigSF_vbfjet[2])
                    self.out.fillBranch('trigSF_vbfjetDown', trigSF_vbfjet[0])
                    return True

        elif not self.isMC:
            self.out.fillBranch('trigSF', 1.)
            self.out.fillBranch('trigSF_single', 1.)
            self.out.fillBranch('trigSF_cross', 1.)
            self.out.fillBranch('trigSF_muUp', 1.)
            self.out.fillBranch('trigSF_muDown', 1.)
            self.out.fillBranch('trigSF_eleUp', 1.)
            self.out.fillBranch('trigSF_eleDown', 1.)
            self.out.fillBranch('trigSF_DM0Up', 1.)
            self.out.fillBranch('trigSF_DM1Up', 1.)
            self.out.fillBranch('trigSF_DM10Up', 1.)
            self.out.fillBranch('trigSF_DM11Up', 1.)
            self.out.fillBranch('trigSF_DM0Down', 1.)
            self.out.fillBranch('trigSF_DM1Down', 1.)
            self.out.fillBranch('trigSF_DM10Down', 1.)
            self.out.fillBranch('trigSF_DM11Down', 1.)
            self.out.fillBranch('trigSF_vbfjetUp', 1.)
            self.out.fillBranch('trigSF_vbfjetDown', 1.)
            return True
        else:
            self.out.fillBranch('trigSF', 0.)
            self.out.fillBranch('trigSF_single', 0.)
            self.out.fillBranch('trigSF_cross', 0.)
            self.out.fillBranch('trigSF_muUp', 0.)
            self.out.fillBranch('trigSF_muDown', 0.)
            self.out.fillBranch('trigSF_eleUp', 0.)
            self.out.fillBranch('trigSF_eleDown', 0.)
            self.out.fillBranch('trigSF_DM0Up', 0.)
            self.out.fillBranch('trigSF_DM1Up', 0.)
            self.out.fillBranch('trigSF_DM10Up', 0.)
            self.out.fillBranch('trigSF_DM11Up', 0.)
            self.out.fillBranch('trigSF_DM0Down', 0.)
            self.out.fillBranch('trigSF_DM1Down', 0.)
            self.out.fillBranch('trigSF_DM10Down', 0.)
            self.out.fillBranch('trigSF_DM11Down', 0.)
            self.out.fillBranch('trigSF_vbfjetUp', 0.)
            self.out.fillBranch('trigSF_vbfjetDown', 0.)
            return True


def Htt_trigSF(**kwargs):
    return lambda: Htt_trigSFProducer(**kwargs)
