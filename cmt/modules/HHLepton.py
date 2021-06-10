from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root
from cmt.modules.tau_utils import LeptonTauPair, TriggerChecker, lepton_veto

ROOT = import_root()

class HHLeptonProducer(Module):
    def __init__(self, isMC, year, runPeriod):
        self.isMC = isMC
        self.year = year
        self.runPeriod = runPeriod
        self.trigger_checker = TriggerChecker()

        if self.year == 2016:
            self.trigger_checker.mutau_triggers = ["HLT_IsoMu22", "HLT_IsoMu22_eta2p1",
                "HLT_IsoTkMu22", "HLT_IsoTkMu22_eta2p1"]
            self.trigger_checker.mutau_crosstriggers = ["HLT_IsoMu19_eta2p1_LooseIsoPFTau20",
                "HLT_IsoMu19_eta2p1_LooseIsoPFTau20_SingleL1"]
            self.trigger_checker.etau_triggers = ["HLT_Ele25_eta2p1_WPTight_Gsf"]
            self.trigger_checker.etau_crosstriggers = []
            if not self.isMC:
                if self.runPeriod != "H":
                    self.trigger_checker.tautau_triggers = [
                        "HLT_DoubleMediumIsoPFTau35_Trk1_eta2p1_Reg"]
                else:
                    self.trigger_checker.tautau_triggers = [
                        "HLT_DoubleMediumCombinedIsoPFTau35_Trk1_eta2p1_Reg"]
            else:
                self.trigger_checker.tautau_triggers = [
                    "HLT_DoubleMediumIsoPFTau35_Trk1_eta2p1_Reg",
                    "HLT_DoubleMediumCombinedIsoPFTau35_Trk1_eta2p1_Reg"]
            self.trigger_checker.vbf_triggers = []

        elif self.year == 2017:
            self.trigger_checker.mutau_triggers = ["HLT_IsoMu24", "HLT_IsoMu27"]
            self.trigger_checker.mutau_crosstriggers = [
                "HLT_IsoMu20_eta2p1_LooseChargedIsoPFTau27_eta2p1_CrossL1"]
            self.trigger_checker.etau_triggers = ["HLT_Ele32_WPTight_Gsf_L1DoubleEG",
                "HLT_Ele32_WPTight_Gsf", "HLT_Ele35_WPTight_Gsf"]
            self.trigger_checker.etau_crosstriggers = [
                "HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTau30_eta2p1_CrossL1"]
            self.trigger_checker.tautau_triggers = [
                "HLT_DoubleTightChargedIsoPFTau35_Trk1_TightID_eta2p1_Reg",
                "HLT_DoubleMediumChargedIsoPFTau40_Trk1_TightID_eta2p1_Reg",
                "HLT_DoubleTightChargedIsoPFTau40_Trk1_eta2p1_Reg"]
            if self.runPeriod in ["D", "E", "F"] or self.isMC:
                self.trigger_checker.vbf_triggers = [
                    "HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_Reg"]
            else:
                self.trigger_checker.vbf_triggers = []

        elif self.year == 2018:
            self.trigger_checker.mutau_triggers = ["HLT_IsoMu24", "HLT_IsoMu27"]
            # lista = lambda e: ([1, 1] if e == 0 else [2, 2])
            self.trigger_checker.mutau_crosstriggers = lambda e: (
                ["HLT_IsoMu20_eta2p1_LooseChargedIsoPFTau27_eta2p1_CrossL1"]
                    if (e.run < 317509 and not self.isMC)
                    else ["HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1"])
            self.trigger_checker.etau_triggers = ["HLT_Ele32_WPTight_Gsf", "HLT_Ele35_WPTight_Gsf"]
            self.trigger_checker.etau_crosstriggers = lambda e: (
                ["HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTau30_eta2p1_CrossL1"]
                    if (e.run < 317509 and not self.isMC)
                    else ["HLT_Ele24_eta2p1_WPTight_Gsf_LooseChargedIsoPFTauHPS30_eta2p1_CrossL1"])
            self.trigger_checker.tautau_triggers = lambda e: (
                ["HLT_DoubleTightChargedIsoPFTau35_Trk1_TightID_eta2p1_Reg",
                "HLT_DoubleMediumChargedIsoPFTau40_Trk1_TightID_eta2p1_Reg",
                "HLT_DoubleTightChargedIsoPFTau40_Trk1_eta2p1_Reg"]
                    if (e.run < 317509 and not self.isMC)
                    else ["HLT_IsoMu20_eta2p1_LooseChargedIsoPFTauHPS27_eta2p1_CrossL1"])
            self.trigger_checker.vbf_triggers = lambda e: (
                ["HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1"]
                    if (e.run < 317509 and not self.isMC)
                    else ["HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1"])
                
        pass

    #def beginJob(self):
    #    pass

    #def endJob(self):
    #    pass

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        self.out.branch('pairType', 'I')
    
        self.out.branch('dau1_pt', 'F')
        self.out.branch('dau1_eta', 'F')
        self.out.branch('dau1_phi', 'F')
        self.out.branch('dau1_mass', 'F')
        self.out.branch('dau1_dxy', 'F')
        self.out.branch('dau1_dz', 'F')
        self.out.branch('dau1_q', 'I')
        self.out.branch('dau1_iso', 'F')
        self.out.branch('dau1_idDecayModeNewDMs', 'b')
        self.out.branch('dau1_idDeepTau2017v2p1VSe', 'I')
        self.out.branch('dau1_idDeepTau2017v2p1VSmu', 'I')
        self.out.branch('dau1_idDeepTau2017v2p1VSjet', 'I')

        self.out.branch('dau2_pt', 'F')
        self.out.branch('dau2_eta', 'F')
        self.out.branch('dau2_phi', 'F')
        self.out.branch('dau2_mass', 'F')
        self.out.branch('dau2_dxy', 'F')
        self.out.branch('dau2_dz', 'F')
        self.out.branch('dau2_q', 'I')
        self.out.branch('dau2_iso', 'F')
        self.out.branch('dau2_idDecayModeNewDMs', 'b')
        self.out.branch('dau2_idDeepTau2017v2p1VSe', 'I')
        self.out.branch('dau2_idDeepTau2017v2p1VSmu', 'I')
        self.out.branch('dau2_idDeepTau2017v2p1VSjet', 'I')

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        electrons = Collection(event, "Electron")
        muons = Collection(event, "Muon")
        taus = Collection(event, "Tau")

        # muon-tau channels
        goodmuons = []
        for muon in muons:
            if (abs(muon.eta) > 2.1 or muon.jetRelIso > 0.15 or abs(muon.dxy) > 0.045
                    or abs(muon.dz) > 0.2 or not muon.tightId):
                continue
            goodmuons.append(muon)
        if goodmuons:
            goodtaus = []
            for tau in taus:
                if tau.idDeepTau2017v2p1VSmu < 15 or tau.idDeepTau2017v2p1VSe < 7:
                    continue
                if tau.decayMode not in [0, 1, 10, 11]:
                    continue
                goodtaus.append(tau)
            
            muontaupairs = []
            for muon in goodmuons:
                for tau in goodtaus:
                    if tau.DeltaR(muon) < 0.5: continue
                    if not self.trigger_checker.check_mutau(event,
                            muon.pt, muon.eta, tau.pt, tau.eta, th1=1, th2=5):
                        continue
                    muontaupair = LeptonTauPair(muon, muon.pfRelIso04_all, tau, tau.rawDeepTau2017v2p1VSjet)
                    if muontaupair.check_charge():
                        muontaupairs.append(muontaupair)

            if len(muontaupairs) != 0:
                muon, tau = max(muontaupairs).pair

                fail_lepton_veto, _ = lepton_veto(electrons, muons, taus, muon)
                if fail_lepton_veto:
                    return False

                self.out.fillBranch("pairType", 0)

                self.out.fillBranch("dau1_pt", muon.pt)
                self.out.fillBranch("dau1_eta", muon.eta)
                self.out.fillBranch("dau1_phi", muon.phi)
                self.out.fillBranch("dau1_mass", muon.mass)
                self.out.fillBranch("dau1_dxy", muon.dxy)
                self.out.fillBranch("dau1_dz", muon.dz)
                self.out.fillBranch("dau1_q", muon.charge)
                self.out.fillBranch("dau1_iso", muon.pfRelIso04_all)
                self.out.fillBranch("dau1_idDecayModeNewDMs", 0)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSe", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSmu", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSjet", -1)

                self.out.fillBranch("dau2_pt", tau.pt)
                self.out.fillBranch("dau2_eta", tau.eta)
                self.out.fillBranch("dau2_phi", tau.phi)
                self.out.fillBranch("dau2_mass", tau.mass)
                self.out.fillBranch("dau2_dxy", tau.dxy)
                self.out.fillBranch("dau2_dz", tau.dz)
                self.out.fillBranch("dau2_q", tau.charge)
                self.out.fillBranch("dau2_iso", tau.rawIso)
                self.out.fillBranch("dau2_idDecayModeNewDMs", tau.idDecayModeNewDMs)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSe", tau.idDeepTau2017v2p1VSe)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSmu", tau.idDeepTau2017v2p1VSmu)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSjet", tau.idDeepTau2017v2p1VSjet)
                return True

        # electron-tau channels
        goodelectrons = []
        for electron in electrons:
            if (not (electron.mvaFall17V2Iso_WP80 or electron.mvaFall17V2noIso_WP80)
                    or abs(electron.dxy) > 0.045 or abs(electron.dz) > 0.2):
                continue
            goodelectrons.append(electron)
        if goodelectrons:
            goodtaus = []
            for tau in taus:
                if tau.idDeepTau2017v2p1VSmu < 15 or tau.idDeepTau2017v2p1VSe < 7:
                    continue
                if tau.dz > 0.2:
                    continue
                if tau.decayMode not in [0, 1, 10, 11]:
                    continue
                goodtaus.append(tau)

            electrontaupairs = []
            for electron in goodelectrons:
                for tau in goodtaus:
                    if tau.DeltaR(electron) < 0.5: continue
                    if not self.trigger_checker.check_etau(event,
                            electron.pt, electron.eta, tau.pt, tau.eta, th1=1, th2=5):
                        continue
                    electrontaupair = LeptonTauPair(electron, electron.pfRelIso03_all, tau,
                        tau.rawDeepTau2017v2p1VSjet)
                    if electrontaupair.check_charge():
                        electrontaupairs.append(electrontaupair)

            if len(electrontaupairs) != 0:
                electron, tau = max(electrontaupairs).pair

                fail_lepton_veto, _ = lepton_veto(electrons, muons, taus, electron)
                if fail_lepton_veto:
                    return False

                self.out.fillBranch("pairType", 1)

                self.out.fillBranch("dau1_pt", electron.pt)
                self.out.fillBranch("dau1_eta", electron.eta)
                self.out.fillBranch("dau1_phi", electron.phi)
                self.out.fillBranch("dau1_mass", electron.mass)
                self.out.fillBranch("dau1_dxy", electron.dxy)
                self.out.fillBranch("dau1_dz", electron.dz)
                self.out.fillBranch("dau1_q", electron.charge)
                self.out.fillBranch("dau1_iso", electron.pfRelIso03_all)
                self.out.fillBranch("dau1_idDecayModeNewDMs", 0)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSe", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSmu", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSjet", -1)

                self.out.fillBranch("dau2_pt", tau.pt)
                self.out.fillBranch("dau2_eta", tau.eta)
                self.out.fillBranch("dau2_phi", tau.phi)
                self.out.fillBranch("dau2_mass", tau.mass)
                self.out.fillBranch("dau2_dxy", tau.dxy)
                self.out.fillBranch("dau2_dz", tau.dz)
                self.out.fillBranch("dau2_q", tau.charge)
                self.out.fillBranch("dau2_iso", tau.rawIso)
                self.out.fillBranch("dau2_idDecayModeNewDMs", tau.idDecayModeNewDMs)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSe", tau.idDeepTau2017v2p1VSe)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSmu", tau.idDeepTau2017v2p1VSmu)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSjet", tau.idDeepTau2017v2p1VSjet)

                return True

        goodtaus = []
        for tau in taus:
            if tau.idDeepTau2017v2p1VSmu < 1 or tau.idDeepTau2017v2p1VSe < 3:
                continue
            if tau.dz > 0.2:
                continue
            if tau.decayMode not in [0, 1, 10, 11]:
                continue
            goodtaus.append(tau)
        
        tautaupairs = []
        for i in range(len(goodtaus) - 1):
            for j in range(i + 1, len(goodtaus)):
                firsttau = goodtaus[i]
                secondtau = goodtaus[j]
            if not (
                    self.trigger_checker.check_tautau(event,
                        firsttau.pt, firsttau.eta, secondtau.pt, secondtau.eta,
                        abs_th1=40, abs_th2=40)
                    or self.trigger_checker.check_vbftautau(event,
                        firsttau.pt, firsttau.eta, secondtau.pt, secondtau.eta,
                        abs_th1=25, abs_th2=25)
                    ):
                continue
            tautaupair = LeptonTauPair(firsttau, firsttau.rawDeepTau2017v2p1VSjet,
                secondtau, secondtau.rawDeepTau2017v2p1VSjet)
            if tautaupair.check_charge():
                tautaupairs.append(tautaupair)

        if len(tautaupairs) != 0:
            tau1, tau2 = max(tautaupairs).pair

            fail_lepton_veto, _ = lepton_veto(electrons, muons, taus)
            if fail_lepton_veto:
                return False

            self.out.fillBranch("pairType", 2)

            self.out.fillBranch("dau1_pt", tau1.pt)
            self.out.fillBranch("dau1_eta", tau1.eta)
            self.out.fillBranch("dau1_phi", tau1.phi)
            self.out.fillBranch("dau1_mass", tau1.mass)
            self.out.fillBranch("dau1_dxy", tau1.dxy)
            self.out.fillBranch("dau1_dz", tau1.dz)
            self.out.fillBranch("dau1_q", tau1.charge)
            self.out.fillBranch("dau1_iso", tau1.rawIso)
            self.out.fillBranch("dau1_idDecayModeNewDMs", tau1.idDecayModeNewDMs)
            self.out.fillBranch("dau1_idDeepTau2017v2p1VSe", tau1.idDeepTau2017v2p1VSe)
            self.out.fillBranch("dau1_idDeepTau2017v2p1VSmu", tau1.idDeepTau2017v2p1VSmu)
            self.out.fillBranch("dau1_idDeepTau2017v2p1VSjet", tau1.idDeepTau2017v2p1VSjet)  

            self.out.fillBranch("dau2_pt", tau2.pt)
            self.out.fillBranch("dau2_eta", tau2.eta)
            self.out.fillBranch("dau2_phi", tau2.phi)
            self.out.fillBranch("dau2_mass", tau2.mass)
            self.out.fillBranch("dau2_dxy", tau2.dxy)
            self.out.fillBranch("dau2_dz", tau2.dz)
            self.out.fillBranch("dau2_q", tau2.charge)
            self.out.fillBranch("dau2_iso", tau2.rawIso)
            self.out.fillBranch("dau2_idDecayModeNewDMs", tau2.idDecayModeNewDMs)
            self.out.fillBranch("dau2_idDeepTau2017v2p1VSe", tau2.idDeepTau2017v2p1VSe)
            self.out.fillBranch("dau2_idDeepTau2017v2p1VSmu", tau2.idDeepTau2017v2p1VSmu)
            self.out.fillBranch("dau2_idDeepTau2017v2p1VSjet", tau2.idDeepTau2017v2p1VSjet)

            return True
        return False


def HHLepton(**kwargs):
    return lambda: HHLeptonProducer(**kwargs)