from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from analysis_tools.utils import import_root
from cmt.modules.tau_utils import LeptonTauPair, TriggerChecker, lepton_veto
from cmt.modules.baseModules import JetLepMetModule

ROOT = import_root()

class HHLeptonProducer(JetLepMetModule):
    def __init__(self, isMC, year, runPeriod, *args, **kwargs):
        super(HHLeptonProducer, self).__init__(*args, **kwargs)
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
        self.out.branch('dau1_index', 'I')
        self.out.branch('dau2_index', 'I')
        self.out.branch('isVBFtrigger', 'I')

        self.out.branch('dau1_eta', 'F')
        self.out.branch('dau1_phi', 'F')
        self.out.branch('dau1_dxy', 'F')
        self.out.branch('dau1_dz', 'F')
        self.out.branch('dau1_q', 'I')
        self.out.branch('dau1_iso', 'F')
        self.out.branch('dau1_decayMode', 'I')
        self.out.branch('dau1_idDecayModeNewDMs', 'b')
        self.out.branch('dau1_idDeepTau2017v2p1VSe', 'I')
        self.out.branch('dau1_idDeepTau2017v2p1VSmu', 'I')
        self.out.branch('dau1_idDeepTau2017v2p1VSjet', 'I')

        self.out.branch('dau2_eta', 'F')
        self.out.branch('dau2_phi', 'F')
        self.out.branch('dau2_dxy', 'F')
        self.out.branch('dau2_dz', 'F')
        self.out.branch('dau2_q', 'I')
        self.out.branch('dau2_iso', 'F')
        self.out.branch('dau2_decayMode', 'I')
        self.out.branch('dau2_idDecayModeNewDMs', 'b')
        self.out.branch('dau2_idDeepTau2017v2p1VSe', 'I')
        self.out.branch('dau2_idDeepTau2017v2p1VSmu', 'I')
        self.out.branch('dau2_idDeepTau2017v2p1VSjet', 'I')
        
        self.histo = ROOT.TH1D("InsideHHLepton", "", 21, -1, 20)

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        prevdir = ROOT.gDirectory
        outputFile.cd()
        if "histos" not in [key.GetName() for key in outputFile.GetListOfKeys()]:
            outputFile.mkdir("histos")
        outputFile.cd("histos")
        self.histo.Write()
        prevdir.cd()
    
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        electrons = Collection(event, "Electron")
        muons = Collection(event, "Muon")
        taus = Collection(event, "Tau")
        self.histo.Fill(-1)
        # muon-tau channels
        goodmuons = []
        for imuon, muon in enumerate(muons):
            if (abs(muon.eta) > 2.1 or muon.jetRelIso > 0.15 or abs(muon.dxy) > 0.045
                    or abs(muon.dz) > 0.2 or not muon.tightId):
                continue
            goodmuons.append((imuon, muon))
        if goodmuons:
            self.histo.Fill(0)
            goodtaus = []
            for itau, tau in enumerate(taus):
                if tau.idDeepTau2017v2p1VSmu < 15 or tau.idDeepTau2017v2p1VSe < 7:
                    continue
                if tau.dz > 0.2:
                    continue
                if tau.decayMode not in [0, 1, 10, 11]:
                    continue
                goodtaus.append((itau, tau))
            
            muontaupairs = []
            for (imuon, muon) in goodmuons:
                for (itau, tau) in goodtaus:
                    self.histo.Fill(1)
                    if tau.DeltaR(muon) < 0.5: continue
                    self.histo.Fill(2)
                    if not self.trigger_checker.check_mutau(event,
                            eval("muon.pt%s" % self.muon_syst), muon.eta,
                            eval("tau.pt%s" % self.tau_syst), tau.eta, th1=1, th2=5):
                        continue
                    self.histo.Fill(3)
                    muontaupair = LeptonTauPair(
                        muon, eval("muon.pt%s" % self.muon_syst), muon.pfRelIso04_all,
                        tau, eval("tau.pt%s" % self.tau_syst), tau.rawDeepTau2017v2p1VSjet)
                    if muontaupair.check_charge():
                        muontaupairs.append((imuon, itau, muontaupair))

            if len(muontaupairs) != 0:
                muontaupairs.sort(key=lambda x: x[2], reverse=True)
                muon, tau = muontaupairs[0][2].pair

                fail_lepton_veto, _ = lepton_veto(electrons, muons, taus, muon)
                if fail_lepton_veto:
                    return False
                self.histo.Fill(4)

                self.out.fillBranch("pairType", 0)
                self.out.fillBranch("isVBFtrigger", 0)

                self.out.fillBranch("dau1_index", muontaupairs[0][0])
                self.out.fillBranch("dau1_eta", muon.eta)
                self.out.fillBranch("dau1_phi", muon.phi)
                self.out.fillBranch("dau1_dxy", muon.dxy)
                self.out.fillBranch("dau1_dz", muon.dz)
                self.out.fillBranch("dau1_q", muon.charge)
                self.out.fillBranch("dau1_iso", muon.pfRelIso04_all)
                self.out.fillBranch("dau1_decayMode", -1)
                self.out.fillBranch("dau1_idDecayModeNewDMs", 0)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSe", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSmu", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSjet", -1)

                self.out.fillBranch("dau2_index", muontaupairs[0][1])
                self.out.fillBranch("dau2_eta", tau.eta)
                self.out.fillBranch("dau2_phi", tau.phi)
                self.out.fillBranch("dau2_dxy", tau.dxy)
                self.out.fillBranch("dau2_dz", tau.dz)
                self.out.fillBranch("dau2_q", tau.charge)
                self.out.fillBranch("dau2_iso", tau.rawIso)
                self.out.fillBranch("dau2_decayMode", tau.decayMode)
                self.out.fillBranch("dau2_idDecayModeNewDMs", tau.idDecayModeNewDMs)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSe", tau.idDeepTau2017v2p1VSe)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSmu", tau.idDeepTau2017v2p1VSmu)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSjet", tau.idDeepTau2017v2p1VSjet)
                return True

        # electron-tau channels
        goodelectrons = []
        for ielectron, electron in enumerate(electrons):
            if (not (electron.mvaFall17V2Iso_WP80 or electron.mvaFall17V2noIso_WP80)
                    or abs(electron.dxy) > 0.045 or abs(electron.dz) > 0.2):
                continue
            goodelectrons.append((ielectron, electron))
        if goodelectrons:
            goodtaus = []
            for itau, tau in enumerate(taus):
                if tau.idDeepTau2017v2p1VSmu < 15 or tau.idDeepTau2017v2p1VSe < 7:
                    continue
                if tau.dz > 0.2:
                    continue
                if tau.decayMode not in [0, 1, 10, 11]:
                    continue
                goodtaus.append((itau, tau))

            electrontaupairs = []
            for (ielectron, electron) in goodelectrons:
                for (itau, tau) in goodtaus:
                    if tau.DeltaR(electron) < 0.5: continue
                    if not self.trigger_checker.check_etau(event,
                            eval("electron.pt%s" % self.electron_syst), electron.eta,
                            eval("tau.pt%s" % self.tau_syst), tau.eta, th1=1, th2=5):
                        continue
                    electrontaupair = LeptonTauPair(
                        electron, eval("electron.pt%s" % self.electron_syst), electron.pfRelIso03_all,
                        tau, eval("tau.pt%s" % self.tau_syst), tau.rawDeepTau2017v2p1VSjet)
                    if electrontaupair.check_charge():
                        electrontaupairs.append((ielectron, itau, electrontaupair))

            if len(electrontaupairs) != 0:
                electrontaupairs.sort(key=lambda x: x[2], reverse=True)
                electron, tau = electrontaupairs[0][2].pair

                fail_lepton_veto, _ = lepton_veto(electrons, muons, taus, electron)
                if fail_lepton_veto:
                    return False

                self.out.fillBranch("pairType", 1)
                self.out.fillBranch("isVBFtrigger", 0)

                self.out.fillBranch("dau1_index", electrontaupairs[0][0])
                self.out.fillBranch("dau1_eta", electron.eta)
                self.out.fillBranch("dau1_phi", electron.phi)
                self.out.fillBranch("dau1_dxy", electron.dxy)
                self.out.fillBranch("dau1_dz", electron.dz)
                self.out.fillBranch("dau1_q", electron.charge)
                self.out.fillBranch("dau1_iso", electron.pfRelIso03_all)
                self.out.fillBranch("dau1_decayMode", -1)
                self.out.fillBranch("dau1_idDecayModeNewDMs", 0)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSe", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSmu", -1)
                self.out.fillBranch("dau1_idDeepTau2017v2p1VSjet", -1)

                self.out.fillBranch("dau2_index", electrontaupairs[0][1])
                self.out.fillBranch("dau2_eta", tau.eta)
                self.out.fillBranch("dau2_phi", tau.phi)
                self.out.fillBranch("dau2_dxy", tau.dxy)
                self.out.fillBranch("dau2_dz", tau.dz)
                self.out.fillBranch("dau2_q", tau.charge)
                self.out.fillBranch("dau2_iso", tau.rawIso)
                self.out.fillBranch("dau2_decayMode", tau.decayMode)
                self.out.fillBranch("dau2_idDecayModeNewDMs", tau.idDecayModeNewDMs)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSe", tau.idDeepTau2017v2p1VSe)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSmu", tau.idDeepTau2017v2p1VSmu)
                self.out.fillBranch("dau2_idDeepTau2017v2p1VSjet", tau.idDeepTau2017v2p1VSjet)

                return True

        goodtaus = []
        for itau, tau in enumerate(taus):
            if tau.idDeepTau2017v2p1VSmu < 1 or tau.idDeepTau2017v2p1VSe < 3:
                continue
            if tau.dz > 0.2:
                continue
            if tau.decayMode not in [0, 1, 10, 11]:
                continue
            goodtaus.append((itau, tau))

        tautaupairs = []
        for i in range(len(goodtaus) - 1):
            for j in range(i + 1, len(goodtaus)):
                tau1_index = goodtaus[i][0]
                tau1 = goodtaus[i][1]
                tau2_index = goodtaus[j][0]
                tau2 = goodtaus[j][1]
            
            pass_ditau = self.trigger_checker.check_tautau(event,
                eval("tau1.pt%s" % self.tau_syst), tau1.eta,
                eval("tau2.pt%s" % self.tau_syst), tau2.eta, abs_th1=40, abs_th2=40)
            # passing vbf trigger ONLY
            pass_vbf = not pass_ditau and self.trigger_checker.check_vbftautau(event,
                eval("tau1.pt%s" % self.tau_syst), tau1.eta,
                eval("tau2.pt%s" % self.tau_syst), tau2.eta, abs_th1=25, abs_th2=25)
            
            if not (pass_ditau or pass_vbf):
                continue

            pass_vbf = int(pass_vbf)

            tautaupair = LeptonTauPair(
                tau1, eval("tau1.pt%s" % self.tau_syst), tau1.rawDeepTau2017v2p1VSjet,
                tau2, eval("tau2.pt%s" % self.tau_syst), tau2.rawDeepTau2017v2p1VSjet)
            if tautaupair.check_charge():
                tautaupairs.append((tau1_index, tau2_index, tautaupair))

        if len(tautaupairs) != 0:
            tautaupairs.sort(key=lambda x: x[2], reverse=True)
            tau1, tau2 = tautaupairs[0][2].pair

            fail_lepton_veto, _ = lepton_veto(electrons, muons, taus)
            if fail_lepton_veto:
                return False

            self.out.fillBranch("pairType", 2)
            self.out.fillBranch("isVBFtrigger", pass_vbf)

            self.out.fillBranch("dau1_index", tautaupairs[0][0])
            self.out.fillBranch("dau1_eta", tau1.eta)
            self.out.fillBranch("dau1_phi", tau1.phi)
            self.out.fillBranch("dau1_dxy", tau1.dxy)
            self.out.fillBranch("dau1_dz", tau1.dz)
            self.out.fillBranch("dau1_q", tau1.charge)
            self.out.fillBranch("dau1_iso", tau1.rawIso)
            self.out.fillBranch("dau1_decayMode", tau1.decayMode)
            self.out.fillBranch("dau1_idDecayModeNewDMs", tau1.idDecayModeNewDMs)
            self.out.fillBranch("dau1_idDeepTau2017v2p1VSe", tau1.idDeepTau2017v2p1VSe)
            self.out.fillBranch("dau1_idDeepTau2017v2p1VSmu", tau1.idDeepTau2017v2p1VSmu)
            self.out.fillBranch("dau1_idDeepTau2017v2p1VSjet", tau1.idDeepTau2017v2p1VSjet)

            self.out.fillBranch("dau2_index", tautaupairs[0][1])
            self.out.fillBranch("dau2_eta", tau2.eta)
            self.out.fillBranch("dau2_phi", tau2.phi)
            self.out.fillBranch("dau2_dxy", tau2.dxy)
            self.out.fillBranch("dau2_dz", tau2.dz)
            self.out.fillBranch("dau2_q", tau2.charge)
            self.out.fillBranch("dau2_iso", tau2.rawIso)
            self.out.fillBranch("dau2_decayMode", tau2.decayMode)
            self.out.fillBranch("dau2_idDecayModeNewDMs", tau2.idDecayModeNewDMs)
            self.out.fillBranch("dau2_idDeepTau2017v2p1VSe", tau2.idDeepTau2017v2p1VSe)
            self.out.fillBranch("dau2_idDeepTau2017v2p1VSmu", tau2.idDeepTau2017v2p1VSmu)
            self.out.fillBranch("dau2_idDeepTau2017v2p1VSjet", tau2.idDeepTau2017v2p1VSjet)

            return True
        return False


class HHLeptonVariableProducer(JetLepMetModule):
    def __init__(self, isMC, *args, **kwargs):
        super(HHLeptonVariableProducer, self).__init__(*args, **kwargs)
        self.isMC = isMC

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        assert sum([self.muon_syst != "", self.electron_syst != "", self.tau_syst != ""]) <= 1
        self.lep_syst = ""
        if self.muon_syst:
            self.lep_syst = self.muon_syst
        elif self.electron_syst:
            self.lep_syst = self.electron_syst
        elif self.tau_syst:
            self.lep_syst = self.tau_syst

        self.out.branch('dau1_pt%s' % self.lep_syst, 'F')
        self.out.branch('dau1_mass%s' % self.lep_syst, 'F')
        self.out.branch('dau2_pt%s' % self.lep_syst, 'F')
        self.out.branch('dau2_mass%s' % self.lep_syst, 'F')
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")

        _, _, dau1_tlv, dau2_tlv = self.get_daus(event, muons, electrons, taus)
        self.out.fillBranch("dau1_pt%s" % self.lep_syst, dau1_tlv.Pt())
        self.out.fillBranch("dau1_mass%s" % self.lep_syst, dau1_tlv.M())
        self.out.fillBranch("dau2_pt%s" % self.lep_syst, dau2_tlv.Pt())
        self.out.fillBranch("dau2_mass%s" % self.lep_syst, dau2_tlv.M())
        return True


def HHLepton(**kwargs):
    return lambda: HHLeptonProducer(**kwargs)


def HHLeptonVariable(**kwargs):
    return lambda: HHLeptonVariableProducer(**kwargs)
