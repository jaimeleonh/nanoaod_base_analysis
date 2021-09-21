from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from math import sin, cos

from analysis_tools.utils import import_root

ROOT = import_root()

class SystModule(Module):
    def __init__(self, isMC=True, systs=None, *args, **kwargs):
        self.isMC = isMC
        self.set_systs(systs)

    def set_systs(self, systs):
        self.systs = []
        for systname, systvalue in systs.items():
            setattr(self, systname, ("" if not self.isMC
                else ("_%s" % systvalue if systvalue != "" else "")))
            if systname == "met_smear_tag":
                continue
            self.systs.append(eval("self.%s" % systname))
        self.systs = "".join(self.systs)

class JetLepMetModule(SystModule):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("jet_syst", "nom")
        kwargs.setdefault("muon_syst", "")
        kwargs.setdefault("electron_syst", "")
        kwargs.setdefault("tau_syst", "")
        kwargs.setdefault("met_syst", "")
        kwargs.setdefault("met_smear_tag", "T1Smear")
        super(JetLepMetModule, self).__init__(kwargs.pop("isMC", True), kwargs)

    def get_daus(self, event, muons, electrons, taus):
        if event.pairType == 0:
            dau1 = muons[event.dau1_index]
            dau2 = taus[event.dau2_index]
            dau1_syst = self.muon_syst
        elif event.pairType == 1:
            dau1 = electrons[event.dau1_index]
            dau2 = taus[event.dau2_index]
            dau1_syst = self.electron_syst
        elif event.pairType == 2:
            dau1 = taus[event.dau1_index]
            dau2 = taus[event.dau2_index]
            dau1_syst = self.tau_syst
        else:
            raise ValueError("pairType %s is not implemented" % event.pairType)
        dau2_syst = self.tau_syst

        # build TLorentzVectors
        dau1_tlv = ROOT.TLorentzVector()
        dau2_tlv = ROOT.TLorentzVector()
        dau1_tlv.SetPtEtaPhiM(
            eval("dau1.pt%s" % dau1_syst), dau1.eta, dau1.phi, eval("dau1.mass%s" % dau1_syst))
        dau2_tlv.SetPtEtaPhiM(
            eval("dau2.pt%s" % dau2_syst), dau2.eta, dau2.phi, eval("dau2.mass%s" % dau2_syst))
        
        return dau1, dau2, dau1_tlv, dau2_tlv

    def get_bjets(self, event, jets):
        bjet1 = jets[event.bjet1_JetIdx]
        bjet2 = jets[event.bjet2_JetIdx]
        
        # build TLorentzVectors
        bjet1_tlv = ROOT.TLorentzVector()
        bjet2_tlv = ROOT.TLorentzVector()
        bjet1_tlv.SetPtEtaPhiM(eval("bjet1.pt%s" % self.jet_syst), bjet1.eta,
            bjet1.phi, eval("bjet1.mass%s" % self.jet_syst))
        bjet2_tlv.SetPtEtaPhiM(eval("bjet2.pt%s" % self.jet_syst), bjet2.eta,
            bjet2.phi, eval("bjet2.mass%s" % self.jet_syst))

        return bjet1, bjet2, bjet1_tlv, bjet2_tlv

    def get_vbfjets(self, event, jets):
        if event.VBFjet1_JetIdx != -1 and event.VBFjet2_JetIdx != -1:
            vbfjet1 = jets[event.VBFjet1_JetIdx]
            vbfjet2 = jets[event.VBFjet2_JetIdx]
            
            # build TLorentzVectors
            vbfjet1_tlv = ROOT.TLorentzVector()
            vbfjet2_tlv = ROOT.TLorentzVector()
            vbfjet1_tlv.SetPtEtaPhiM(eval("vbfjet1.pt%s" % self.jet_syst), vbfjet1.eta,
                vbfjet1.phi, eval("vbfjet1.mass%s" % self.jet_syst))
            vbfjet2_tlv.SetPtEtaPhiM(eval("vbfjet2.pt%s" % self.jet_syst), vbfjet2.eta,
                vbfjet2.phi, eval("vbfjet2.mass%s" % self.jet_syst))
        else:
            return None, None, None, None
        return vbfjet1, vbfjet2, vbfjet1_tlv, vbfjet2_tlv

    def get_met(self, event):
        met = Object(event, "MET")
        met_bis = Object(event, "MET%s" % self.met_smear_tag)

        met_tlv = ROOT.TLorentzVector()
        met_tlv.SetPxPyPzE(
            eval("met_bis.pt%s" % self.met_syst) * cos(eval("met_bis.phi%s" % self.met_syst)),
            eval("met_bis.pt%s" % self.met_syst) * sin(eval("met_bis.phi%s" % self.met_syst)),
            0,
            eval("met_bis.pt%s" % self.met_syst)
        )
        return met, met_tlv