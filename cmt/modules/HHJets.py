import os
from array import array
import math

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root
from cmt.modules.jet_utils import deltaRSmearedJet, JetPair


ROOT = import_root()

class HHJetsProducer(Module):
    def __init__(self, *args, **kwargs):
        ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/external/eigen/d812f411c3f9-bcolbf/include/eigen3")
        ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/external/tensorflow/2.1.0-bcolbf/include")
        
        base_hhbtag = "{}/{}/src/HHTools/HHbtag".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
        base = "{}/{}/src/Tools/Tools".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gSystem.Load("libToolsTools.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/HHbtagInterface.h".format(base))
        self.year = kwargs["year"]
        models = [base_hhbtag + "/models/HHbtag_v1_par_%i" % i for i in range(2)]

        self.HHbtagger = ROOT.HHbtagInterface(models[0], models[1], self.year)

        pass
    
    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        
        self.out.branch('bjet1_JetIdx', 'I')
        self.out.branch('bjet2_JetIdx', 'I')
        self.out.branch('VBFjet1_JetIdx', 'I')
        self.out.branch('VBFjet2_JetIdx', 'I')
        
        self.out.branch('Jet_HHbtag', "F", lenVar='nJet')
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        jets = Collection(event, "Jet")
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")
        met = Object(event, "MET_T1Smear")
        
        if event.pairType == 0:
            dau1 = muons[event.dau1_index]
            dau2 = taus[event.dau2_index]
        elif event.pairType == 1:
            dau1 = electrons[event.dau1_index]
            dau2 = taus[event.dau2_index]
        elif event.pairType == 2:
            dau1 = taus[event.dau1_index]
            dau2 = taus[event.dau2_index]
        else:
            raise ValueError("pairType %s is not implemented" % event.pairType)

        bjets = []
        for ijet, jet in enumerate(jets):
            if jet.pt_nom < 20 or abs(jet.eta) > 2.4:
                continue
            if (deltaRSmearedJet(jet, dau1) < 0.5 or deltaRSmearedJet(jet, dau2) < 0.5):
                continue
            bjets.append((ijet, jet))

        if len(bjets) < 2:
            return False

        bjets.sort(key = lambda x: x[1].btagDeepFlavB, reverse=True)

        tauH_p4 = dau1.p4() + dau2.p4()

        HHbtag_jet_pt_ = ROOT.vector(float)()
        HHbtag_jet_eta_ = ROOT.vector(float)()
        HHbtag_rel_jet_M_pt_ = ROOT.vector(float)()
        HHbtag_rel_jet_E_pt_ = ROOT.vector(float)()
        HHbtag_jet_htt_deta_ = ROOT.vector(float)()
        HHbtag_jet_htt_dphi_ = ROOT.vector(float)()
        HHbtag_jet_deepFlavour_ = ROOT.vector(float)()

        for jet in bjets:
            HHbtag_jet_pt_.push_back(jet[1].pt)
            HHbtag_jet_eta_.push_back(jet[1].eta)
            HHbtag_rel_jet_M_pt_.push_back(jet[1].mass / jet[1].pt)
            HHbtag_rel_jet_E_pt_.push_back(jet[1].mass / jet[1].p4().E())
            HHbtag_jet_htt_deta_.push_back(tauH_p4.Eta() - jet[1].eta)
            HHbtag_jet_htt_dphi_.push_back(ROOT.Math.VectorUtil.DeltaPhi(tauH_p4, jet[1].p4()))
            HHbtag_jet_deepFlavour_.push_back(jet[1].btagDeepFlavB)

        met_p4 = ROOT.TLorentzVector()
        met_p4.SetPxPyPzE(met.pt * math.cos(met.phi), met.pt * math.sin(met.phi), 0, met.pt)
        HHbtag_htt_met_dphi_ = ROOT.Math.VectorUtil.DeltaPhi(tauH_p4, met_p4)
        HHbtag_htt_scalar_pt_ = dau1.pt + dau2.pt
        HHbtag_rel_met_pt_htt_pt_ = met_p4.Pt() / HHbtag_htt_scalar_pt_
        HHbtag_tauH_pt_ = tauH_p4.Pt()
        HHbtag_tauH_eta_ = tauH_p4.Eta()

        HHbtag_evt_ = event.event
        HHbtag_year_ = self.year
        if event.pairType == 0:
            HHbtag_channel_ = 1
        elif event.pairType == 1:
            HHbtag_channel_ = 0
        elif event.pairType == 2:
            HHbtag_channel_ = 1
        else:
            raise ValueError("Pairtype {} is not supported for HHbtag computation".format(
                event.pairType))
        
        HHbtag_scores = self.HHbtagger.GetScore(HHbtag_jet_pt_, HHbtag_jet_eta_,
            HHbtag_rel_jet_M_pt_, HHbtag_rel_jet_E_pt_, HHbtag_jet_htt_deta_,
            HHbtag_jet_deepFlavour_, HHbtag_jet_htt_dphi_, HHbtag_year_, HHbtag_channel_,
            HHbtag_tauH_pt_, HHbtag_tauH_eta_, HHbtag_htt_met_dphi_,
            HHbtag_rel_met_pt_htt_pt_, HHbtag_htt_scalar_pt_, HHbtag_evt_)

        HHbtag_scores = zip([bjet[0] for bjet in bjets], HHbtag_scores)
        HHbtag_scores.sort(key=lambda x:x[1])  # sort by the obtained HHbtag score

        # 2 "bjets" with the higher HHbtag score are the selected H(bb) candidates
        bjet1 = HHbtag_scores[0]
        bjet2 = HHbtag_scores[1]

        bjets = dict(bjets)
        vbf_jet_pairs = []
        if len(HHbtag_scores) >= 4:
            for i in range(2, len(HHbtag_scores) - 1):
                for j in range(i + 1, len(HHbtag_scores)):
                    jet1_idx = HHbtag_scores[i][0]
                    jet2_idx = HHbtag_scores[j][0]
                    if (bjets[jet1_idx].pt < 30 or bjets[jet2_idx].pt < 30
                            or abs(bjets[jet1_idx].eta) > 4.7 or abs(bjets[jet2_idx].eta) > 4.7):
                        vbf_jet_pairs.append(JetPair(bjets[jet1_idx], bjets[jet2_idx],
                            index1=jet1_idx, index2=jet2_idx))
            if vbf_jet_pairs:
                vbf_pair = max(vbf_jet_pairs)

        self.out.fillBranch("bjet1_JetIdx", bjet1[0])
        self.out.fillBranch("bjet2_JetIdx", bjet2[0])

        if vbf_jet_pairs:
            self.out.fillBranch("VBFjet1_JetIdx", vbf_pair.obj1_index)
            self.out.fillBranch("VBFjet2_JetIdx", vbf_pair.obj2_index)
        else:
            self.out.fillBranch("VBFjet1_JetIdx", -1)
            self.out.fillBranch("VBFjet2_JetIdx", -1)

        Jet_HHbtag = []
        HHbtag_scores = dict(HHbtag_scores)  # so it's easier to check the index from the good jets
        for i in range(event.nJet):
            Jet_HHbtag.append(HHbtag_scores[i] if i in HHbtag_scores.keys() else -999.)

        self.out.fillBranch("Jet_HHbtag", Jet_HHbtag)
        return True


def HHJets(**kwargs):
    return lambda: HHJetsProducer(**kwargs)