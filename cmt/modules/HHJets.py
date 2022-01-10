import os
from array import array

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root
from cmt.modules.jet_utils import JetPair
from cmt.modules.baseModules import JetLepMetModule

ROOT = import_root()

class HHJetsProducer(JetLepMetModule):
    def __init__(self, *args, **kwargs):
        super(HHJetsProducer, self).__init__(self, *args, **kwargs)
        
        # print ROOT.gSystem.GetLibraries()
        if "/libToolsTools.so" not in ROOT.gSystem.GetLibraries():
            ROOT.gSystem.Load("libToolsTools.so")

        ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/"
            "external/eigen/d812f411c3f9-bcolbf/include/eigen3")
        ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/"
            "external/tensorflow/2.1.0-bcolbf/include")
        base = "{}/{}/src/Tools/Tools".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gROOT.ProcessLine(".L {}/interface/HHbtagInterface.h".format(base))
        
        self.year = kwargs.pop("year")
        base_hhbtag = "{}/{}/src/HHTools/HHbtag".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
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
        
        self.out.branch('isBoosted', 'I')
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        jets = Collection(event, "Jet")
        fatjets = Collection(event, "FatJet")
        subjets = Collection(event, "SubJet")
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")

        dau1, dau2, dau1_tlv, dau2_tlv = self.get_daus(event, muons, electrons, taus)
        met, met_tlv = self.get_met(event)

        bjets = []
        all_jet_indexes = []
        #print "** JETS **"
        for ijet, jet in enumerate(jets):
            print eval("jet.pt%s" % self.jet_syst), jet.eta, jet.puId, jet.jetId
            if (jet.puId < 4 and eval("jet.pt%s" % self.jet_syst) <= 50) or jet.jetId < 2:
                # print "does not pass id"
                continue
            jet_tlv = ROOT.TLorentzVector()
            jet_tlv.SetPtEtaPhiM(
                eval("jet.pt%s" % self.jet_syst),
                jet.eta,
                jet.phi,
                eval("jet.mass%s" % self.jet_syst)
            )
            if abs(jet_tlv.DeltaR(dau1_tlv)) < 0.5 or abs(jet.DeltaR(dau2_tlv)) < 0.5:
                # print "does not pass deltaR with leptons"
                continue
            # print jet.pt, jet.eta
            # print "passes everything"
            if eval("jet.pt%s" % self.jet_syst) > 20 and abs(jet.eta) < 2.4:
                bjets.append((ijet, jet))
            # store also jets w/ eta < 4.7 for vbf analysis
            if eval("jet.pt%s" % self.jet_syst) > 20 and abs(jet.eta) < 4.7:
                all_jet_indexes.append(ijet)

        if len(bjets) < 2:
            return False

        bjets.sort(key=lambda x: x[1].btagDeepFlavB, reverse=True)
        htt_tlv = dau1_tlv + dau2_tlv

        HHbtag_jet_pt_ = ROOT.vector(float)()
        HHbtag_jet_eta_ = ROOT.vector(float)()
        HHbtag_rel_jet_M_pt_ = ROOT.vector(float)()
        HHbtag_rel_jet_E_pt_ = ROOT.vector(float)()
        HHbtag_jet_htt_deta_ = ROOT.vector(float)()
        HHbtag_jet_htt_dphi_ = ROOT.vector(float)()
        HHbtag_jet_deepFlavour_ = ROOT.vector(float)()

        for jet in bjets:
            jet_tlv = ROOT.TLorentzVector()
            jet_tlv.SetPtEtaPhiM(
                eval("jet[1].pt%s" % self.jet_syst),
                jet[1].eta,
                jet[1].phi,
                eval("jet[1].mass%s" % self.jet_syst)
            )

            HHbtag_jet_pt_.push_back(jet_tlv.Pt())
            HHbtag_jet_eta_.push_back(jet_tlv.Eta())
            HHbtag_rel_jet_M_pt_.push_back(jet_tlv.M() / jet_tlv.Pt())
            HHbtag_rel_jet_E_pt_.push_back(jet_tlv.M() / jet_tlv.E())
            HHbtag_jet_htt_deta_.push_back(htt_tlv.Eta() - jet_tlv.Eta())
            HHbtag_jet_htt_dphi_.push_back(ROOT.Math.VectorUtil.DeltaPhi(htt_tlv, jet_tlv))
            HHbtag_jet_deepFlavour_.push_back(jet[1].btagDeepFlavB)

        HHbtag_htt_met_dphi_ = ROOT.Math.VectorUtil.DeltaPhi(htt_tlv, met_tlv)
        HHbtag_htt_scalar_pt_ = dau1.pt + dau2.pt
        HHbtag_rel_met_pt_htt_pt_ = met_tlv.Pt() / HHbtag_htt_scalar_pt_
        HHbtag_htt_pt_ = htt_tlv.Pt()
        HHbtag_htt_eta_ = htt_tlv.Eta()

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
            HHbtag_htt_pt_, HHbtag_htt_eta_, HHbtag_htt_met_dphi_,
            HHbtag_rel_met_pt_htt_pt_, HHbtag_htt_scalar_pt_, HHbtag_evt_)

        HHbtag_scores = zip([bjet[0] for bjet in bjets], HHbtag_scores)
        HHbtag_scores.sort(key=lambda x:x[1], reverse=True)  # sort by the obtained HHbtag score

        # 2 "bjets" with the higher HHbtag score are the selected H(bb) candidates
        bjet1_JetIdx = HHbtag_scores[0][0]
        bjet2_JetIdx = HHbtag_scores[1][0]

        if (eval("jets[bjet1_JetIdx].pt%s" % self.jet_syst) <
                eval("jets[bjet2_JetIdx].pt%s" % self.jet_syst)):
            bjet1_JetIdx = HHbtag_scores[1][0]
            bjet2_JetIdx = HHbtag_scores[0][0]

        # let's get the H(bb) object and tlv for the boosted analysis (later)
        bjet1_obj = jets[bjet1_JetIdx]
        bjet2_obj = jets[bjet2_JetIdx]
        bjet1_tlv = ROOT.TLorentzVector()
        bjet2_tlv = ROOT.TLorentzVector()
        bjet1_tlv.SetPtEtaPhiM(eval("bjet1_obj.pt%s" % self.jet_syst), bjet1_obj.eta,
            bjet1_obj.phi, eval("bjet1_obj.mass%s" % self.jet_syst))
        bjet2_tlv.SetPtEtaPhiM(eval("bjet2_obj.pt%s" % self.jet_syst), bjet2_obj.eta,
            bjet2_obj.phi, eval("bjet2_obj.mass%s" % self.jet_syst))

        vbf_jet_pairs = []
        if len(all_jet_indexes) >= 4:
            for i in range(len(all_jet_indexes)):
                if all_jet_indexes[i] in [bjet1_JetIdx, bjet2_JetIdx]:
                    continue
                for j in range(i + 1, len(all_jet_indexes)):
                    if all_jet_indexes[j] in [bjet1_JetIdx, bjet2_JetIdx]:
                        continue

                    jet1_idx = all_jet_indexes[i]
                    jet2_idx = all_jet_indexes[j]
                    if (eval("jets[jet1_idx].pt%s" % self.jet_syst) < 30
                            or eval("jets[jet2_idx].pt%s" % self.jet_syst) < 30
                            or abs(jets[jet1_idx].eta) > 4.7 or abs(jets[jet2_idx].eta) > 4.7):
                        continue
                    vbf_jet_pairs.append(JetPair(jets[jet1_idx], jets[jet2_idx], self.jet_syst,
                        index1=jet1_idx, index2=jet2_idx))
                    print eval("vbf_jet_pairs[-1].obj1.pt%s" % self.jet_syst), eval("vbf_jet_pairs[-1].obj2.pt%s" % self.jet_syst), vbf_jet_pairs[-1].inv_mass

            if vbf_jet_pairs:
                vbf_pair = max(vbf_jet_pairs)

        self.out.fillBranch("bjet1_JetIdx", bjet1_JetIdx)
        self.out.fillBranch("bjet2_JetIdx", bjet2_JetIdx)

        if vbf_jet_pairs:
            if vbf_pair.obj1.pt > vbf_pair.obj2.pt:
                self.out.fillBranch("VBFjet1_JetIdx", vbf_pair.obj1_index)
                self.out.fillBranch("VBFjet2_JetIdx", vbf_pair.obj2_index)
            else:
                self.out.fillBranch("VBFjet1_JetIdx", vbf_pair.obj2_index)
                self.out.fillBranch("VBFjet2_JetIdx", vbf_pair.obj1_index)
        else:
            self.out.fillBranch("VBFjet1_JetIdx", -1)
            self.out.fillBranch("VBFjet2_JetIdx", -1)

        Jet_HHbtag = []
        HHbtag_scores = dict(HHbtag_scores)  # so it's easier to check the index from the good jets
        for i in range(event.nJet):
            Jet_HHbtag.append(HHbtag_scores[i] if i in HHbtag_scores.keys() else -999.)

        self.out.fillBranch("Jet_HHbtag", Jet_HHbtag)

        # is the event boosted?
        # we loop over the fat AK8 jets, apply a mass cut and verify that its subjets match
        # the jets we selected before.
        is_boosted = 0
        for ifatjet, fatjet in enumerate(fatjets):
            if fatjet.msoftdrop < 30:
                continue
            if fatjet.subJetIdx1 == -1 or fatjet.subJetIdx2 == -1:
                continue
            subj1 = subjets[fatjet.subJetIdx1]
            subj2 = subjets[fatjet.subJetIdx2]
            subj1_tlv = ROOT.TLorentzVector()
            subj2_tlv = ROOT.TLorentzVector()
            # subj1_tlv.SetPtEtaPhiM(eval("subj1.pt%s" % self.jet_syst), subj1.eta,
                # subj1.phi, eval("subj1.mass%s" % self.jet_syst))
            # subj2_tlv.SetPtEtaPhiM(eval("subj2.pt%s" % self.jet_syst), subj2.eta,
                # subj2.phi, eval("subj2.mass%s" % self.jet_syst))
            subj1_tlv.SetPtEtaPhiM(subj1.pt, subj1.eta, subj1.phi, subj1.mass)
            subj2_tlv.SetPtEtaPhiM(subj2.pt, subj2.eta, subj2.phi, subj2.mass)
            if ((abs(bjet1_tlv.DeltaR(subj1_tlv)) > 0.4
                    or abs(bjet2_tlv.DeltaR(subj2_tlv)) > 0.4)
                and
                (abs(bjet1_tlv.DeltaR(subj2_tlv)) > 0.4
                    or abs(bjet2_tlv.DeltaR(subj1_tlv)) > 0.4)):
                continue
            is_boosted = 1
        self.out.fillBranch("isBoosted", is_boosted)
        return True


def HHJets(**kwargs):
    return lambda: HHJetsProducer(**kwargs)
