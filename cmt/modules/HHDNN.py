import os
from array import array

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from analysis_tools.utils import import_root
from cmt.modules.baseModules import JetLepMetModule

ROOT = import_root()


class HHDNNProducer(JetLepMetModule):
    def __init__(self, *args, **kwargs):
        super(HHDNNProducer, self).__init__(*args, **kwargs)
        ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/"
            "external/eigen/d812f411c3f9-bcolbf/include/eigen3")
        ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/"
            "external/tensorflow/2.1.0-bcolbf/include")
        base = "{}/{}/src/Tools/Tools".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gSystem.Load("libToolsTools.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/HHDNNinterface.h".format(base))

        feature_file = kwargs.pop(
            "feature_file",
            "{}/{}/src/cms_runII_dnn_models/models/nonres_gluglu/2020-07-31-0/features.txt".format(
                os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
        )
        req_features = ROOT.vector(str)()
        with open(feature_file) as f:
            for feature in f.readlines():
                req_features.push_back(feature)       

        model_dir = kwargs.pop(
            "model_dir",
            "{}/{}/src/cms_runII_dnn_models/models/nonres_gluglu/2020-07-31-0/ensemble".format(
                os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
        )

        kls = ROOT.vector(float)()
        kls.push_back(1)  # FIXME, may not be needed

        self.hhdnn = ROOT.HHDNNinterface(model_dir, req_features, kls)
        self.hhdnn.SetGlobalInputs(kwargs.pop("year", 2018), 2)
        pass
    
    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        # self.out.branch("DNNoutSM_kl_1%s" % self.systs, "F")

        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
    
        return True
    
        """process event, return True (go to next module) or False (fail, go to next event)"""
        htt_svfit = Object(event, "Htt_svfit")
        HHKinFit = Object(event, "HHKinFit")
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")
        jets = Collection(event, "Jet")

        dau1, dau2, dau1_tlv, dau2_tlv = self.get_daus(event, muons, electrons, taus) #FIXME FROM HERE
        bjet1, bjet2, bjet1_tlv, bjet2_tlv = self.get_bjets(event, jets)
        vbfjet1, vbfjet2, vbfjet1_tlv, vbfjet2_tlv = self.get_vbfjets(event, jets)
        if not vbfjet1:  # dummy tlv
            vbfjet1_tlv.SetPtEtaPhiM(1, 1, 1, 1)
            vbfjet2_tlv.SetPtEtaPhiM(1, 1, 1, 1)
        met, met_tlv = self.get_met(event)
        htt_svfit_tlv = ROOT.TLorentzVector()
        htt_svfit_tlv.SetPtEtaPhiM(
            eval("htt_svfit.pt%s" % self.systs), 
            eval("htt_svfit.eta%s" % self.systs), 
            eval("htt_svfit.phi%s" % self.systs), 
            eval("htt_svfit.mass%s" % self.systs),
        )

        # the definition of channels is a bit different between the DNN features
        # and our definition

        # Channel | Our Def. | DNN
        # ------------------------
        # mutau   |    0     |  1
        # etau    |    1     |  2
        # tautau  |    2     |  0
        
        if event.pairType == 0:
            channel = 1
        elif event.pairType == 1:
            channel = 2
        elif event.pairType == 2:
            channel = 0
        else:
            raise ValueError("pairType %s is not implemented" % event.pairType)
        
        self.hhdnn.SetEventInputs(channel, event.isBoosted, (2 if vbfjet1 else 0), event.event,
            bjet1_tlv, bjet2_tlv, dau1_tlv, dau2_tlv, vbfjet1_tlv, vbfjet2_tlv, met_tlv,
            htt_svfit_tlv, HHKinFit.mass, HHKinFit.chi2, (HHKinFit.chi2 > 0),
            (eval("htt_svfit.mass%s" % self.systs) > 0),)

        # void SetEventInputs(Channel channel, int is_boosted, int nvbf, unsigned long long int eventn,
          # TLorentzVector b1, TLorentzVector b2, TLorentzVector l1, TLorentzVector l2, 
          # TLorentzVector vbf1, TLorentzVector vbf2, TLorentzVector met, TLorentzVector svfit, 
          # float KinFitMass, float KinFitChi2, bool KinFitConv, bool SVfitConv,
          # float HHbtag_b1, float HHbtag_b2, float HHbtag_vbf1, float HHbtag_vbf2,
          # float CvsL_b1, float CvsL_b2, float CvsL_vbf1, float CvsL_vbf2,
          # float CvsB_b1, float CvsB_b2, float CvsB_vbf1, float CvsB_vbf2,
          # float cv, float c2v, float c3, bool pass_massCut
        # );

        
        return True


def HH(**kwargs):
    return lambda: HHProducer(**kwargs)
