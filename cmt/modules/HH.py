import os
from array import array

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from analysis_tools.utils import import_root
from cmt.modules.baseModules import JetLepMetModule

ROOT = import_root()


class HHProducer(JetLepMetModule):
    def __init__(self, *args, **kwargs):
        super(HHProducer, self).__init__(*args, **kwargs)
        base = "{}/{}/src/HHKinFit2/HHKinFit2Scenarios".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gSystem.Load("libHHKinFit2HHKinFit2Scenarios.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/HHKinFitMasterHeavyHiggs.h".format(base))
        
        base = "{}/{}/src/HHKinFit2/HHKinFit2Core".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
        ROOT.gSystem.Load("libHHKinFit2HHKinFit2Core.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/exceptions/HHInvMConstraintException.h".format(base))
        ROOT.gROOT.ProcessLine(".L {}/interface/exceptions/HHEnergyRangeException.h".format(base))
        ROOT.gROOT.ProcessLine(".L {}/interface/exceptions/HHEnergyConstraintException.h".format(base))

        pass
    
    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        self.out.branch("Hbb_pt%s" % self.systs, "F")
        self.out.branch("Hbb_eta%s" % self.systs, "F")
        self.out.branch("Hbb_phi%s" % self.systs, "F")
        self.out.branch("Hbb_mass%s" % self.systs, "F")

        self.out.branch("Htt_pt%s" % self.systs, "F")
        self.out.branch("Htt_eta%s" % self.systs, "F")
        self.out.branch("Htt_phi%s" % self.systs, "F")
        self.out.branch("Htt_mass%s" % self.systs, "F")

        self.out.branch("HH_pt%s" % self.systs, "F")
        self.out.branch("HH_eta%s" % self.systs, "F")
        self.out.branch("HH_phi%s" % self.systs, "F")
        self.out.branch("HH_mass%s" % self.systs, "F")

        self.out.branch("HH_svfit_pt%s" % self.systs, "F")
        self.out.branch("HH_svfit_eta%s" % self.systs, "F")
        self.out.branch("HH_svfit_phi%s" % self.systs, "F")
        self.out.branch("HH_svfit_mass%s" % self.systs, "F")

        self.out.branch("HHKinFit_mass%s" % self.systs, "F")
        self.out.branch("HHKinFit_chi2%s" % self.systs, "F")
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        htt_svfit = Object(event, "Htt_svfit")
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")
        jets = Collection(event, "Jet")

        dau1, dau2, dau1_tlv, dau2_tlv = self.get_daus(event, muons, electrons, taus) #FIXME FROM HERE
        bjet1, bjet2, bjet1_tlv, bjet2_tlv = self.get_bjets(event, jets)
        met, met_tlv = self.get_met(event)

        met_tv = ROOT.TVector2(met_tlv.Px(), met_tlv.Py())

        cov = ROOT.TMatrixD(2, 2)
        cov[0][0] = met.covXX
        cov[0][1] = met.covXY
        cov[1][0] = met.covXY
        cov[1][1] = met.covYY

        # HH TLorentzVector
        htt_tlv = dau1_tlv + dau2_tlv
        hbb_tlv = bjet1_tlv + bjet2_tlv
        hh_tlv = htt_tlv + hbb_tlv

        # HH (with htt_svfit) TLorentzVector
        htt_svfit_tlv = ROOT.TLorentzVector()
        htt_svfit_tlv.SetPtEtaPhiM(
            eval("htt_svfit.pt%s" % self.systs), 
            eval("htt_svfit.eta%s" % self.systs), 
            eval("htt_svfit.phi%s" % self.systs), 
            eval("htt_svfit.mass%s" % self.systs),
        )
        hh_svfit_tlv = hbb_tlv + htt_svfit_tlv

        # HH Kin. Fit
        # constructor https://github.com/bvormwald/HHKinFit2/blob/CMSSWversion/HHKinFit2Scenarios/src/HHKinFitMasterHeavyHiggs.cpp#L27
        kinFit = ROOT.HHKinFit2.HHKinFitMasterHeavyHiggs(bjet1_tlv, bjet2_tlv, dau1_tlv, dau2_tlv, met_tv, cov)
        kinFit.addHypo(125, 125);
        try:
            kinFit.fit()
            HHKinFit_mass = kinFit.getMH()
            HHKinFit_chi2 = kinFit.getChi2()
        except (ROOT.HHKinFit2.HHInvMConstraintException()
                or ROOT.HHKinFit2.HHEnergyRangeException()
                or ROOT.HHKinFit2.HHEnergyConstraintException()):
            HHKinMass = -999.
            HHKinChi2 = -999.
        
        self.out.fillBranch("Hbb_pt%s" % self.systs, hbb_tlv.Pt())
        self.out.fillBranch("Hbb_eta%s" % self.systs, hbb_tlv.Eta())
        self.out.fillBranch("Hbb_phi%s" % self.systs, hbb_tlv.Phi())
        self.out.fillBranch("Hbb_mass%s" % self.systs, hbb_tlv.M())

        self.out.fillBranch("Htt_pt%s" % self.systs, htt_tlv.Pt())
        self.out.fillBranch("Htt_eta%s" % self.systs, htt_tlv.Eta())
        self.out.fillBranch("Htt_phi%s" % self.systs, htt_tlv.Phi())
        self.out.fillBranch("Htt_mass%s" % self.systs, htt_tlv.M())

        self.out.fillBranch("HH_pt%s" % self.systs, hh_tlv.Pt())
        self.out.fillBranch("HH_eta%s" % self.systs, hh_tlv.Eta())
        self.out.fillBranch("HH_phi%s" % self.systs, hh_tlv.Phi())
        self.out.fillBranch("HH_mass%s" % self.systs, hh_tlv.M())

        self.out.fillBranch("HH_svfit_pt%s" % self.systs, hh_svfit_tlv.Pt())
        self.out.fillBranch("HH_svfit_eta%s" % self.systs, hh_svfit_tlv.Eta())
        self.out.fillBranch("HH_svfit_phi%s" % self.systs, hh_svfit_tlv.Phi())
        self.out.fillBranch("HH_svfit_mass%s" % self.systs, hh_svfit_tlv.M())

        self.out.fillBranch("HHKinFit_mass%s" % self.systs, HHKinFit_mass)
        self.out.fillBranch("HHKinFit_chi2%s" % self.systs, HHKinFit_chi2)
        return True


def HH(**kwargs):
    return lambda: HHProducer(**kwargs)
