import os
from array import array
import math

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from analysis_tools.utils import import_root
from cmt.modules.baseModules import JetLepMetModule


ROOT = import_root()

class SVFitProducer(JetLepMetModule):
    def __init__(self, *args, **kwargs):
        super(SVFitProducer, self).__init__(*args, **kwargs)
        if "/libToolsTools.so" not in ROOT.gSystem.GetLibraries():
            ROOT.gSystem.Load("libToolsTools.so")

        base = "{}/{}/src/Tools/Tools".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
        ROOT.gROOT.ProcessLine(".L {}/interface/SVfitinterface.h".format(base))
    
    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        self.out.branch('Htt_svfit_pt%s' % self.systs, 'F')
        self.out.branch('Htt_svfit_eta%s' % self.systs, 'F')
        self.out.branch('Htt_svfit_phi%s' % self.systs, 'F')
        self.out.branch('Htt_svfit_mass%s' % self.systs, 'F')
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""

        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")

        dau1, dau2, dau1_tlv, dau2_tlv = self.get_daus(event, muons, electrons, taus)
        met, met_tlv = self.get_met(event)

        decayMode1 = (dau1.decayMode if dau1 in taus else -1)
        decayMode2 = (dau2.decayMode if dau2 in taus else -1)

        svfit = ROOT.SVfitinterface(
            0, event.pairType, decayMode1, decayMode2,
            dau1_tlv.Pt(), dau1_tlv.Eta(), dau1_tlv.Phi(), dau1_tlv.M(),
            dau2_tlv.Pt(), dau2_tlv.Eta(), dau2_tlv.Phi(), dau2_tlv.M(),
            met_tlv.Pt(), met_tlv.Phi(), met.covXX, met.covXY, met.covYY
        )

        result = svfit.FitAndGetResult()

        self.out.fillBranch("Htt_svfit_pt%s" % self.systs, result[0])
        self.out.fillBranch("Htt_svfit_eta%s" % self.systs, result[1])
        self.out.fillBranch("Htt_svfit_phi%s" % self.systs, result[2])
        self.out.fillBranch("Htt_svfit_mass%s" % self.systs, result[3])
        return True


def SVFit(**kwargs):
    return lambda: SVFitProducer(**kwargs)