import os
from array import array
import math

from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection, Object
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root


ROOT = import_root()

class SVFitProducer(Module):
    def __init__(self, *args, **kwargs):
        # ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/external/eigen/d812f411c3f9-bcolbf/include/eigen3")
        # ROOT.gROOT.ProcessLine(".include /cvmfs/cms.cern.ch/slc7_amd64_gcc820/external/tensorflow/2.1.0-bcolbf/include")
        
        # base_hhbtag = "{}/{}/src/HHTools/HHbtag".format(
            # os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))
        base = "{}/{}/src/Tools/Tools".format(
            os.getenv("CMT_CMSSW_BASE"), os.getenv("CMT_CMSSW_VERSION"))

        ROOT.gSystem.Load("libToolsTools.so")
        ROOT.gROOT.ProcessLine(".L {}/interface/SVfitinterface.h".format(base))
        # self.year = kwargs["year"]
        # models = [base_hhbtag + "/models/HHbtag_v1_par_%i" % i for i in range(2)]

        # self.HHbtagger = ROOT.HHbtagInterface(models[0], models[1], self.year)

        pass
    
    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree

        self.out.branch('Htt_svfit_pt', 'F')
        self.out.branch('Htt_svfit_eta', 'F')
        self.out.branch('Htt_svfit_phi', 'F')
        self.out.branch('Htt_svfit_mass', 'F')
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        met = Object(event, "MET")
        met_bis = Object(event, "MET_T1Smear")
        
        muons = Collection(event, "Muon")
        electrons = Collection(event, "Electron")
        taus = Collection(event, "Tau")
        
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
            
        decayMode1 = (dau1.decayMode if dau1 in taus else -1)
        decayMode2 = (dau2.decayMode if dau2 in taus else -1)

        svfit = ROOT.SVfitinterface(
            0, event.pairType, decayMode1, decayMode2,
            dau1.pt, dau1.eta, dau1.phi, dau1.mass, dau2.pt, dau2.eta, dau2.phi, dau2.mass,
            met_bis.pt, met_bis.phi, met.covXX, met.covXY, met.covYY
        )

        result = svfit.FitAndGetResult()

        self.out.fillBranch("Htt_svfit_pt", result[0])
        self.out.fillBranch("Htt_svfit_eta", result[1])
        self.out.fillBranch("Htt_svfit_phi", result[2])
        self.out.fillBranch("Htt_svfit_mass", result[3])
        return True


def SVFit(**kwargs):
    return lambda: SVFitProducer(**kwargs)