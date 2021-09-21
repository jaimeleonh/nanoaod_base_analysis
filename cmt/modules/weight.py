from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.modules.common.puWeightProducer import (
    puWeight_2016, puWeight_2017, puWeight_2018
)
from PhysicsTools.NanoAODTools.postprocessing.modules.common.PrefireCorr import PrefCorr
from PhysicsTools.NanoAODTools.postprocessing.modules.common.tauCorrProducer import (
    TauCorrectionsProducer
)


class PrescaleWeight(Module):
    def __init__(self, isMC, year, *args, **kwargs):
        super(PrescaleWeight, self).__init__(*args, **kwargs)
        self.isMC = isMC
        self.year = year

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch("prefiringWeight", "F")

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        if not self.isMC:
            self.out.fillBranch("prefiringWeight", 1.)
        else:
            if (self.year == 2017
                    and event.HLT_VBF_DoubleLooseChargedIsoPFTau20_Trk1_eta2p1_Reg == 1):
                self.out.fillBranch("prefiringWeight", 0.65308574)
            elif (self.year == 2018
                    and event.HLT_VBF_DoubleLooseChargedIsoPFTauHPS20_Trk1_eta2p1 == 1):
                self.out.fillBranch("prefiringWeight", 0.990342)
            else:
                self.out.fillBranch("prefiringWeight", 1.)
        return True

def prescaleWeight(**kwargs):
    return lambda: PrescaleWeight(**kwargs)


def puWeight(**kwargs):
    isMC = kwargs.pop("isMC")
    year = int(kwargs.pop("year"))

    print isMC, year
    
    if not isMC:
        return Module
    else:
        if year == 2016:
            return puWeight_2016
        elif year == 2017:
            return puWeight_2017
        elif year == 2018:
            return puWeight_2018


def prefiringWeight(**kwargs):
    return lambda: PrefCorr(**kwargs)


def tauCorrections(**kwargs):
    year = int(kwargs.pop("year"))
    if year == 2016:
        return lambda: TauCorrectionsProducer('2016Legacy', **kwargs)
    elif year == 2017:
        return lambda: TauCorrectionsProducer('2017ReReco', **kwargs)
    elif year == 2018:
        return lambda: TauCorrectionsProducer('2018ReReco', **kwargs)
    