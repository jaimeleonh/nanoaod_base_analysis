from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from PhysicsTools.NanoAODTools.postprocessing.modules.common.puWeightProducer import (
    puWeight_2016, puWeight_2017, puWeight_2018
)
from PhysicsTools.NanoAODTools.postprocessing.modules.common.PrefireCorr import PrefCorr
from PhysicsTools.NanoAODTools.postprocessing.modules.common.tauCorrProducer import (
    TauCorrectionsProducer
)
from cmt.modules.baseModules import DummyModule


class PrescaleWeight(Module):
    def __init__(self, isMC, year, *args, **kwargs):
        super(PrescaleWeight, self).__init__(*args, **kwargs)
        self.year = year

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch("prefiringWeight", "F")

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""

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
    isMC = kwargs.pop("isMC")
    if isMC:
        return lambda: PrescaleWeight(**kwargs)
    else:
        return lambda: DummyModule(**kwargs)

def puWeight(**kwargs):
    isMC = kwargs.pop("isMC")
    year = int(kwargs.pop("year"))

    if not isMC:
        return lambda: DummyModule(**kwargs)
    else:
        if year == 2016:
            return puWeight_2016
        elif year == 2017:
            return puWeight_2017
        elif year == 2018:
            return puWeight_2018


def prefiringWeight(**kwargs):
    isMC = kwargs.pop("isMC")
    if not isMC:
        return lambda: DummyModule(**kwargs)
    return lambda: PrefCorr(**kwargs)


def tauCorrections(**kwargs):
    isMC = kwargs.pop("isMC")
    year = int(kwargs.pop("year"))
    if not isMC:
        return lambda: DummyModule(**kwargs)
    if year == 2016:
        return lambda: TauCorrectionsProducer('2016Legacy', **kwargs)
    elif year == 2017:
        return lambda: TauCorrectionsProducer('2017ReReco', **kwargs)
    elif year == 2018:
        return lambda: TauCorrectionsProducer('2018ReReco', **kwargs)
    