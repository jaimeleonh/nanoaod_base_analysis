from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root

ROOT = import_root()


class HHKinFit(Module):
    def __init__(self, jetSelection):
        pass

    def beginJob(self):
        pass

    def endJob(self):
        pass

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.out = wrappedOutputTree
        self.out.branch("EventMass", "F")

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        electrons = Collection(event, "Electron")
        muons = Collection(event, "Muon")
        jets = Collection(event, "Jet")
        eventSum = ROOT.TLorentzVector()
        for lep in muons:
            eventSum += lep.p4()
        for lep in electrons:
            eventSum += lep.p4()
        for j in filter(self.jetSel, jets):
            eventSum += j.p4()
        self.out.fillBranch("EventMass", eventSum.M())
        return True