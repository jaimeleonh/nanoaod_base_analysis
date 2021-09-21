from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root

ROOT = import_root()

class EventCounter(Module):
    def __init__(self, *args, **kwargs):
        self.histo_name = kwargs.pop("histo_name", "initial_count")

    def beginJob(self):
        pass

    def endJob(self):
        pass

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        self.histo = ROOT.TH1D(self.histo_name, "", 20, 0, 20)
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        prevdir = ROOT.gDirectory
        outputFile.cd()
        if "histos" not in [key.GetName() for key in outputFile.GetListOfKeys()]:
            outputFile.mkdir("histos")
        outputFile.cd("histos")
        self.histo.Write()
        prevdir.cd()
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        self.histo.Fill(0)
        return True


def Counter(**kwargs):
    return lambda: EventCounter(**kwargs)