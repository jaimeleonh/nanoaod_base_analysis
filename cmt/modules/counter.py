from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module
from analysis_tools.utils import import_root

ROOT = import_root()

class CounterProducer(Module):
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


class WeightedCounterProducer(CounterProducer):
    def __init__(self, *args, **kwargs):
        super(WeightedCounterProducer, self).__init__(*args, **kwargs)
        self.weights = kwargs.pop("weights", ["1."])
        isMC = kwargs.pop("isMC")
        if not isMC:
            self.weights = ["1."]

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        weights = [("event." if weight != "1." else "") + "%s" % weight for weight in self.weights]
        self.histo.Fill(0)
        self.histo.Fill(1, eval(" * ".join(weights)))
        return True


def Counter(**kwargs):
    return lambda: CounterProducer(**kwargs)


def WeightedCounter(**kwargs):
    return lambda: WeightedCounterProducer(**kwargs)