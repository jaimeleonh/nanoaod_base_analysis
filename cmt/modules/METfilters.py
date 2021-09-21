from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop import Module

# extracted from
# https://twiki.cern.ch/twiki/bin/viewauth/CMS/MissingETOptionalFiltersRun2

class MetFilterProducer(Module):
    def __init__(self, isMC, year, *args, **kwargs):
        super(MetFilterProducer, self).__init__(*args, **kwargs)
        self.isMC = isMC
        self.year = int(year)

    def beginFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def endFile(self, inputFile, outputFile, inputTree, wrappedOutputTree):
        pass

    def analyze(self, event):
        """process event, return True (go to next module) or False (fail, go to next event)"""
        flags = 0
        flags += (1 if event.Flag_goodVertices else 0)
        flags += (1 if event.Flag_globalSuperTightHalo2016Filter else 0)
        flags += (1 if event.Flag_HBHENoiseFilter else 0)
        flags += (1 if event.Flag_HBHENoiseIsoFilter else 0)
        flags += (1 if event.Flag_EcalDeadCellTriggerPrimitiveFilter else 0)
        flags += (1 if event.Flag_BadPFMuonFilter else 0)
        flags += (1 if event.Flag_eeBadScFilter and not self.isMC else 0)
        flags += (1 if event.Flag_ecalBadCalibFilterV2 and self.year != 2016 else 0)

        # all True -> only 2017 and 2018 data
        if flags == 8:  
            return True
        # all True but one -> MC from 2017 or 2018 or data from 2016
        elif flags == 7 and ((self.isMC and self.year != 2016)
                or (not self.isMC and self.year == 2016)):
            return True
        # all True but two -> MC from 2016
        elif flags == 6 and self.isMC and self.year == 2016:
            return True

        return False


def MetFilter(**kwargs):
    return lambda: MetFilterProducer(**kwargs)
