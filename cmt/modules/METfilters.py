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
        if hasattr(event, "Flag_ecalBadCalibFilterV2") and self.year != 2016:
            flags += (1 if event.Flag_ecalBadCalibFilterV2 else 0)
        else:
            flags += 1

        # all True -> Data
        if flags == 8:  
            return True
        # all True but one -> MC 
        elif flags == 7 and self.isMC:
            return True

        return False


def MetFilter(**kwargs):
    return lambda: MetFilterProducer(**kwargs)
