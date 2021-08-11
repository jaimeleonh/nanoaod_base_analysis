from analysis_tools.utils import import_root

ROOT = import_root()

class JetPair:
    """Container class to pair and order tau decay candidates."""
    def __init__(self, obj1, obj2, jet_syst, **kwargs):
        self.obj1 = obj1
        tlv_obj1 = ROOT.TLorentzVector()
        tlv_obj1.SetPtEtaPhiM(eval("obj1.pt%s" % jet_syst),
            obj1.eta, obj1.phi, eval("obj1.mass%s" % jet_syst))
        self.obj2 = obj2
        tlv_obj2 = ROOT.TLorentzVector()
        tlv_obj2.SetPtEtaPhiM(eval("obj2.pt%s" % jet_syst),
            obj2.eta, obj2.phi, eval("obj2.mass%s" % jet_syst))
        self.inv_mass = (tlv_obj1 + tlv_obj2).M()

        if "index1" in kwargs:
            self.obj1_index = kwargs["index1"]
        if "index2" in kwargs:
            self.obj2_index = kwargs["index2"]

    def __gt__(self, opair):
        return self.inv_mass > opair.inv_mass
