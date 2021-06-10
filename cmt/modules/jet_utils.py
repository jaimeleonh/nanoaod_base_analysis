from analysis_tools.utils import import_root

ROOT = import_root()

def deltaRSmearedJet(jet, tau, jet_pt="pt_nom"):
    jet_tlv = ROOT.TLorentzVector()
    jet_tlv.SetPtEtaPhiM(jet[jet_pt], jet.eta, jet.phi, jet.mass_nom)

    tau_tlv = ROOT.TLorentzVector()
    tau_tlv.SetPtEtaPhiM(tau.pt, tau.eta, tau.phi, tau.eta)
    
    return jet_tlv.DeltaR(tau_tlv)


class JetPair:
    """Container class to pair and order tau decay candidates."""
    def __init__(self, obj1, obj2, **kwargs):
        self.obj1 = obj1
        tlv_obj1 = ROOT.TLorentzVector()
        tlv_obj1.SetPtEtaPhiM(obj1.pt, obj1.eta, obj1.phi, obj1.mass)
        self.obj2 = obj2
        tlv_obj2 = ROOT.TLorentzVector()
        tlv_obj2.SetPtEtaPhiM(obj1.pt, obj1.eta, obj1.phi, obj1.mass)
        self.inv_mass = (tlv_obj1 + tlv_obj2).M()

        if "index1" in kwargs:
            self.obj1_index = kwargs["index1"]
        if "index2" in kwargs:
            self.obj2_index = kwargs["index2"]

    def __gt__(self, opair):
        return self.inv_mass > opair.inv_mass
