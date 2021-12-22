import re
import os
from math import sqrt
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel import Collection

def Phi_mpi_pi(x):
    while (x >= 3.14159):
        x -= (2 * 3.14159)
    while (x < -3.14159):
        x += (2 * 3.14159)
    return x;

def deltaR(obj1_eta, obj1_phi, obj2_eta, obj2_phi):
    deta = obj1_eta - obj2_eta
    dphi = Phi_mpi_pi(obj1_phi - obj2_phi)
    return sqrt(deta * deta + dphi * dphi)


# extracted from https://github.com/cms-tau-pog/TauFW/blob/master/PicoProducer/python/analysis/utils.py
class LeptonPair:
    """Container class to pair and order tau decay candidates."""
    def __init__(self, obj1, iso1, pt1, obj2, iso2, pt2):
        self.obj1 = obj1
        self.obj2 = obj2
        self.pt1  = pt1
        self.pt2  = pt2
        self.iso1 = iso1
        self.iso2 = iso2
        self.pair = [obj1, obj2]
      
    def __gt__(self, opair):
        """Order dilepton pairs according to the pT of both objects first, then in isolation."""
        if   self.pt1  != opair.pt1:  return self.pt1  > opair.pt1  # greater = higher pT
        elif self.pt2  != opair.pt2:  return self.pt2  > opair.pt2  # greater = higher pT
        elif self.iso1 != opair.iso1: return self.iso1 < opair.iso1 # greater = smaller isolation
        elif self.iso2 != opair.iso2: return self.iso2 < opair.iso2 # greater = smaller isolation
        return True

    def check_charge(self):
        if (self.obj1.charge + self.obj2.charge == 0) and abs(self.obj1.charge) == 1:
            return True
        else:
            return False

  
class LeptonTauPair(LeptonPair):
  def __gt__(self, opair):
    """Override for tau isolation."""
    if   self.pt1  != opair.pt1:  return self.pt1  > opair.pt1  # greater = higher pT
    elif self.pt2  != opair.pt2:  return self.pt2  > opair.pt2  # greater = higher pT
    elif self.iso1 != opair.iso1: return self.iso1 < opair.iso1 # greater = smaller lepton isolation
    elif self.iso2 != opair.iso2: return self.iso2 > opair.iso2 # greater = larger tau isolation
    return True


class TriggerChecker:
    """Class to check whether the event passed certain HLT trigger paths"""
    def __init__(self, year, path="$CMT_BASE/cmt/modules/tau_triggers/tau_triggers_{}.json"):
        self.year = year
        self.path = os.path.expandvars(path.format(year))
        self.json = self.load_json(self.path)
    
    def load_json(self, path):
        import json
        with open(path) as f:
            d = json.load(f)
        return d

    def is_function(self, event, triggers):
        if callable(triggers):
            return triggers(event)
        else:
            return triggers

    def check_mutau(self, event, pt1, eta1, phi1, pt2, eta2, phi2,
            th1=None, th2=None, abs_th1=None, abs_th2=None):
        assert hasattr(self, "mutau_triggers") and hasattr(self, "mutau_crosstriggers")

        # check single lepton triggers
        mutau_triggers = self.is_function(event, self.mutau_triggers)
        for trigger in mutau_triggers:
            # muon and tau eta requirements
            if abs(eta1) > 2.1 or abs(eta2) > 2.3:
                continue

            if abs_th1:
                if pt1 < abs_th1: continue
            elif th1:
                pt_th = int(re.search(r"Mu[0-9]+", trigger).group()[2:])
                if pt1 < pt_th + th1: continue
            else:
                raise ValueError("Mu trigger pt thresholds are not set")
            if (eval("event." + trigger) != 1):
                continue
            trigObjs = Collection(event, "TrigObj")
            if self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Muon"):
                return True

        # check cross triggers
        mutau_crosstriggers = self.is_function(event, self.mutau_crosstriggers)
        for cross_trigger in mutau_crosstriggers:
            trigger = cross_trigger
            # muon and tau eta requirements
            if abs(eta1) > 2.1 or abs(eta2) > 2.1:
                continue

            # mu pt requirements
            if abs_th1:
                if pt1 < abs_th1: continue
            elif th1:
                pt_th = int(re.search(r"Mu[0-9]+", cross_trigger).group()[2:])
                if pt1 < pt_th + th1: continue
            else:
                raise ValueError("Mu cross trigger pt thresholds are not set")
            # tau pt requirements
            if abs_th2:
                if pt2 < abs_th2: continue
            elif th2:
                pt_th = int(re.search(r"Tau[0-9]+", trigger.replace("TauHPS", "Tau")).group()[3:])
                if pt2 < pt_th + th2: continue
            else:
                raise ValueError("Tau cross trigger pt thresholds are not set")
            if eval("event." + cross_trigger) != 1:
                continue
            trigObjs = Collection(event, "TrigObj")
            if (self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Muon") and 
                    self.match_hlt_object(trigger, trigObjs, eta2, phi2, "Tau")):
                return True
        return False
        
    def check_etau(self, event, pt1, eta1, phi1, pt2, eta2, phi2,
            th1=None, th2=None, abs_th1=None, abs_th2=None):
        assert hasattr(self, "etau_triggers") and hasattr(self, "etau_crosstriggers")

        # check single lepton triggers
        etau_triggers = self.is_function(event, self.etau_triggers)
        for trigger in etau_triggers:
            # muon and tau eta requirements
            if abs(eta1) > 2.1 or abs(eta2) > 2.3:
                continue

            if abs_th1:
                if pt1 < abs_th1: continue
            elif th1:
                pt_th = int(re.search(r"Ele[0-9]+", trigger).group()[3:])
                # print trigger, pt_th, pt1 + th1
                if pt1 < pt_th + th1: continue
            else:
                raise ValueError("E trigger pt thresholds are not set")
            if eval("event." + trigger) != 1:
                continue
            trigObjs = Collection(event, "TrigObj")
            # print trigger, self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Electron")
            if self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Electron"):
                return True

        # check cross triggers
        etau_crosstriggers = self.is_function(event, self.etau_crosstriggers)
        for cross_trigger in etau_crosstriggers:
            trigger = cross_trigger

            # muon and tau eta requirements
            if abs(eta1) > 2.1 or abs(eta2) > 2.1:
                continue

            # mu pt requirements
            if abs_th1:
                if pt1 < abs_th1: continue
            elif th1:
                pt_th = int(re.search(r"Ele[0-9]+", cross_trigger).group()[3:])
                # print trigger, pt1, pt_th + th1
                if pt1 < pt_th + th1: continue
            else:
                raise ValueError("E cross trigger pt thresholds are not set")
            # tau pt requirements
            if abs_th2:
                if pt2 < abs_th2: continue
            elif th2:
                pt_th = int(re.search(r"Tau[0-9]+", trigger.replace("TauHPS", "Tau")).group()[3:])
                # print trigger, pt2, pt_th + th2
                if pt2 < pt_th + th2: continue
            else:
                raise ValueError("Tau cross trigger pt thresholds are not set")
            if eval("event." + cross_trigger) != 1:
                continue
            trigObjs = Collection(event, "TrigObj")
            if (self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Electron")
                    and self.match_hlt_object(trigger, trigObjs, eta2, phi2, "Tau")):
                return True
        return False

    def check_tautau(self, event, pt1, eta1, phi1, pt2, eta2, phi2,
            th1=None, th2=None, abs_th1=None, abs_th2=None):
        assert hasattr(self, "tautau_triggers")

        # muon and tau eta requirements
        if abs(eta1) > 2.1 or abs(eta2) > 2.1:
            return False

        # check tau triggers
        tautau_triggers = self.is_function(event, self.tautau_triggers)
        for tautau_trigger in tautau_triggers:
            trigger = tautau_trigger
            # tau1 pt requirements
            if abs_th1:
                if pt1 < abs_th1: continue
            elif th1:
                pt_th = int(re.search(r"Tau[0-9]+", trigger.replace("TauHPS", "Tau")).group()[3:])
                if pt1 < pt_th + th1: continue
            else:
                raise ValueError("Tau1 trigger pt thresholds are not set")
            # tau2 pt requirements
            if abs_th2:
                if pt2 < abs_th2: continue
            elif th2:
                pt_th = int(re.search(r"Tau[0-9]+", trigger).group()[3:])
                if pt2 < pt_th + th2: continue
            else:
                raise ValueError("Tau2 trigger pt thresholds are not set")
            if eval("event." + tautau_trigger) != 1:
                continue
            trigObjs = Collection(event, "TrigObj")
            if (self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Tau")
                    and self.match_hlt_object(trigger, trigObjs, eta2, phi2, "Tau")):
                return True
        return False

    def check_vbftautau(self, event, pt1, eta1, phi1, pt2, eta2, phi2,
            th1=None, th2=None, abs_th1=None, abs_th2=None):
        assert hasattr(self, "vbf_triggers")

        # muon and tau eta requirements
        if abs(eta1) > 2.1 or abs(eta2) > 2.1:
            return False

        # check tau triggers
        vbf_triggers = self.is_function(event, self.vbf_triggers)
        for tautau_trigger in vbf_triggers:
            trigger = tautau_trigger
            # tau1 pt requirements
            if abs_th1:
                if pt1 < abs_th1: continue
            elif th1:
                pt_th = int(re.search(r"Tau[0-9]+", trigger.replace("TauHPS", "Tau")).group()[3:])
                if pt1 < pt_th + th1: continue
            else:
                raise ValueError("Tau1 trigger pt thresholds are not set")
            # tau2 pt requirements
            if abs_th2:
                if pt2 < abs_th2: continue
            elif th2:
                pt_th = int(re.search(r"Tau[0-9]+", trigger).group()[3:])
                if pt2 < pt_th + th2: continue
            else:
                raise ValueError("Tau2 trigger pt thresholds are not set")
            if eval("event." + tautau_trigger) != 1:
                continue
            trigObjs = Collection(event, "TrigObj")
            if (self.match_hlt_object(trigger, trigObjs, eta1, phi1, "Tau")
                    and self.match_hlt_object(trigger, trigObjs, eta2, phi2, "Tau")):
                return True
        return False

    def match_hlt_object(self, trigger_path, trigger_objs, offline_eta, offline_phi, obj_type):
        assert obj_type in ["Tau", "Electron", "Muon"]
        filter_bits = self.json["hltpaths"][trigger_path][obj_type]["filterbits"]
        for trigger_obj in trigger_objs:
            matched_trigger_bits = True
            for filter_bit in filter_bits:
                if (trigger_obj.filterBits & self.json["filterbits"][obj_type][filter_bit]) == 0:
                    matched_trigger_bits = False
                    break
            if not matched_trigger_bits:
                continue
            if deltaR(offline_eta, offline_phi, trigger_obj.eta, trigger_obj.phi) < 0.5:
                return True
        return False


def lepton_veto(electrons, muons, taus, obj=None):
    # https://twiki.cern.ch/twiki/bin/viewauth/CMS/HiggsToTauTauWorkingLegacyRun2#Common_lepton_vetoes
    nleps = 0
    # check extra muon veto

    # print "muons: "
    for muon in muons:
        # print muon.pt, muon.eta, muon.phi, muon.dz, muon.dxy, muon.pfRelIso04_all, muon.mediumId, muon.tightId,
        if obj == muon:
            # print "mymuon"
            continue
        # if any([muon.DeltaR(tau) < 0.4 for tau in taus]):
            # print "deltaR"
            # continue
        if (abs(muon.eta) > 2.4 or muon.pt < 10 or abs(muon.dz) > 0.2
                or abs(muon.dxy) > 0.045 or muon.pfRelIso04_all > 0.3):
            # print "stuff"
            continue
        if not (muon.mediumId or muon.tightId):
            continue
        nleps += 1

    # check extra electron veto
    # print "electrons: "
    for electron in electrons:
        # print electron.pt, electron.eta, electron.phi, electron.dz, electron.dxy, electron.pfRelIso03_all, electron.convVeto, electron.lostHits, electron.mvaFall17V2Iso_WP90,
        if obj == electron:
            # print "mye"
            continue
        # if any([electron.DeltaR(tau) < 0.4 for tau in taus]):
            # print "deltaR"
            # continue
        if (abs(electron.eta) > 2.5 or electron.pt < 10 or abs(electron.dz) > 0.2
                or abs(electron.dxy) > 0.045):
            # print "stuff"
            continue
        if not ((electron.pfRelIso03_all < 0.3 and electron.mvaFall17V2noIso_WP90)
                or electron.mvaFall17V2Iso_WP90):
            continue
        # if electron.convVeto == 1 and electron.lostHits <= 1:
            # print
        nleps += 1
        # print "cosas raras"

    return (nleps > 0), nleps
