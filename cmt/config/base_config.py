from analysis_tools import ObjectCollection, Category, Process, Dataset, Feature
from analysis_tools.utils import DotDict
from plotting_tools import Label

class Config():
    def __init__(self, name, year, ecm, lumi, **kwargs):
        self.name=name
        self.year=year
        self.ecm=ecm
        self.lumi=lumi
        self.x = kwargs

        self.categories = self.add_categories()
        self.processes, self.process_group_names = self.add_processes()
        self.datasets = self.add_datasets()
        self.features = self.add_features()
        self.versions = self.add_versions()
        self.weights = self.add_weights()

    def add_categories(self):
        categories = [
            Category("base", "base category"),
            Category("dum", "dummy category", selection="Htt_svfit_mass_nom > 50 "
                " && Htt_svfit_mass_nom < 150")
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_sm", Label("GGFSM"), color=(0, 0, 0), isSignal=True),
            Process("data_tau", Label(latex="DATA\\_TAU"), color=(255, 255, 255), isData=True)
        ]

        process_group_names = {
            "default": [
                "ggf_sm",
            ]
        }
        return ObjectCollection(processes), process_group_names

    def add_datasets(self):
        datasets = [
            Dataset("ggf_sm",
                "/store/mc/RunIIAutumn18NanoAODv7/GluGluToHHTo2B2Tau_node_cHHH1_TuneCP5_PSWeights_13TeV-powheg-pythia8"
                "/NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/",
                self.processes.get("ggf_sm"),
                prefix="ingrid-se04.cism.ucl.ac.be:1094/",
                xs=0.001726),
            Dataset("data_dum",
                "/store/data/Run2018A/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="A",
                prefix="grid-dcache.physik.rwth-aachen.de:1094/")
        ]
        return ObjectCollection(datasets)

    def add_features(self):
        features = [
            Feature("Htt_svfit_mass", "Htt_svfit_mass", binning=(10, 50, 150),
                x_title=Label("H(#tau^{+} #tau^{-}) (SVfit) mass"),
                units="GeV",
                central="nom"),
            Feature("bjet1_pt", "Jet_pt.at(bjet1_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_1 p_t"),
                units="GeV",
                central="nom"),
            Feature("lep1_pt", "dau1_pt", binning=(40, 0, 400),
                x_title=Label("lep_1 p_t"),
                units="GeV",
                central=""),
        ]
        return ObjectCollection(features)

    def add_versions(self):
        versions = {}
        return versions

    def add_weights(self):
        weights = DotDict()
        weights.default = "1"
        return weights


config = Config("base", year=2018, ecm=13, lumi=59741)