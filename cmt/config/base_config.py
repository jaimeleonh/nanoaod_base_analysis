from analysis_tools import ObjectCollection, Category, Process, Dataset, Feature
from analysis_tools.utils import DotDict
from plotting_tools import Label

class Config():
    def __init__(self, name, year, ecm, lumi_fb=None, lumi_pb=None, **kwargs):
        self.name=name
        self.year=year
        self.ecm=ecm
        assert lumi_fb or lumi_pb
        if lumi_fb:
            self.lumi_fb = lumi_fb
            self.lumi_pb = lumi_fb * 1000.
        else:
            self.lumi_fb = lumi_pb / 1000.
            self.lumi_pb = lumi_pb 

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
                " && Htt_svfit_mass_nom < 150"),
            Category("tautau", "#tau#tau channel", selection="pairType == 2")
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_sm", Label("GGFSM"), color=(0, 0, 0), isSignal=True),
            Process("dy", Label("DY"), color=(0, 0, 0)),
            Process("tt", Label("t#bar{t}"), color=(0, 0, 0)),
            Process("tt_dl", Label("t#bar{t} DL"), color=(0, 0, 0), parent_process="tt"),
            Process("tt_sl", Label("t#bar{t} SL"), color=(0, 0, 0), parent_process="tt"),
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
                "/store/mc/RunIIAutumn18NanoAODv7/"
                "GluGluToHHTo2B2Tau_node_cHHH1_TuneCP5_PSWeights_13TeV-powheg-pythia8"
                "/NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/",
                self.processes.get("ggf_sm"),
                prefix="cms-xrd-global.cern.ch//",
                locate="ingrid-se04.cism.ucl.ac.be:1094/",
                xs=0.001726),

            # Background samples
            Dataset("dy_high",
                "/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/"
                "RunIIAutumn18NanoAOD-102X_upgrade2018_realistic_v15-v1/NANOAODSIM/",
                self.processes.get("dy"),
                prefix="cms-xrd-global.cern.ch//",
                locate="xrootd.cmsaf.mit.edu:1094/",
                xs=6077.22),
            Dataset("tt_dl",
                "/store/mc/RunIIAutumn18NanoAODv7/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/"
                "NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/",
                self.processes.get("tt_dl"),
                prefix="cms-xrd-global.cern.ch//",
                locate="ingrid-se04.cism.ucl.ac.be:1094/",
                xs=365.34),
            Dataset("tt_sl",
                "/store/mc/RunIIAutumn18NanoAODv7/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/"
                "NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21_ext3-v1/",
                self.processes.get("tt_sl"),
                prefix="cms-xrd-global.cern.ch//",
                locate="se-xrd01.jinr-t1.ru:1095/",
                xs=88.29),
            
            # Tau 2018
            Dataset("data_tau_a",
                "/store/data/Run2018A/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="A",
                prefix="xrootd-cms.infn.it//",
                locate="grid-dcache.physik.rwth-aachen.de:1094/"),
            Dataset("data_tau_b",
                "/store/data/Run2018B/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="B",
                prefix="xrootd-cms.infn.it//",
                locate="se-xrd01.jinr-t1.ru:1095/"),
            Dataset("data_tau_c",
                "/store/data/Run2018C/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="C",
                prefix="xrootd-cms.infn.it//",
                locate="cms03.lcg.cscs.ch:1094/"),
            Dataset("data_tau_d",
                "/store/data/Run2018D/Tau/NANOAOD/02Apr2020-v2/",
                self.processes.get("data_tau"),
                runPeriod="D",
                prefix="xrootd-cms.infn.it//",
                locate="se-xrd01.jinr-t1.ru:1095/"),

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
        weights.channels = {
            "mutau": ["genWeight", "puWeight", "PrefireWeight", "trigSF"],
            "etau": ["genWeight", "puWeight", "PrefireWeight", "trigSF"],
            "tautau": ["genWeight", "puWeight", "PrefireWeight", "trigSF",
                "Tau_sfDeepTau2017v2p1VSjet_Medium.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSjet_Medium.at(dau2_index)",
                "Tau_sfDeepTau2017v2p1VSe_VVLoose.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSe_VVLoose.at(dau2_index)",
                "Tau_sfDeepTau2017v2p1VSmu_VLoose.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSmu_VLoose.at(dau2_index)"]
        }
        return weights


config = Config("base", year=2018, ecm=13, lumi_pb=59741)