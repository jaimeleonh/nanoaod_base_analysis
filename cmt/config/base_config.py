from analysis_tools import ObjectCollection, Category, Process, Dataset, Feature
from analysis_tools.utils import DotDict
from analysis_tools.utils import join_root_selection as jrs
from plotting_tools import Label
from collections import OrderedDict

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

        self.channels = self.add_channels()
        self.regions = self.add_regions()
        self.categories = self.add_categories()
        self.processes, self.process_group_names = self.add_processes()
        self.datasets = self.add_datasets()
        self.features = self.add_features()
        self.versions = self.add_versions()
        self.weights = self.add_weights()

    def join_selection_channels(self, selection):
        return jrs([jrs(jrs(selection[ch.name], op="and"), ch.selection, op="and")
            for ch in self.channels], op="or")

    def add_regions(self):
        selection = OrderedDict()
        region_names = ["Signal region", "OS inv. iso", "SS iso", "SS inv. iso"]
        selection["os_iso"] = {
            "mutau": ["isOS == 1", "dau1_iso < 0.15", "dau2_idDeepTau2017v2p1VSjet >= 5"],
            "etau": ["isOS == 1", "dau1_iso == 1", "dau2_idDeepTau2017v2p1VSjet >= 5"],
            "tautau": ["isOS == 1", "dau1_idDeepTau2017v2p1VSjet >= 5",
                "dau2_idDeepTau2017v2p1VSjet >= 5"],
        }
        selection["os_inviso"] = {
            "mutau": ["isOS == 1", "dau1_iso < 0.15", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 5"],
            "etau": ["isOS == 1", "dau1_iso == 1", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 5"],
            "tautau": ["isOS == 1", "dau1_idDeepTau2017v2p1VSjet >= 5",
                "dau2_idDeepTau2017v2p1VSjet >= 1", "dau2_idDeepTau2017v2p1VSjet < 5"],
        }
        selection["ss_iso"] = {
            "mutau": ["isOS == 0", "dau1_iso < 0.15", "dau2_idDeepTau2017v2p1VSjet >= 5"],
            "etau": ["isOS == 0", "dau1_iso == 1", "dau2_idDeepTau2017v2p1VSjet >= 5"],
            "tautau": ["isOS == 0", "dau1_idDeepTau2017v2p1VSjet >= 5",
                "dau2_idDeepTau2017v2p1VSjet >= 5"],
        }
        selection["ss_inviso"] = {
            "mutau": ["isOS == 0", "dau1_iso < 0.15", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 5"],
            "etau": ["isOS == 0", "dau1_iso == 1", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 5"],
            "tautau": ["isOS == 0", "dau1_idDeepTau2017v2p1VSjet >= 5",
                "dau2_idDeepTau2017v2p1VSjet >= 1", "dau2_idDeepTau2017v2p1VSjet < 5"],
        }
        regions = []
        for ikey, key in enumerate(selection):
            regions.append(Category(key, label=Label(region_names[ikey]),
                selection=self.join_selection_channels(selection[key])))
            for channel in self.channels:
                regions.append(Category("_".join([channel.name, key]),
                    label=Label(", ".join([channel.label.root, region_names[ikey]])),
                    selection=jrs(channel.selection,
                        jrs(selection[key][channel.name], op="and"), op="and")))
        return ObjectCollection(regions)

    def add_channels(self):
        channels = [
            Category("mutau", Label("#tau_{#mu}#tau_{h}"), selection="pairType == 0"),
            Category("etau", Label("#tau_{e}#tau_{h}"), selection="pairType == 1"),
            Category("tautau", Label("#tau_{h}#tau_{h}"), selection="pairType == 2"),
        ]
        return ObjectCollection(channels)

    def add_categories(self):
        categories = [
            Category("base", "base category"),
            Category("dum", "dummy category", selection="Htt_svfit_mass_nom > 50 "
                " && Htt_svfit_mass_nom < 150"),
            Category("mutau", "#mu#tau channel", selection="pairType == 0"),
            Category("etau", "e#tau channel", selection="pairType == 1"),
            Category("tautau", "#tau#tau channel", selection="pairType == 2"),
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_sm", Label("$HH_{ggF}$"), color=(0, 0, 0), isSignal=True),
            Process("dy", Label("DY"), color=(255, 102, 102)),
            Process("tt", Label("t#bar{t}"), color=(255, 153, 0)),
            Process("tt_dl", Label("t#bar{t} DL"), color=(205, 0, 9), parent_process="tt"),
            Process("tt_sl", Label("t#bar{t} SL"), color=(255, 153, 0), parent_process="tt"),
            Process("tt_fh", Label("t#bar{t} FH"), color=(131, 38, 10), parent_process="tt"),
            Process("data", Label("DATA"), color=(0, 0, 0), isData=True),
            Process("data_tau", Label("DATA\_TAU"), color=(0, 0, 0), parent_process="data", isData=True),
            Process("data_e", Label("DATA\_E"), color=(0, 0, 0), parent_process="data", isData=True),
            Process("data_mu", Label("DATA\_MU"), color=(0, 0, 0), parent_process="data", isData=True)
        ]

        process_group_names = {
            "default": [
                "ggf_sm",
                "data_tau",
                "tt_dl",
            ],
            "data_tau": [
                "data_tau",
            ],
            "bkg": [
                "tt_dl",
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
                xs=88.29, 
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                splitting=100000),
            Dataset("tt_sl",
                "/store/mc/RunIIAutumn18NanoAODv7/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/"
                "NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21_ext3-v1/",
                self.processes.get("tt_sl"),
                prefix="cms-xrd-global.cern.ch//",
                locate="se-xrd01.jinr-t1.ru:1095/",
                xs=365.34),
            
            # Tau 2018
            Dataset("data_tau_a",
                "/store/data/Run2018A/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="A",
                prefix="xrootd-cms.infn.it//",
                locate="grid-dcache.physik.rwth-aachen.de:1094/",
                splitting=200000,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_b",
                "/store/data/Run2018B/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="B",
                prefix="xrootd-cms.infn.it//",
                locate="se-xrd01.jinr-t1.ru:1095/",
                splitting=200000,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_c",
                "/store/data/Run2018C/Tau/NANOAOD/02Apr2020-v1/",
                self.processes.get("data_tau"),
                runPeriod="C",
                prefix="xrootd-cms.infn.it//",
                locate="cms03.lcg.cscs.ch:1094/",
                splitting=200000,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_d",
                "/store/data/Run2018D/Tau/NANOAOD/02Apr2020-v2/",
                self.processes.get("data_tau"),
                runPeriod="D",
                prefix="xrootd-cms.infn.it//",
                locate="se-xrd01.jinr-t1.ru:1095/",
                splitting=200000,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),

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
        weights.total_events_weights = ["genWeight", "puWeight"]
        weights.channels = {
            "mutau": ["genWeight", "puWeight", "PrefireWeight", "trigSF"],
            "etau": ["genWeight", "puWeight", "PrefireWeight", "trigSF"],
            # "tautau": ["genWeight", "puWeight", "PrefireWeight", "trigSF",
            "tautau": ["puWeight", "PrefireWeight", "trigSF",
                "Tau_sfDeepTau2017v2p1VSjet_Medium.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSjet_Medium.at(dau2_index)",
                "Tau_sfDeepTau2017v2p1VSe_VVLoose.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSe_VVLoose.at(dau2_index)",
                "Tau_sfDeepTau2017v2p1VSmu_VLoose.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSmu_VLoose.at(dau2_index)"]
        }
        return weights


config = Config("base", year=2018, ecm=13, lumi_pb=59741)
