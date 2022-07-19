from analysis_tools import ObjectCollection, Category, Process, Dataset, Feature, Systematic
from analysis_tools.utils import DotDict
from analysis_tools.utils import join_root_selection as jrs
from plotting_tools import Label
from collections import OrderedDict

from cmt.config.base_config import Config

class Config_ul_2018(Config):
    
    def __init__(self, name, year, ecm, lumi_fb=None, lumi_pb=None, **kwargs):
        super(Config_ul_2018, self).__init__(name, year, ecm, lumi_fb, lumi_pb, **kwargs)

    def add_datasets(self):
        datasets = [
            Dataset("ggf_sm",
                dataset="/GluGluToHHTo2B2Tau_TuneCP5_PSWeights_node_SM_13TeV-madgraph-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("ggf_sm"),
                xs=0.03105,
                tags=["ul"]),
            Dataset("ggf_sm_test",
                folder="/eos/user/j/jleonhol/NanoTest/GluGluToHHTo2B2Tau_TuneCP5_PSWeights_node_SM_13TeV-madgraph-pythia8/NanoTestFullDis/220706_103244/0000",
                process=self.processes.get("ggf_sm"),
                xs=0.03105,
                tags=["ul"]),
            # Dataset("vbf_sm",
                # dataset="/VBFHHTo2B2Tau_CV_1_C2V_1_C3_1_dipoleRecoilOff"
                # "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                # "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                # process=self.processes.get("vbf_sm"),
                # # prefix="xrootd-cms.infn.it//",
                # xs=0.001726),

            # Background samples
            Dataset("dy_high",
                dataset="/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("dy_high"),
                # prefix="xrootd-cms.infn.it//",
                # prefix="cms-xrd-global.cern.ch//",
                xs=6077.22, 
                merging={
                    "tautau": 20,
                    "etau": 20,
                    "mutau": 20,
                },
                splitting=100000,
                tags=["ul"]),
            Dataset("tt_dl",
                dataset="/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("tt_dl"),
                # prefix="xrootd-cms.infn.it//",
                xs=88.29, 
                merging={
                    "tautau": 20,
                    "mutau": 20,
                    "etau": 40,
                },
                scaling=(0.96639, 0.00863),
                tags=["ul"]),
            Dataset("tt_sl",
                dataset="/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("tt_sl"),
                # prefix="xrootd-cms.infn.it//",
                xs=365.34,
                merging={
                    "tautau": 20,
                    "mutau": 60,
                    "etau": 40,
                },
                scaling=(0.96639, 0.00863),
                tags=["ul"]),
            Dataset("tt_fh",
                dataset="/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("tt_fh"),
                # prefix="xrootd-cms.infn.it//",
                xs=377.96,
                scaling=(0.96639, 0.00863),
                tags=["ul"]),

            # Others
            # ttH
            Dataset("tth_bb",
                dataset="/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM",
                process=self.processes.get("tth_bb"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.2953,
                splitting=200000,
                tags=["ul"]),
            Dataset("tth_tautau",
                dataset="/ttHToTauTau_M125_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("tth_tautau"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.031805,
                splitting=200000,
                tags=["ul"]),
            Dataset("tth_nonbb",
                dataset="/ttHToNonbb_M125_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM",
                process=self.processes.get("tth_nonbb"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.17996,
                splitting=200000,
                tags=["ul"]),

            # Wjets
            Dataset("wjets",
                dataset="/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("wjets"),
                # prefix="xrootd-cms.infn.it//",
                xs=61526.7,
                merging={
                    "tautau": 5,
                    "etau": 10,
                },
                splitting=200000,
                tags=["ul"]),
            # tW
            Dataset("st_tw_antitop",
                dataset="/ST_tW_antitop_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM",
                process=self.processes.get("tw"),
                # prefix="xrootd-cms.infn.it//",
                xs=35.85,
                splitting=200000,
                tags=["ul"]),
            Dataset("st_tw_top",
                dataset="/ST_tW_top_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM",
                process=self.processes.get("tw"),
                # prefix="xrootd-cms.infn.it//",
                xs=35.85,
                splitting=200000,
                tags=["ul"]),
            # single top
            # Dataset("st_antitop",
                # dataset="/ST_t-channel_antitop_5f_TuneCP5_13TeV-powheg-pythia8/"
                # "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                # process=self.processes.get("singlet"),
                # # prefix="xrootd-cms.infn.it//",
                # xs=80.95,
                # splitting=200000),
            Dataset("st_top",
                dataset="/ST_t-channel_antitop_5f_InclusiveDecays_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM",
                process=self.processes.get("singlet"),
                # prefix="xrootd-cms.infn.it//",
                xs=136.02,
                splitting=200000,
                tags=["ul"]),

            # DATA
            # Tau 2018
            Dataset("data_tau_a",
                dataset="/Tau/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                selection="pairType == 2",
                process=self.processes.get("data_tau"),
                runPeriod="A",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                tags=["ul"]),
            Dataset("data_tau_b",
                dataset="/Tau/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                selection="pairType == 2",
                process=self.processes.get("data_tau"),
                runPeriod="B",
                # prefix="xrootd-cms.infn.it//",
                # locate="se-xrd01.jinr-t1.ru:1095/",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                tags=["ul"]),
            Dataset("data_tau_c",
                dataset="/Tau/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                selection="pairType == 2",
                process=self.processes.get("data_tau"),
                runPeriod="C",
                # prefix="xrootd-cms.infn.it//",
                # locate="cms03.lcg.cscs.ch:1094/",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                tags=["ul"]),
            Dataset("data_tau_d",
                dataset="/Tau/Run2018D-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD",
                selection="pairType == 2",
                process=self.processes.get("data_tau"),
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                tags=["ul"]),

            # EGamma 2018
            Dataset("data_etau_a",
                dataset="/EGamma/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                selection="pairType == 1",
                process=self.processes.get("data_etau"),
                runPeriod="A",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 10,
                },
                tags=["ul"]),
            Dataset("data_etau_b",
                dataset="/EGamma/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                selection="pairType == 1",
                process=self.processes.get("data_etau"),
                runPeriod="B",
                # prefix="xrootd-cms.infn.it//",
                # locate="se-xrd01.jinr-t1.ru:1095/",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 10,
                },
                tags=["ul"]),
            Dataset("data_etau_c",
                dataset="/EGamma/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                selection="pairType == 1",
                process=self.processes.get("data_etau"),
                runPeriod="C",
                # prefix="xrootd-cms.infn.it//",
                # locate="cms03.lcg.cscs.ch:1094/",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 10,
                },
                tags=["ul"]),
            Dataset("data_etau_d",
                dataset="/EGamma/Run2018D-UL2018_MiniAODv2_NanoAODv9-v3/NANOAOD",
                selection="pairType == 1",
                process=self.processes.get("data_etau"),
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 10,
                },
                tags=["ul"]),

            # SingleMuon 2018
            Dataset("data_mutau_a",
                dataset="/SingleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD",
                selection="pairType == 0",
                process=self.processes.get("data_mutau"),
                runPeriod="A",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "mutau": 20,
                    "tautau": 10,
                    "etau": 10,
                },
                tags=["ul"]),
            Dataset("data_mutau_b",
                dataset="/SingleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD",
                process=self.processes.get("data_mutau"),
                selection="pairType == 0",
                runPeriod="B",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "mutau": 20,
                    "tautau": 10,
                    "etau": 10,
                },
                tags=["ul"]),
            Dataset("data_mutau_c",
                dataset="/SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD",
                process=self.processes.get("data_mutau"),
                selection="pairType == 0",
                runPeriod="C",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "mutau": 20,
                    "tautau": 10,
                    "etau": 10,
                },
                tags=["ul"]),
            Dataset("data_mutau_d",
                dataset="/SingleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD",
                process=self.processes.get("data_mutau"),
                selection="pairType == 0",
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
                merging={
                    "mutau": 20,
                    "tautau": 10,
                    "etau": 10,
                },
                tags=["ul"]),

        ]
        return ObjectCollection(datasets)

    def add_weights(self):
        weights = DotDict()
        weights.default = "1"
        # weights.total_events_weights = ["genWeight", "puWeight", "DYstitchWeight"]
        weights.total_events_weights = ["genWeight", "puWeight"]
        weights.channels = {
            "mutau": ["genWeight", "puWeight", "prescaleWeight", "trigSF",
                "idAndIsoAndFakeSF_deep_pt", "L1PreFiringWeight_Nom", "PUjetID_SF", "bTagweightReshape2"
            ],
        }
        weights.channels["etau"] = weights.channels["mutau"]
        weights.channels["tautau"] = weights.channels["mutau"]

        weights.channels_mult = {channel: jrs(weights.channels[channel], op="*")
            for channel in weights.channels}
        return weights

config = Config_ul_2018("ul_2018", year=2018, ecm=13, lumi_pb=59741)
