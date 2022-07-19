from analysis_tools import ObjectCollection, Category, Process, Dataset, Feature, Systematic
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

        # https://twiki.cern.ch/twiki/bin/viewauth/CMS/BtagRecommendation102X
        self.btag=DotDict(tight=0.7264, medium=0.2770, loose=0.0494)

        self.channels = self.add_channels()
        self.regions = self.add_regions()
        self.categories = self.add_categories()
        self.processes, self.process_group_names, self.process_training_names = self.add_processes()
        self.datasets = self.add_datasets()
        self.features = self.add_features()
        self.versions = self.add_versions()
        self.weights = self.add_weights()
        self.systematics = self.add_systematics()

    def join_selection_channels(self, selection):
        return jrs([jrs(jrs(selection[ch.name], op="and"), ch.selection, op="and")
            for ch in self.channels], op="or")

    def combine_selections_per_channel(self, selection1, selection2):
        selection = DotDict()
        for channel in selection1:
            selection[channel] = jrs(selection1[channel], selection2[channel], op="or")
        return selection

    def add_regions(self):
        selection = OrderedDict()
        region_names = ["Signal region", "OS inv. iso", "SS iso", "SS inv. iso"]
        selection["os_iso"] = {
            "mutau": ["isOS == 1", "dau2_idDeepTau2017v2p1VSjet >= 31"],
            "etau": ["isOS == 1", "dau2_idDeepTau2017v2p1VSjet >= 31"],
            "tautau": ["isOS == 1", "dau1_idDeepTau2017v2p1VSjet >= 31",
                "dau2_idDeepTau2017v2p1VSjet >= 31"],
        }
        selection["os_inviso"] = {
            "mutau": ["isOS == 1", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 31"],
            "etau": ["isOS == 1", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 31"],
            "tautau": ["isOS == 1", "dau1_idDeepTau2017v2p1VSjet >= 31",
                "dau2_idDeepTau2017v2p1VSjet >= 1", "dau2_idDeepTau2017v2p1VSjet < 31"],
        }
        selection["ss_iso"] = {
            "mutau": ["isOS == 0", "dau2_idDeepTau2017v2p1VSjet >= 31"],
            "etau": ["isOS == 0",  "dau2_idDeepTau2017v2p1VSjet >= 31"],
            "tautau": ["isOS == 0", "dau1_idDeepTau2017v2p1VSjet >= 31",
                "dau2_idDeepTau2017v2p1VSjet >= 31"],
        }
        selection["ss_inviso"] = {
            "mutau": ["isOS == 0", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 31"],
            "etau": ["isOS == 0", "dau2_idDeepTau2017v2p1VSjet >= 1",
                "dau2_idDeepTau2017v2p1VSjet < 31"],
            "tautau": ["isOS == 0", "dau1_idDeepTau2017v2p1VSjet >= 31",
                "dau2_idDeepTau2017v2p1VSjet >= 1", "dau2_idDeepTau2017v2p1VSjet < 31"],
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
        reject_sel = ["pairType == -31415"]

        sel = DotDict()
        df = lambda i, op, wp: "Jet_btagDeepFlavB.at(bjet{}_JetIdx) {} {}".format(i, op, self.btag[wp])
        sel["btag"] = DotDict(
            m_first=[df(1, ">", "medium")],
            m_second=[df(2, ">", "medium")],
            m_any=[jrs(df(1, ">", "medium"), df(2, ">", "medium"), op="or")],
            l=[df(1, ">", "loose"), df(2, "<", "loose")],
            ll=[df(1, ">", "loose"), df(2, ">", "loose")],
            m=[jrs(jrs(df(1, ">", "medium"), df(2, "<", "medium"), op="and"),
                jrs(df(1, "<", "medium"), df(2, ">", "medium"), op="and"), op="or")],
            mm=[df(1, ">", "medium"), df(2, ">", "medium")],
            not_mm=[df(1, "<", "medium"), df(2, "<", "medium")],
        )

        _excl_vbf_loose_nob = ["[VBFjj_mass] > 500", "abs([VBFjj_deltaEta]) > 3",
            "isVBFtrigger == 0"]
        _excl_vbf_loose = _excl_vbf_loose_nob + sel.btag.m_any
        _excl_non_vbf_loose = ["!" + jrs(_excl_vbf_loose, op="and")]
        
        _excl_vbf_tight_nob = ["[VBFjet1_pt] > 140", "[VBFjet2_pt] > 60", "[VBFjj_mass] > 800",
            "abs([VBFjj_deltaEta]) > 3", "isVBFtrigger == 1"]
        _excl_vbf_tight = _excl_vbf_tight_nob + sel.btag.m_any
        _excl_non_vbf_tight = ["!" + jrs(_excl_vbf_tight, op="and")]

        _excl_non_vbf = ["!" + jrs(jrs(_excl_vbf_loose, op="and"), jrs(_excl_vbf_tight, op="and"),
            op="or")]

        mass_ellipse_sel = ["(([Htt_svfit_mass] - 129.) * ([Htt_svfit_mass] - 129.)/ (53. * 53.)"
            " + ([Hbb_mass] - 169.) * ([Hbb_mass] - 169.) / (145. * 145.)) < 1"]
        mass_boost_sel = ["(([Htt_svfit_mass] - 128.) * ([Htt_svfit_mass] - 128.) / (60. * 60.)"
            " + ([Hbb_mass] - 159.) * ([Hbb_mass] - 159.) / (94. * 94.)) < 1"]
        sel["resolved_1b"] = DotDict({
            ch: (sel.btag.m + mass_ellipse_sel + ["isBoosted != 1"]
                + _excl_non_vbf_loose)
            for ch in self.channels.names()
        })
        sel["resolved_1b_combined"] = self.join_selection_channels(sel["resolved_1b"])
        sel["resolved_2b"] = DotDict({
            ch: (sel.btag.mm + mass_ellipse_sel + ["isBoosted != 1"]
                + _excl_non_vbf)
            for ch in self.channels.names()
        })
        sel["resolved_2b_combined"] = self.join_selection_channels(sel["resolved_2b"])
        sel["boosted"] = DotDict({
            ch: (sel.btag.ll + mass_boost_sel + ["isBoosted == 1"]
                + _excl_non_vbf)
            for ch in self.channels.names()
        })
        sel["boosted_combined"] = self.join_selection_channels(sel["boosted"])
        sel["vbf_loose"] = DotDict({
            ch: _excl_vbf_loose
            for ch in self.channels.names()
        })
        sel["vbf_loose_combined"] = self.join_selection_channels(sel.vbf_loose)
        sel["vbf_tight"] = DotDict(
            mutau=reject_sel,  # category not used, should always reject
            etau=reject_sel,  # category not used, should always reject
            tautau=_excl_vbf_tight + sel.btag.m_any,
        )
        sel["vbf_tight_combined"] = self.join_selection_channels(sel.vbf_tight)
        sel["vbf"] = self.combine_selections_per_channel(sel.vbf_tight, sel.vbf_loose)
        sel["vbf_combined"] = self.join_selection_channels(sel.vbf)

        categories = [
            Category("base", "base category"),
            Category("base_selection", "base category",
                nt_selection="(Sum$(Tau_pt->fElements > 17) > 0"
                    " && ((Sum$(Muon_pt->fElements > 17) > 0"
                    " || Sum$(Electron_pt->fElements > 17) > 0)"
                    " || Sum$(Tau_pt->fElements > 17) > 1)"
                    " && Sum$(Jet_pt->fElements > 17) > 1)",
                selection="Tau_pt[Tau_pt > 17].size() > 0 "
                    "&& ((Muon_pt[Muon_pt > 17].size() > 0"
                    "|| Electron_pt[Electron_pt > 17].size() > 0)"
                    "|| Tau_pt[Tau_pt > 17].size() > 1)"
                    "&& Jet_pt[Jet_pt > 17].size() > 0"),
            # Category("dum", "dummy category", selection="event == 220524669"),
            Category("dum", "dummy category", selection="event == 74472670"),
            Category("mutau", "#mu#tau channel", selection="pairType == 0"),
            Category("etau", "e#tau channel", selection="pairType >= 1"),
            # Category("etau", "e#tau channel", selection="pairType >= -999"),
            # Category("etau", "e#tau channel", selection="1."),
            Category("tautau", "#tau#tau channel", selection="pairType == 2"),
            Category("resolved_1b", label="Resolved 1b category",
                selection=sel["resolved_1b_combined"]),
            Category("resolved_2b", label="Resolved 2b category",
                selection=sel["resolved_2b_combined"]),
            Category("boosted", label="Boosted category",
                selection=sel["boosted_combined"]),
            Category("vbf_loose", label="VBF (loose) category",
                selection=sel["vbf_loose_combined"]),
            Category("vbf_tight", label="VBF (tight) category",
                selection=sel["vbf_tight_combined"]),
            Category("vbf", label="VBF category",
                selection=sel["vbf_combined"]),
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf", Label("$HH_{ggF}$"), color=(0, 0, 0), isSignal=True, llr_name="ggH"),
            Process("ggf_sm", Label("$HH_{ggF}$"), color=(0, 0, 0), isSignal=True, parent_process="ggf"),

            Process("vbf", Label("$HH_{VBF}$"), color=(0, 0, 0), isSignal=True, llr_name="qqH"),
            Process("vbf_sm", Label("$HH_{VBF}$"), color=(0, 0, 0), isSignal=True, parent_process="vbf"),
            Process("vbf_0p5_1_1", Label("$HH_{VBF}^{(0.5,1,1)}$"), color=(0, 0, 0),
                isSignal=True, parent_process="vbf"),
            Process("vbf_1p5_1_1", Label("$HH_{VBF}^{(1.5,1,1)}$"), color=(0, 0, 0),
                isSignal=True, parent_process="vbf"),
            Process("vbf_1_0_1", Label("$HH_{VBF}^{(1,0,1)}$"), color=(0, 0, 0),
                isSignal=True, parent_process="vbf"),
            Process("vbf_1_1_0", Label("$HH_{VBF}^{(1,1,0)}$"), color=(0, 0, 0),
                isSignal=True, parent_process="vbf"),
            Process("vbf_1_1_2", Label("$HH_{VBF}^{(1,1,2)}$"),
                color=(0, 0, 0), isSignal=True, parent_process="vbf"),
            Process("vbf_1_2_1", Label("$HH_{VBF}^{(1,2,1)}$"),
                color=(0, 0, 0), isSignal=True, parent_process="vbf"),

            Process("dy", Label("DY"), color=(255, 102, 102), isDY=True, llr_name="DY"),
            Process("dy_high", Label("DY"), color=(255, 102, 102), isDY=True, parent_process="dy"),

            Process("tt", Label("t#bar{t}"), color=(255, 153, 0), llr_name="TT"),
            Process("tt_dl", Label("t#bar{t} DL"), color=(205, 0, 9), parent_process="tt"),
            Process("tt_sl", Label("t#bar{t} SL"), color=(255, 153, 0), parent_process="tt"),
            Process("tt_fh", Label("t#bar{t} FH"), color=(131, 38, 10), parent_process="tt"),

            Process("tth", Label("t#bar{t}H"), color=(255, 153, 0), llr_name="TTH"),
            Process("tth_bb", Label("t#bar{t}H"), color=(255, 153, 0), parent_process="tth"),
            Process("tth_tautau", Label("t#bar{t}H"), color=(255, 153, 0), parent_process="tth"),
            Process("tth_nonbb", Label("t#bar{t}H"), color=(255, 153, 0), parent_process="tth"),

            Process("others", Label("Others"), color=(134, 136, 138)),
            Process("wjets", Label("W + jets"), color=(134, 136, 138), parent_process="others",
                llr_name="WJets"),
            Process("tw", Label("t + W"), color=(134, 136, 138), parent_process="others",
                llr_name="TW"),
            Process("singlet", Label("Single t"), color=(134, 136, 138), parent_process="others",
                llr_name="singleT"),

            Process("data", Label("DATA"), color=(0, 0, 0), isData=True),
            Process("data_tau", Label("DATA\_TAU"), color=(0, 0, 0), parent_process="data", isData=True),
            Process("data_etau", Label("DATA\_E"), color=(0, 0, 0), parent_process="data", isData=True),
            Process("data_mutau", Label("DATA\_MU"), color=(0, 0, 0), parent_process="data", isData=True)
        ]

        process_group_names = {
            "default": [
                "ggf_sm",
                "data_tau",
                "dy_high",
                "tt_dl",
            ],
            "data_tau": [
                "data_tau",
            ],
            "data_etau": [
                "data_etau",
            ],
            "bkg": [
                "tt_dl",
            ],
            "signal": [
                "ggf_sm",
            ],
            "etau": [
                "tt_dl",
                "tt_sl",
                "dy",
                "wjets",
                "data_etau",
            ],
            "mutau": [
                "tt_dl",
                "tt_sl",
                "dy",
                "wjets",
                "data_mutau",
            ],
            "tautau": [
                "tt_dl",
                "tt_sl",
                "dy",
                "wjets",
                "data_tau",
            ],
            "vbf": [
                "vbf_sm",
                "vbf_0p5_1_1",
                "vbf_1p5_1_1",
                "vbf_1_0_1",
                "vbf_1_1_0",
                "vbf_1_1_2",
                "vbf_1_2_1"
            ]
        }

        process_training_names = {
            "default": [
                "ggf_sm",
                "dy"
            ]
        }

        return ObjectCollection(processes), process_group_names, process_training_names

    def add_datasets(self):
        datasets = [
            Dataset("ggf_sm",
                dataset="/GluGluToHHTo2B2Tau_node_cHHH1_TuneCP5_PSWeights_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/"
                    "NANOAODSIM",
                process=self.processes.get("ggf_sm"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.03105),

            Dataset("vbf_sm",
                dataset="/VBFHHTo2B2Tau_CV_1_C2V_1_C3_1_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_sm"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.001726),

            Dataset("vbf_0p5_1_1",
                dataset="/VBFHHTo2B2Tau_CV_0_5_C2V_1_C3_1_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_0p5_1_1"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.010824),

            Dataset("vbf_1p5_1_1",
                dataset="/VBFHHTo2B2Tau_CV_1_5_C2V_1_C3_1_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_1p5_1_1"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.066018),

            Dataset("vbf_1_0_1",
                dataset="/VBFHHTo2B2Tau_CV_1_C2V_0_C3_1_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_1_0_1"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.027080),

            Dataset("vbf_1_1_0",
                dataset="/VBFHHTo2B2Tau_CV_1_C2V_1_C3_0_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_1_1_0"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.004609),

            Dataset("vbf_1_1_2",
                dataset="/VBFHHTo2B2Tau_CV_1_C2V_1_C3_2_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_1_1_2"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.001423),

            Dataset("vbf_1_2_1",
                dataset="/VBFHHTo2B2Tau_CV_1_C2V_2_C3_1_dipoleRecoilOff"
                "-TuneCP5_PSweights_13TeV-madgraph-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("vbf_1_2_1"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.014218),

            # Background samples
            Dataset("dy_high",
                dataset="/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("dy_high"),
                # prefix="xrootd-cms.infn.it//",
                # prefix="cms-xrd-global.cern.ch//",
                xs=6077.22, 
                merging={
                    "tautau": 20,
                    "etau": 20,
                }),
            Dataset("tt_dl",
                dataset="/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/"
                    "NANOAODSIM",
                process=self.processes.get("tt_dl"),
                # prefix="xrootd-cms.infn.it//",
                xs=88.29, 
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                scaling=(0.96639, 0.00863),),
            Dataset("tt_sl",
                dataset="/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21_ext3-v1/"
                "NANOAODSIM",
                process=self.processes.get("tt_sl"),
                # prefix="xrootd-cms.infn.it//",
                xs=365.34,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                scaling=(0.96639, 0.00863),),
            Dataset("tt_fh",
                dataset="/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21_ext2-v1/"
                    "NANOAODSIM",
                process=self.processes.get("tt_fh"),
                # prefix="xrootd-cms.infn.it//",
                xs=377.96,
                scaling=(0.96639, 0.00863),
            ),

            # Others
            # ttH
            Dataset("tth_bb",
                dataset="/ttHTobb_M125_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_EXT_102X_upgrade2018_realistic_v21-v1/"
                    "NANOAODSIM",
                process=self.processes.get("tth_bb"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.2953),
            Dataset("tth_tautau",
                dataset="/ttHToTauTau_M125_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/"
                    "NANOAODSIM",
                process=self.processes.get("tth_tautau"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.031805),
            Dataset("tth_nonbb",
                dataset="/ttHToNonbb_M125_TuneCP5_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/"
                    "NANOAODSIM",
                process=self.processes.get("tth_nonbb"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.17996),

            # Wjets
            Dataset("wjets",
                dataset="/WJetsToLNu_TuneCP5_13TeV-madgraphMLM-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("wjets"),
                # prefix="xrootd-cms.infn.it//",
                xs=61526.7,
                merging={
                    "tautau": 5,
                    "etau": 10,
                }),
            # tW
            Dataset("st_tw_antitop",
                dataset="/ST_tW_antitop_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21_ext1-v1/"
                "NANOAODSIM",
                process=self.processes.get("tw"),
                # prefix="xrootd-cms.infn.it//",
                xs=35.85),
            Dataset("st_tw_top",
                dataset="/ST_tW_top_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21_ext1-v1/"
                "NANOAODSIM",
                process=self.processes.get("tw"),
                # prefix="xrootd-cms.infn.it//",
                xs=35.85),
            # single top
            Dataset("st_antitop",
                dataset="/ST_t-channel_antitop_5f_TuneCP5_13TeV-powheg-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("singlet"),
                # prefix="xrootd-cms.infn.it//",
                xs=80.95),
            Dataset("st_top",
                dataset="/ST_t-channel_top_5f_TuneCP5_13TeV-powheg-pythia8/"
                "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/NANOAODSIM",
                process=self.processes.get("singlet"),
                # prefix="xrootd-cms.infn.it//",
                xs=136.02),

            # DATA
            # Tau 2018
            Dataset("data_tau_a",
                dataset="/Tau/Run2018A-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_tau"),
                runPeriod="A",
                # prefix="xrootd-cms.infn.it//",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_b",
                dataset="/Tau/Run2018B-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_tau"),
                runPeriod="B",
                # prefix="xrootd-cms.infn.it//",
                # locate="se-xrd01.jinr-t1.ru:1095/",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_c",
                dataset="/Tau/Run2018C-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_tau"),
                runPeriod="C",
                # prefix="xrootd-cms.infn.it//",
                # locate="cms03.lcg.cscs.ch:1094/",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_d",
                dataset="/Tau/Run2018D-02Apr2020-v2/NANOAOD",
                process=self.processes.get("data_tau"),
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),

            # EGamma 2018
            Dataset("data_etau_a",
                dataset="/EGamma/Run2018A-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_etau"),
                runPeriod="A",
                # prefix="xrootd-cms.infn.it//",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_etau_b",
                dataset="/EGamma/Run2018B-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_etau"),
                runPeriod="B",
                # prefix="xrootd-cms.infn.it//",
                # locate="se-xrd01.jinr-t1.ru:1095/",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_etau_c",
                dataset="/EGamma/Run2018C-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_etau"),
                runPeriod="C",
                # prefix="xrootd-cms.infn.it//",
                # locate="cms03.lcg.cscs.ch:1094/",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_etau_d",
                dataset="/EGamma/Run2018D-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_etau"),
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),

        ]
        return ObjectCollection(datasets)

    def add_features(self):
        features = [
            # bjet features
            Feature("bjet1_pt", "Jet_pt.at(bjet1_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_{1} p_{t}"),
                units="GeV",
                central="jet_smearing"),
            Feature("bjet1_eta", "Jet_eta.at(bjet1_JetIdx)", binning=(20, -5., 5.),
                x_title=Label("b_{1} #eta")),
            Feature("bjet1_phi", "Jet_phi.at(bjet1_JetIdx)", binning=(20, -3.2, 3.2),
                x_title=Label("b_{1} #phi")),
            Feature("bjet1_mass", "Jet_mass.at(bjet1_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_{1} m"),
                units="GeV",
                central="jet_smearing"),
            Feature("bjet2_pt", "Jet_pt.at(bjet2_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_2 p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("bjet2_eta", "Jet_eta.at(bjet2_JetIdx)", binning=(20, -5., 5.),
                x_title=Label("b_{2} #eta")),
            Feature("bjet2_phi", "Jet_phi.at(bjet2_JetIdx)", binning=(20, -3.2, 3.2),
                x_title=Label("b_{2} #phi")),
            Feature("bjet2_mass", "Jet_mass.at(bjet2_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_{2} m"),
                units="GeV",
                central="jet_smearing"),

            Feature("bjet_difpt", "abs([bjet1_pt] - [bjet2_pt])", binning=(10, 50, 150),
                x_title=Label("bb #Delta p_t"),
                units="GeV",
                central="jet_smearing"),

            # lepton features
            Feature("lep1_pt", "dau1_pt", binning=(10, 50, 150),
                x_title=Label("#tau_{1} p_{t}")),
            Feature("lep1_eta", "dau1_eta", binning=(20, -5., 5.),
                x_title=Label("#tau_{1} #eta")),
            Feature("lep1_phi", "dau1_phi", binning=(20, -3.2, 3.2),
                x_title=Label("#tau_{1} #phi")),
            Feature("lep1_mass", "dau1_mass", binning=(10, 50, 150),
                x_title=Label("#tau_{1} m")),
            Feature("lep2_pt", "dau2_pt", binning=(10, 50, 150),
                x_title=Label("#tau_{2} p_{t}")),
            Feature("lep2_eta", "dau2_eta", binning=(20, -5., 5.),
                x_title=Label("#tau_{2} #eta")),
            Feature("lep2_phi", "dau2_phi", binning=(20, -3.2, 3.2),
                x_title=Label("#tau_{2} #phi")),
            Feature("lep2_mass", "dau2_mass", binning=(10, 50, 150),
                x_title=Label("#tau_{2} m")),

            # MET
            Feature("met_pt", "MET_smeared_pt", binning=(10, 50, 150),
                x_title=Label("MET p_t"),
                units="GeV"),
            Feature("met_phi", "MET_smeared_phi", binning=(20, -3.2, 3.2),
                x_title=Label("MET #phi")),

            # Hbb
            Feature("Hbb_pt", "Hbb_pt", binning=(10, 50, 150),
                x_title=Label("H(b #bar{b}) p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("Hbb_eta", "Hbb_eta", binning=(20, -5., 5.),
                x_title=Label("H(b #bar{b}) #eta"),
                central="jet_smearing"),
            Feature("Hbb_phi", "Hbb_phi", binning=(20, -3.2, 3.2),
                x_title=Label("H(b #bar{b}) #phi"),
                central="jet_smearing"),
            Feature("Hbb_mass", "Hbb_mass", binning=(30, 0, 300),
                x_title=Label("H(b #bar{b}) m"),
                units="GeV",
                central="jet_smearing"),

            # Htt
            Feature("Htt_pt", "Htt_pt", binning=(10, 50, 150),
                x_title=Label("H(#tau^{+} #tau^{-}) p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("Htt_eta", "Htt_eta", binning=(20, -5., 5.),
                x_title=Label("H(#tau^{+} #tau^{-}) #eta"),
                central="jet_smearing"),
            Feature("Htt_phi", "Htt_phi", binning=(20, -3.2, 3.2),
                x_title=Label("H(#tau^{+} #tau^{-}) #phi"),
                central="jet_smearing"),
            Feature("Htt_mass", "Htt_mass", binning=(30, 0, 300),
                x_title=Label("H(#tau^{+} #tau^{-}) m"),
                units="GeV",
                central="jet_smearing"),

            # Htt (SVFit)
            Feature("Htt_svfit_pt", "Htt_svfit_pt", binning=(10, 50, 150),
                x_title=Label("H(#tau^{+} #tau^{-}) p_t (SVFit)"),
                units="GeV",
                central="jet_smearing"),
            Feature("Htt_svfit_eta", "Htt_svfit_eta", binning=(20, -5., 5.),
                x_title=Label("H(#tau^{+} #tau^{-}) #eta (SVFit)"),
                central="jet_smearing"),
            Feature("Htt_svfit_phi", "Htt_svfit_phi", binning=(20, -3.2, 3.2),
                x_title=Label("H(#tau^{+} #tau^{-}) #phi (SVFit)"),
                central="jet_smearing"),
            Feature("Htt_svfit_mass", "Htt_svfit_mass", binning=(30, 0, 300),
                x_title=Label("H(#tau^{+} #tau^{-}) m (SVFit)"),
                units="GeV",
                central="jet_smearing"),

            # HH
            Feature("HH_pt", "HH_pt", binning=(10, 50, 150),
                x_title=Label("HH p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("HH_eta", "HH_eta", binning=(20, -5., 5.),
                x_title=Label("HH #eta"),
                central="jet_smearing"),
            Feature("HH_phi", "HH_phi", binning=(20, -3.2, 3.2),
                x_title=Label("HH #phi"),
                central="jet_smearing"),
            Feature("HH_mass", "HH_mass", binning=(50, 0, 1000),
                x_title=Label("HH m"),
                units="GeV",
                central="jet_smearing"),

            # HH (SVFit)
            Feature("HH_svfit_pt", "HH_svfit_pt", binning=(10, 50, 150),
                x_title=Label("HH p_t (SVFit)"),
                units="GeV",
                central="jet_smearing"),
            Feature("HH_svfit_eta", "HH_svfit_eta", binning=(20, -5., 5.),
                x_title=Label("HH #eta (SVFit)"),
                central="jet_smearing"),
            Feature("HH_svfit_phi", "HH_svfit_phi", binning=(20, -3.2, 3.2),
                x_title=Label("HH #phi (SVFit)"),
                central="jet_smearing"),
            Feature("HH_svfit_mass", "HH_svfit_mass", binning=(50, 0, 1000),
                x_title=Label("HH m (SVFit)"),
                units="GeV",
                central="jet_smearing"),

            # HH KinFit
            Feature("HHKinFit_mass", "HHKinFit_mass", binning=(50, 0, 1000),
                x_title=Label("HH m (Kin. Fit)"),
                units="GeV",
                central="jet_smearing"),
            Feature("HHKinFit_chi2", "HHKinFit_chi2", binning=(30, 0, 10),
                x_title=Label("HH #chi^2 (Kin. Fit)"),
                central="jet_smearing"),

            # VBFjet features
            Feature("vbfjet1_pt", "Jet_pt.at(VBFjet1_JetIdx)", binning=(10, 50, 150),
                x_title=Label("VBFjet1 p_{t}"),
                units="GeV",
                central="jet_smearing"),
            Feature("vbfjet1_eta", "Jet_eta.at(VBFjet1_JetIdx)", binning=(20, -5., 5.),
                x_title=Label("VBFjet1 #eta")),
            Feature("vbfjet1_phi", "Jet_phi.at(VBFjet1_JetIdx)", binning=(20, -3.2, 3.2),
                x_title=Label("VBFjet1 #phi")),
            Feature("vbfjet1_mass", "Jet_mass.at(VBFjet1_JetIdx)", binning=(10, 50, 150),
                x_title=Label("VBFjet1 m"),
                units="GeV",
                central="jet_smearing"),
            Feature("vbfjet2_pt", "Jet_pt.at(VBFjet2_JetIdx)", binning=(10, 50, 150),
                x_title=Label("VBFjet2 p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("vbfjet2_eta", "Jet_eta.at(VBFjet2_JetIdx)", binning=(20, -5., 5.),
                x_title=Label("VBFjet2 #eta")),
            Feature("vbfjet2_phi", "Jet_phi.at(VBFjet2_JetIdx)", binning=(20, -3.2, 3.2),
                x_title=Label("VBFjet2 #phi")),
            Feature("vbfjet2_mass", "Jet_mass.at(VBFjet2_JetIdx)", binning=(10, 50, 150),
                x_title=Label("VBFjet2 m"),
                units="GeV",
                central="jet_smearing"),

            # VBFjj
            Feature("VBFjj_mass", "VBFjj_mass", binning=(40, 0, 1000),
                x_title=Label("VBFjj mass"),
                units="GeV",
                central="jet_smearing"),
            Feature("VBFjj_deltaEta", "VBFjj_deltaEta", binning=(40, -8, 8),
                x_title=Label("#Delta#eta(VBFjj)"),
                central="jet_smearing"),
            Feature("VBFjj_deltaPhi", "VBFjj_deltaPhi", binning=(40, -6.4, 6.4),
                x_title=Label("#Delta#phi(VBFjj)"),
                central="jet_smearing"),
        ]
        return ObjectCollection(features)

    def add_versions(self):
        versions = {}
        return versions

    def add_weights(self):
        weights = DotDict()
        weights.default = "1"
        # weights.total_events_weights = ["genWeight", "puWeight", "DYstitchWeight"]
        weights.total_events_weights = ["genWeight", "puWeight"]
        weights.channels = {
            "mutau": ["genWeight", "puWeight", "prescaleWeight", "trigSF",
                "L1PreFiringWeight_Nom", "PUjetID_SF"
            ],
            # "DYscale_MTT"],
        }
        weights.channels["etau"] = weights.channels["mutau"]
        weights.channels["tautau"] = weights.channels["mutau"]
        
        weights.channels_mult = {channel: jrs(weights.channels[channel], op="*")
            for channel in weights.channels}
        return weights

    def add_systematics(self):
        systematics = [
            Systematic("jet_smearing", "_nom"),
            Systematic("empty", "", up="", down="")
        ]
        return ObjectCollection(systematics)

    # feature methods

    def get_central_value(self, feature):
        """
        Return the expression from the central value of a feature
        """
        if feature.central == "":
            return self.central
        return self.systematics.get(feature.central).expression

    def get_object_expression(self, feature, isMC=False,
            syst_name="central", systematic_direction=""):
        """
        Returns a feature or category's expression including the systematic considered
        """

        def get_expression(obj):
            if isinstance(obj, Feature):
                return obj.expression
            elif isinstance(obj, Category):
                return obj.selection
            else:
                raise ValueError("Object %s cannot be used in method get_feature_expression" % obj)

        def add_systematic_tag(feat_expression, tag):
            """
            Includes systematic tag in the feature expression.
                - Directly if it does not come from a vector
                - Before ".at" if it comes from a vector
            """
            if ".at" in feat_expression:
                index = feat_expression.find(".at")
                return feat_expression[:index] + tag + feat_expression[index:]
            else:
                return feat_expression + tag

        feature_expression = get_expression(feature)
        if "[" in feature_expression:  # derived expression
            while "[" in feature_expression:
                initial = feature_expression.find("[")
                final = feature_expression.find("]")
                feature_name_to_look = feature_expression[initial + 1: final]
                feature_to_look = self.features.get(feature_name_to_look)

                if not isMC:
                    tag = ""
                elif syst_name == "central":
                    if feature_to_look.central != "":
                        tag = ""
                    else:
                        tag = "%s" % self.systematics.get(feature_to_look.central).expression
                elif isMC and syst_name in feature_to_look.systematics:
                    syst = self.systematics.get(syst_name)
                    tag = "%s%s" % (syst.expression, eval("syst.%s" % systematic_direction))

                feature_to_look_expression = add_systematic_tag(feature_to_look.expression, tag)
                feature_expression = feature_expression.replace(feature_expression[initial: final + 1],
                    feature_to_look_expression)
            return feature_expression

        elif isinstance(feature, Feature):  # not derived expression and not a category
            if not isMC:
                tag = ""
            elif syst_name == "central":
                if feature.central != "":
                    tag = "%s" % self.systematics.get(feature.central).expression
                else:
                    tag = ""
            elif isMC and syst_name in feature.systematics:
                syst = self.systematics.get(syst_name)
                tag = "%s%s" % (syst.expression, eval("syst.%s" % systematic_direction))

            return add_systematic_tag(feature.expression, tag)
        else:
            return get_expression(feature)

    # other methods

    def get_channel_from_region(self, region):
        for sign in ["os", "ss"]:
            if sign in region.name:
                if region.name.startswith(sign):
                    return ""
                return region.name[:region.name.index("_%s" % sign)]
        return ""

    def get_qcd_regions(self, region, category, wp="", shape_region="os_inviso",
            signal_region_wp="os_iso", sym=False):
        # the region must be set and tagged os_iso
        if not region:
            raise Exception("region must not be empty")
        # if not region.has_tag("qcd_os_iso"):
        #     raise Exception("region must be tagged as 'qcd_os_iso' but isn't")

        # the category must be compatible with the estimation technique
        # if category.has_tag("qcd_incompatible"):
        #     raise Exception("category '{}' incompatible with QCD estimation".format(category.name))

        if wp != "":
            wp = "__" + wp

        # get other qcd regions
        prefix = region.name[:-len(signal_region_wp)]
        qcd_regions = {"ss_inviso": self.regions.get(prefix + "ss_inviso" + wp)}
        # for the inverted regions, allow different working points
        default_config = ["os_inviso", "ss_iso"]
        for key in default_config:
            region_name = (prefix + key + wp if key != "ss_iso"
                else prefix + "ss_" + signal_region_wp[len("os_"):])
            qcd_regions[key] = self.regions.get(region_name)

        if sym:
            qcd_regions["shape1"] = self.regions.get(prefix + shape_region + wp)
            qcd_regions["shape2"] = self.regions.get(
                prefix + "ss_" + signal_region_wp[len("os_"):])
        else:
            if shape_region == "os_inviso":
                qcd_regions["shape"] = self.regions.get(prefix + shape_region + wp)
            else:
                qcd_regions["shape"] = self.regions.get(
                    prefix + "ss_" + signal_region_wp[len("os_"):])
        return DotDict(qcd_regions)


config = Config("base", year=2018, ecm=13, lumi_pb=59741)
