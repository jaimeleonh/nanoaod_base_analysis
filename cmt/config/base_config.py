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
        self.processes, self.process_group_names = self.add_processes()
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
            Category("dum", "dummy category", selection="Htt_svfit_mass_nom > 50 "
                " && Htt_svfit_mass_nom < 150"),
            Category("mutau", "#mu#tau channel", selection="pairType == 0"),
            Category("etau", "e#tau channel", selection="pairType == 1"),
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
            Process("ggf_sm", Label("$HH_{ggF}$"), color=(0, 0, 0), isSignal=True),
            Process("dy", Label("DY"), color=(255, 102, 102)),
            Process("tt", Label("t#bar{t}"), color=(255, 153, 0)),
            Process("tt_dl", Label("t#bar{t} DL"), color=(205, 0, 9), parent_process="tt"),
            Process("tt_sl", Label("t#bar{t} SL"), color=(255, 153, 0), parent_process="tt"),
            Process("tt_fh", Label("t#bar{t} FH"), color=(131, 38, 10), parent_process="tt"),
            Process("data", Label("DATA"), color=(0, 0, 0), isData=True),
            Process("data_tau", Label("DATA\_TAU"), color=(0, 0, 0), parent_process="data", isData=True),
            Process("data_etau", Label("DATA\_E"), color=(0, 0, 0), parent_process="data", isData=True),
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
            ],
            "signal": [
                "ggf_sm",
            ],
            "etau": [
                "tt_dl",
                "data_etau",
            ]
        }
        return ObjectCollection(processes), process_group_names

    def add_datasets(self):
        datasets = [
            Dataset("ggf_sm",
                # "/store/mc/RunIIAutumn18NanoAODv7/"
                # "GluGluToHHTo2B2Tau_node_cHHH1_TuneCP5_PSWeights_13TeV-powheg-pythia8"
                # "/NANOAODSIM/Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/",
                dataset="/GluGluToHHTo2B2Tau_node_cHHH1_TuneCP5_PSWeights_13TeV-powheg-pythia8/"
                    "RunIIAutumn18NanoAODv7-Nano02Apr2020_102X_upgrade2018_realistic_v21-v1/"
                    "NANOAODSIM",
                process=self.processes.get("ggf_sm"),
                # prefix="xrootd-cms.infn.it//",
                xs=0.001726,
                splitting=400000,),

            # Background samples
            Dataset("dy_high",
                dataset="/DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/"
                    "RunIIAutumn18NanoAOD-102X_upgrade2018_realistic_v15-v1/NANOAODSIM",
                process=self.processes.get("dy"),
                # prefix="xrootd-cms.infn.it//",
                # prefix="cms-xrd-global.cern.ch//",
                xs=6077.22, 
                merging={
                    "tautau": 20,
                    "etau": 40,
                },
                splitting=100000),
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
                splitting=100000),
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
                splitting=100000),
            
            # Tau 2018
            Dataset("data_tau_a",
                dataset="/Tau/Run2018A-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_tau"),
                runPeriod="A",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
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
                splitting=-1,
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
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_tau_d",
                dataset="/Tau/Run2018D-02Apr2020-v2/NANOAOD",
                process=self.processes.get("data_tau"),
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
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
                splitting=-1,
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
                splitting=-1,
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
                splitting=-1,
                merging={
                    "tautau": 20,
                    "etau": 40,
                },),
            Dataset("data_etau_d",
                dataset="/EGamma/Run2018D-02Apr2020-v1/NANOAOD",
                process=self.processes.get("data_etau"),
                runPeriod="D",
                # prefix="xrootd-cms.infn.it//",
                splitting=-1,
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
                central="jet_smearing"),
            Feature("Hbb_mass", "Hbb_mass", binning=(10, 50, 150),
                x_title=Label("H(b #bar{b})) mass"),
                units="GeV",
                central="jet_smearing"),
            Feature("bjet1_pt", "Jet_pt.at(bjet1_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_1 p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("bjet2_pt", "Jet_pt.at(bjet2_JetIdx)", binning=(10, 50, 150),
                x_title=Label("b_2 p_t"),
                units="GeV",
                central="jet_smearing"),
            Feature("bjet_difpt", "abs([bjet1_pt] - [bjet2_pt])", binning=(10, 50, 150),
                x_title=Label("bb #Delta p_t"),
                units="GeV",),
                # central="jet_smearing"),
            Feature("lep1_pt", "dau1_pt", binning=(40, 0, 400),
                x_title=Label("lep_1 p_t"),
                units="GeV",
                central=""),
            Feature("VBFjj_mass", "VBFjj_mass", binning=(40, 0, 1000),
                x_title=Label("VBFjj mass"),
                units="GeV",
                central="jet_smearing"),
            Feature("VBFjj_deltaEta", "VBFjj_deltaEta", binning=(40, 0, 1000),
                x_title=Label("#Delta#eta(VBFjj)"),
                central="jet_smearing"),
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
            "tautau": ["genWeight", "puWeight", "PrefireWeight", "trigSF",
                "Tau_sfDeepTau2017v2p1VSjet_Medium.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSjet_Medium.at(dau2_index)",
                "Tau_sfDeepTau2017v2p1VSe_VVLoose.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSe_VVLoose.at(dau2_index)",
                "Tau_sfDeepTau2017v2p1VSmu_VLoose.at(dau1_index)",
                "Tau_sfDeepTau2017v2p1VSmu_VLoose.at(dau2_index)"]
        }
        weights.channels_mult = {channel: jrs(weights.channels[channel], op="*")
            for channel in weights.channels}
        return weights

    def add_systematics(self):
        systematics = [
            Systematic("jet_smearing", "nom")
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
            systematic=None, systematic_direction=None):
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

                if isMC and systematic in feature_to_look.systematics:
                    syst = self.systematics.get(systematic)
                    tag = "_%s%s" % (syst.expression, syst.get(systematic_direction))
                else:
                    tag = ""

                if self.get_central_value(feature_to_look) != "" and isMC:
                    tag = "_%s%s" % (self.get_central_value(feature_to_look), tag)
                else:
                    tag = ""

                feature_to_look_expression = add_systematic_tag(feature_to_look.expression, tag)
                feature_expression = feature_expression.replace(feature_expression[initial: final + 1],
                    feature_to_look_expression)
            return feature_expression
        elif isinstance(feature, Feature):  # not derived expression and not a category
            if not systematic or systematic not in feature.systematics:
                tag = ""
            elif systematic in feature.systematics:
                syst = self.systematics.get(systematic)
                tag = "_%s%s" % (syst.expression, syst.get(systematic_direction))
            
            if self.get_central_value(feature) != "" and isMC:
                tag = ("_%s%s" % (self.get_central_value(feature), tag)
                if tag else "_%s" % (self.get_central_value(feature)))
            else:
                tag = ""
            return add_systematic_tag(feature.expression, tag)
        else:  # for now, o
            return get_expression(feature)


config = Config("base", year=2018, ecm=13, lumi_pb=59741)
