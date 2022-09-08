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

        self.categories = self.add_categories()
        self.processes, self.process_group_names, self.process_training_names = self.add_processes()
        self.datasets = self.add_datasets()
        self.features = self.add_features()
        self.versions = self.add_versions()
        self.weights = self.add_weights()
        self.systematics = self.add_systematics()

    def add_categories(self):
        categories = []
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
        ]

        process_group_names = {
            "default": [],
        }

        process_training_names = {
            "default": [
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
        features = []
        return ObjectCollection(features)

    def add_versions(self):
        versions = {}
        return versions

    def add_weights(self):
        weights = DotDict()
        weights.default = "1"
        return weights

    def add_systematics(self):
        systematics = []
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

