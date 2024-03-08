from analysis_tools import ObjectCollection, Category, Process, Dataset, Feature, Systematic
from analysis_tools.utils import DotDict
from analysis_tools.utils import join_root_selection as jrs
from plotting_tools import Label
from collections import OrderedDict

class Config():
    def __init__(self, name, year, ecm, runPeriod="", lumi_fb=None, lumi_pb=None, **kwargs):
        self.name=name
        self.year=year
        self.ecm=ecm
        self.runPeriod=runPeriod
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
        if 'xrd_redir' in kwargs:
            self.prefix_datasets(self.datasets, kwargs['xrd_redir'])
        self.features = self.add_features()
        self.versions = self.add_versions()
        self.weights = self.add_weights()
        self.systematics = self.add_systematics()
        self.default_module_files = self.add_default_module_files()

    def get_aux(self, name, default=None):
        return self.x.get(name, default)

    def add_categories(self):
        categories = []
        return ObjectCollection(categories)

    def add_processes(self):
        processes = []

        process_group_names = {
            "default": [],
        }

        process_training_names = {
            "default": DotDict(
                processes=[],
                process_group_ids=()
            )
        }

        return ObjectCollection(processes), process_group_names, process_training_names


    def prefix_datasets(self, datasets, prefix):

        for dataset in datasets:
            dataset.prefix = prefix + '//'


    def add_datasets(self):
        datasets = []
        # Dataset("example_ggf_sm",
        #         dataset="/GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00_TuneCP5_13p6TeV_powheg-pythia8/"
        #                 "Run3Summer22EENanoAODv12-Poisson60KeepRAW_130X_mcRun3_2022_realistic_postEE_v6-v2/NANOAODSIM",
        #         process=self.processes.get("example_ggf_sm"),
        #         # prefix="xrootd-cms.infn.it//",
        #         xs=0.03443,
        #         tags=["NanoAODv12"])

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

    def add_default_module_files(self):
        defaults = {}
        return defaults

        def add_tau_id(self, year, tauId_algo):
        self.tauId_algo = tauId_algo

        if year <= 2018 and self.tauId_algo == "idDeepTau2018v2p5":
            raise ValueError("Wrong tau id requested. "
                             "Only idDeepTau2017v2p1 available in Run2 MC.")

        elif year <= 2018 and self.tauId_algo == "idDeepTau2017v2p1":
            # DeepTau2017v2p1 wpbit is in power 2 in Run2 MC NanoAOD
            self.tauId_algo_wps=DotDict(
                vsjet=DotDict(VVVLoose = 1, VVLoose = 3, VLoose = 7, Loose = 15,
                              Medium = 31, Tight = 63, VTight = 127, VVTight = 255),
                vse=DotDict(VVVLoose = 1, VVLoose = 3, VLoose = 7, Loose = 15,
                            Medium = 31, Tight = 63, VTight = 127, VVTight = 255),
                vsmu=DotDict(VLoose = 1, Loose = 3, Medium = 7, Tight = 15) )

        elif year >= 2022 and self.tauId_algo == "idDeepTau2018v2p5":
            # DeepTau2017v2p1/DeepTau2018v2p5 wpbit os integer in latest NanoAOD
            self.tauId_algo_wps=DotDict(
                vsjet = DotDict(VVVLoose = 1, VVLoose = 2, VLoose = 3, Loose = 4, 
                                Medium = 5, Tight = 6, VTight = 7, VVTight = 8),
                vse   = DotDict(VVVLoose = 1, VVLoose = 2, VLoose = 3, Loose = 4, 
                                Medium = 5, Tight = 6, VTight = 7, VVTight = 8),
                vsmu  = DotDict(VLoose = 1, Loose = 2, Medium = 3, Tight = 4) )

        else:
            raise ValueError("Wrong tau id requested. "
                             "Only idDeepTau2017v2p1 or idDeepTau2018v2p5 available at the moment.")

        return self

    def add_bjet_id(self, btag_algo = "DeepFlavB"):
        self.btag_algo = btag_algo
        
        if self.btag_algo == "DeepFlavB":
            # https://twiki.cern.ch/twiki/bin/viewauth/CMS/BtagRecommendation102X
            self.btag_algo_wps = DotDict(tight = 0.7264, medium = 0.2770, loose = 0.0494)

        elif self.btag_algo == "PNetB":
            # ParticleNet WPs taken from BTV SF json
            self.btag_algo_wps = DotDict(xxtight = 0.9610, xtight = 0.7862, tight = 0.6734,
                                         medium = 0.2450, loose = 0.0470)

        else:
            raise ValueError("Wrong jet id requested. "
                             "Only DeepFlavB or PNetB available at the moment.")

        return self

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
            elif isinstance(obj, str):
                return obj
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
        if "{{" in feature_expression:  # derived expression
            while "{{" in feature_expression:
                initial = feature_expression.find("{{")
                final = feature_expression.find("}}")
                feature_name_to_look = feature_expression[initial + 2: final]
                feature_to_look = self.features.get(feature_name_to_look)
                feature_to_look_expression = feature_to_look.expression
                if not isMC:
                    tag = ""
                elif syst_name in feature_to_look.systematics:
                    syst = self.systematics.get(syst_name)
                    if type(syst.expression) == tuple:
                        feature_to_look_expression = feature_to_look_expression.replace(
                            syst.expression[0], syst.expression[1])
                        tag = ""
                    else:
                        tag = syst.expression
                    tag += eval("syst.%s" % systematic_direction)
                else:
                    if feature_to_look.central == "":
                        tag = ""
                    else:
                        central = self.systematics.get(feature_to_look.central)
                        if type(central.expression) == tuple:
                            feature_to_look_expression = feature_to_look_expression.replace(
                                central.expression[0], central.expression[1])
                            tag = ""
                        else:
                            tag = central.expression

                feature_to_look_expression = add_systematic_tag(feature_to_look_expression, tag)
                feature_expression = feature_expression.replace(feature_expression[initial: final + 2],
                    feature_to_look_expression)
            return feature_expression

        elif isinstance(feature, Feature):  # not derived expression and not a category
            if not isMC:
                return add_systematic_tag(feature.expression, "")
            feature_expression = feature.expression
            tag = ""
            if syst_name in feature.systematics:
                syst = self.systematics.get(syst_name)
                if type(syst.expression) == tuple:
                    feature_expression = feature_expression.replace(syst.expression[0],
                        syst.expression[1])
                    tag = ""
                else:
                    tag = syst.expression
                tag += eval("syst.%s" % systematic_direction)
            else:
                if feature.central != "":
                    central = self.systematics.get(feature.central)
                    if type(central.expression) == tuple:
                        feature_expression = feature_expression.replace(central.expression[0],
                            central.expression[1])
                        tag = ""
                    else:
                        tag = central.expression
            return add_systematic_tag(feature_expression, tag)
        else:
            return get_expression(feature)

    def get_systematics_from_expression(self, expression):
        systs = []
        while "{{" in expression:
            initial = expression.find("{{")
            final = expression.find("}}")
            feature_name_to_look = expression[initial + 2: final]
            feature_to_look = self.features.get(feature_name_to_look)
            feature_to_look_expression = feature_to_look.expression
            expression = expression.replace(expression[initial: final + 2], "")
            systs += (feature_to_look.systematics + self.get_systematics_from_expression(
                feature_to_look_expression))
        return systs

    def get_weights_systematics(self, list_of_weights, isMC=False):
        systematics = []
        config_systematics = self.systematics.names()
        if isMC:
            for weight in list_of_weights:
                try:
                    feature = self.features.get(weight)
                    for syst in feature.systematics:
                        if syst not in systematics and syst in config_systematics:
                            systematics.append(syst)
                except ValueError:
                    continue
        return systematics

    def get_norm_systematics(self, process_datasets, region):
        return []

    def get_weights_expression(self, list_of_weights, syst_name="central", systematic_direction=""):
        weights = []
        for weight in list_of_weights:
            try:
                feature = self.features.get(weight)
                weights.append(self.get_object_expression(
                    feature, True, syst_name, systematic_direction))
            except ValueError:
                weights.append(weight)
        return "*".join(weights)

    def is_process_from_dataset(self, process_name, dataset_name=None, dataset=None):
        assert dataset_name or dataset
        assert not (dataset_name and dataset)

        if not dataset:
            dataset = self.datasets.get(dataset_name)

        process = dataset.process
        while True:
            if process.name == process_name:
                return True
            elif process.parent_process:
                process = self.processes.get(process.parent_process)
            else:
                return False

    def get_children_from_process(self, original_process_name):
        processes = []
        for process in self.processes:
            child = process
            if process.name == original_process_name:
                continue
            while True:
                if process.parent_process == original_process_name:
                    processes.append(child)
                    break
                elif process.parent_process:
                    process=self.processes.get(process.parent_process)
                else:
                    break
        return processes
