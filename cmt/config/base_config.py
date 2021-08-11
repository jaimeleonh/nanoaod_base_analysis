from analysis_tools import ObjectCollection, Category, Process, Dataset

class Config():
    def __init__(self, name, year, ecm, lumi, **kwargs):
        self.name=name
        self.year=year
        self.ecm=ecm
        self.lumi=lumi
        self.x = kwargs

        self.categories = self.add_categories()
        self.processes = self.add_processes()
        self.datasets = self.add_datasets()
        self.versions = self.add_versions()

    def add_categories(self):
        categories = [
            Category("base", "base category"),
            Category("dum", "dummy category", selection="Htt_svfit_mass_nom > 50 "
                " && Htt_svfit_mass_nom < 150")
        ]
        return ObjectCollection(categories)

    def add_processes(self):
        processes = [
            Process("ggf_sm", "GGFSM", color=(0, 0, 0)),
            Process("data_tau", "DATA_TAU", color=(255, 255, 255))
        ]
        return ObjectCollection(processes)

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
                isData=True,
                runPeriod="A",
                prefix="grid-dcache.physik.rwth-aachen.de:1094/")
        ]
        return ObjectCollection(datasets)

    def add_versions(self):
        versions = {}
        return versions

config = Config("base", year=2018, ecm=13, lumi=59741)