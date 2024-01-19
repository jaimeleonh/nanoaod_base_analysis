# coding: utf-8

"""
SGE workflow implementation. See https://research.cs.wisc.edu/sge.
"""

__all__ = ["SGEWorkflow"]


import os
from abc import abstractmethod
from collections import OrderedDict

import luigi
import six

from law.workflow.remote import BaseRemoteWorkflow, BaseRemoteWorkflowProxy
from law.job.base import JobArguments, JobInputFile, DeprecatedInputFiles
from law.task.proxy import ProxyCommand
from law.target.file import get_path, get_scheme
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR
from law.util import law_src_path, merge_dicts, DotDict
from law.logger import get_logger

from cmt.sge.job import SGEJobManager, SGEJobFileFactory


logger = get_logger(__name__)


class SGEWorkflowProxy(BaseRemoteWorkflowProxy):

    workflow_type = "sge"

    def create_job_manager(self, **kwargs):
        return self.task.sge_create_job_manager(**kwargs)

    def create_job_file_factory(self, **kwargs):
        return self.task.sge_create_job_file_factory(**kwargs)

    def create_job_file(self, job_num, branches):
        task = self.task

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)

        # create the config
        c = self.job_file_factory.get_config()
        c.input_files = DeprecatedInputFiles()
        c.output_files = []
        c.render_variables = {}
        c.custom_content = []

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = task.sge_wrapper_file()
        law_job_file = task.sge_job_file()
        if wrapper_file and get_path(wrapper_file) != get_path(law_job_file):
            c.input_files["executable_file"] = wrapper_file
            c.executable = wrapper_file
        else:
            c.executable = law_job_file
        c.input_files["job_file"] = law_job_file

        # collect task parameters
        exclude_args = (
            task.exclude_params_branch |
            task.exclude_params_workflow |
            task.exclude_params_remote_workflow |
            task.exclude_params_sge_workflow |
            {"workflow"}
        )
        proxy_cmd = ProxyCommand(
            task.as_branch(branches[0]),
            exclude_task_args=exclude_args,
            exclude_global_args=["workers", "local-scheduler"],
        )
        if task.sge_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.sge_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        job_args = JobArguments(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            workers=task.job_workers,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.job_data.attempts.get(job_num, 0)),
        )
        c.arguments = job_args.join()

        # add the bootstrap file
        bootstrap_file = task.sge_bootstrap_file()
        if bootstrap_file:
            c.input_files["bootstrap_file"] = bootstrap_file

        # add the stageout file
        stageout_file = task.sge_stageout_file()
        if stageout_file:
            c.input_files["stageout_file"] = stageout_file

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            c.input_files["dashboard_file"] = dashboard_file

        # logging
        # we do not use sge's logging mechanism since it might require that the submission
        # directory is present when it retrieves logs, and therefore we use a custom log file
        c.log = None
        c.stdout = None
        c.stderr = None
        if task.transfer_logs:
            c.custom_log_file = "stdall.txt"

        # when the output dir is local, we can run within this directory for easier output file
        # handling and use absolute paths for input files
        output_dir = task.sge_output_directory()
        if isinstance(output_dir, six.string_types) and get_scheme(output_dir) in (None, "file"):
            output_dir = LocalDirectoryTarget(output_dir)
        output_dir_is_local = isinstance(output_dir, LocalDirectoryTarget)
        if output_dir_is_local:
            c.absolute_paths = True
            c.custom_content.append(("initialdir", output_dir.abspath))

        # task hook
        c = task.sge_job_config(c, job_num, branches)

        # when the output dir is not local, direct output files are not possible
        if not output_dir_is_local:
            del c.output_files[:]

        # build the job file and get the sanitized config
        job_file, c = self.job_file_factory(postfix=postfix, **c.__dict__)

        # get the location of the custom local log file if any
        abs_log_file = None
        if output_dir_is_local and c.custom_log_file:
            abs_log_file = os.path.join(output_dir.abspath, c.custom_log_file)

        # return job and log files
        return {"job": job_file, "config": c, "log": abs_log_file}

    def destination_info(self):
        info = super(SGEWorkflowProxy, self).destination_info()

        if self.task.sge_pool and self.task.sge_pool != NO_STR:
            info["pool"] = "pool: {}".format(self.task.sge_pool)

        if self.task.sge_scheduler and self.task.sge_scheduler != NO_STR:
            info["scheduler"] = "scheduler: {}".format(self.task.sge_scheduler)

        info = self.task.sge_destination_info(info)

        return info


class SGEWorkflow(BaseRemoteWorkflow):

    workflow_proxy_cls = SGEWorkflowProxy

    sge_workflow_run_decorators = None
    sge_job_manager_defaults = None
    sge_job_file_factory_defaults = None

    sge_pool = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target sge pool; default: empty",
    )
    sge_scheduler = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="target sge scheduler; default: empty",
    )

    sge_job_kwargs = ["sge_pool", "sge_scheduler"]
    sge_job_kwargs_submit = None
    sge_job_kwargs_cancel = None
    sge_job_kwargs_query = None

    exclude_params_branch = {"sge_pool", "sge_scheduler"}

    exclude_params_sge_workflow = set()

    exclude_index = True

    @abstractmethod
    def sge_output_directory(self):
        return None

    def sge_workflow_requires(self):
        return DotDict()

    def sge_bootstrap_file(self):
        return None

    def sge_wrapper_file(self):
        return None

    def sge_job_file(self):
        return JobInputFile(law_src_path("job", "law_job.sh"))

    def sge_stageout_file(self):
        return None

    def sge_output_postfix(self):
        return ""

    def sge_job_manager_cls(self):
        return SGEJobManager

    def sge_create_job_manager(self, **kwargs):
        kwargs = merge_dicts(self.sge_job_manager_defaults, kwargs)
        return self.sge_job_manager_cls()(**kwargs)

    def sge_job_file_factory_cls(self):
        return SGEJobFileFactory

    def sge_create_job_file_factory(self, **kwargs):
        # job file fectory config priority: kwargs > class defaults
        kwargs = merge_dicts({}, self.sge_job_file_factory_defaults, kwargs)
        return self.sge_job_file_factory_cls()(**kwargs)

    def sge_job_config(self, config, job_num, branches):
        return config

    def sge_check_job_completeness(self):
        return False

    def sge_check_job_completeness_delay(self):
        return 0.0

    def sge_use_local_scheduler(self):
        return False

    def sge_cmdline_args(self):
        return {}

    def sge_destination_info(self, info):
        return info
