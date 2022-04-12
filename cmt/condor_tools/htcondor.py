from cmt.condor_tools.job_base import JobArgumentsExt

from collections import OrderedDict

import os
import luigi
import law

from law.task.proxy import ProxyCommand
from law.target.file import get_path
from law.target.local import LocalDirectoryTarget
from law.parameter import NO_STR

# law.contrib.load("htcondor")


class HTCondorWorkflowProxyExt(law.contrib.htcondor.workflow.HTCondorWorkflowProxy):
    
    def create_job_file(self, job_num, branches):
        task = self.task
        config = self.job_file_factory.Config()

        # the file postfix is pythonic range made from branches, e.g. [0, 1, 2, 4] -> "_0To5"
        postfix = "_{}To{}".format(branches[0], branches[-1] + 1)
        config.postfix = postfix
        pf = lambda s: "__law_job_postfix__:{}".format(s)

        # get the actual wrapper file that will be executed by the remote job
        wrapper_file = get_path(task.htcondor_wrapper_file())
        config.executable = os.path.basename(wrapper_file)

        # collect task parameters
        proxy_cmd = ProxyCommand(task.as_branch(branches[0]), exclude_task_args={"branch"},
            exclude_global_args=["workers", "local-scheduler"])
        if task.htcondor_use_local_scheduler():
            proxy_cmd.add_arg("--local-scheduler", "True", overwrite=True)
        for key, value in OrderedDict(task.htcondor_cmdline_args()).items():
            proxy_cmd.add_arg(key, value, overwrite=True)

        # job script arguments
        job_args = JobArgumentsExt(
            task_cls=task.__class__,
            task_params=proxy_cmd.build(skip_run=True),
            branches=branches,
            auto_retry=False,
            dashboard_data=self.dashboard.remote_hook_data(
                job_num, self.submission_data.attempts.get(job_num, 0)),
            args=["$(Proxy_path)"],
        )
        config.arguments = job_args.join()

        # prepare render variables
        config.render_variables = {}

        # input files
        config.input_files = [wrapper_file, os.path.expandvars("$CMT_BASE/cmt/condor_tools/job.sh")]
        config.render_variables["job_file"] = pf("job.sh")

        # add the bootstrap file
        bootstrap_file = task.htcondor_bootstrap_file()
        if bootstrap_file:
            config.input_files.append(bootstrap_file)
            config.render_variables["bootstrap_file"] = pf(os.path.basename(bootstrap_file))

        # add the stageout file
        stageout_file = task.htcondor_stageout_file()
        if stageout_file:
            config.input_files.append(stageout_file)
            config.render_variables["stageout_file"] = pf(os.path.basename(stageout_file))

        # does the dashboard have a hook file?
        dashboard_file = self.dashboard.remote_hook_file()
        if dashboard_file:
            config.input_files.append(dashboard_file)
            config.render_variables["dashboard_file"] = pf(os.path.basename(dashboard_file))

        config.input_files.append(os.path.expandvars("$X509_USER_PROXY"))

        # output files
        config.output_files = []

        # custom content
        config.custom_content = []

        # logging
        # we do not use condor's logging mechanism since it requires that the submission directory
        # is present when it retrieves logs, and therefore we rely on the job.sh script
        config.log = None
        config.stdout = None
        config.stderr = None
        if task.transfer_logs:
            log_file = "stdall.txt"
            config.custom_log_file = log_file
            config.render_variables["log_file"] = pf(log_file)

        # we can use condor's file stageout only when the output directory is local
        # otherwise, one should use the stageout_file and stageout manually
        output_dir = task.htcondor_output_directory()
        if isinstance(output_dir, LocalDirectoryTarget):
            config.absolute_paths = True
            config.custom_content.append(("initialdir", output_dir.path))
        else:
            del config.output_files[:]

        # task hook
        config = task.htcondor_job_config(config, job_num, branches)

        # determine basenames of input files and add that list to the render data
        input_basenames = [pf(os.path.basename(path)) for path in config.input_files[1:]]
        config.render_variables["input_files"] = " ".join(input_basenames)

        # build the job file and get the sanitized config
        job_file, config = self.job_file_factory(**config.__dict__)

        # determine the absolute custom log file if set
        abs_log_file = None
        if config.custom_log_file and isinstance(output_dir, LocalDirectoryTarget):
            abs_log_file = output_dir.child(config.custom_log_file, type="f").path

        # return job and log files
        return {"job": job_file, "log": abs_log_file}

    def destination_info(self):
        info = []
        if self.task.htcondor_pool != NO_STR:
            info.append(", pool: {}".format(self.task.htcondor_pool))
        if self.task.htcondor_scheduler != NO_STR:
            info.append(", scheduler: {}".format(self.task.htcondor_scheduler))
        return ", ".join(info)


class HTCondorWorkflowExt(law.contrib.htcondor.workflow.HTCondorWorkflow):

    workflow_proxy_cls = HTCondorWorkflowProxyExt
