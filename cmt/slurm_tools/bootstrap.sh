#!/usr/bin/env bash

# Bootstrap file for batch jobs that is sent with all jobs and
# automatically called by the law remote job wrapper script to find the
# setup.sh file of this example which sets up software and some environment
# variables. The "{{cmt_base}}" variable is defined in the workflow
# base tasks in $CMT_BASE/cmt/base_tasks/base.py.

action() {
    export CMT_ON_SLURM="1"
    export X509_USER_PROXY="{{cmt_base}}/x509up"

    source "{{cmt_base}}/setup.sh"
}
action
