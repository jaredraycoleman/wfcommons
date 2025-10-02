#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021 The WfCommons Team.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

import pathlib
import sys
import re
import fnmatch

from logging import Logger
from typing import List, Optional, Union, Pattern

from .abstract_logs_parser import LogsParser
from ...common.file import File
from ...common.machine import Machine, MachineSystem
from ...common.task import Task, TaskType
from ...common.workflow import Workflow

####
## Example logs provided by the TaskVine team
##
## https://github.com/cooperative-computing-lab/taskvine-example-logs?tab=readme-ov-file
###

class TaskVineLogsParser(LogsParser):
    """
    Parse TaskVine logs to generate workflow instance. This parser has some limitations in that
    it may miss task input/output items due to them not being files or URLs. More importantly,
    because in TaskVine different tasks can have the same file names as input/output but those file names
    actually my correspond to different data sources, the parser will assume that these tasks have
    the exact same input/output files.  There is likely a way to address this, but it hasn't been done yet.
    For instance, the Gutenberg TaskVine example isn't parsed correctly by this parser due to the above feature.

    :param vine_logs_dir: TaskVine's vine-logs directory
    :type vine_logs_dir: pathlib.Path
    :param filenames_to_ignore: TaskVine sometimes considers that executables and package files
                                are input to tasks. This argument is the list of names of files or glob patterns that should be
                                ignored in the reconstructed instances, which typically do not include such
                                files at task input. For instance, if reconstructing a workflow from an execution
                                of a WfBench-generated benchmark, one could pass ["wfbench", "cpu-benchmark", "stress-ng"]. Or
                                one could pass something like ["*.py"] to ignore all Python source files.
    :type filenames_to_ignore: Optional[List[Union[str, Pattern[str]]]]
    :param description: Workflow instance description.
    :type description: Optional[str]
    :param logger: The logger where to log information/warning or errors (optional).
    :type logger: Optional[Logger]
    """
    def __init__(self,
                 vine_logs_dir: pathlib.Path,
                 filenames_to_ignore: Optional[List[Union[str, Pattern[str]]]] = None,
                 description: Optional[str] = None,
                 logger: Optional[Logger] = None) -> None:
        """Create an object of the makeflow log parser."""
        super().__init__('TaskVine', 'http://https://ccl.cse.nd.edu/software/taskvine/', description, logger)

        # Sanity check
        if not vine_logs_dir.is_dir():
            raise OSError(f'The provided path does not exist or is not a folder: {vine_logs_dir}')

        debug_file:  pathlib.Path = vine_logs_dir / "debug"
        if not debug_file.is_file():
            raise OSError(f'Cannot find file: {debug_file}')
        taskgraph_file: pathlib.Path = vine_logs_dir / "taskgraph"
        if not taskgraph_file.is_file():
            raise OSError(f'Cannot find file: {taskgraph_file}')
        transactions_file: pathlib.Path = vine_logs_dir / "transactions"
        if not transactions_file.is_file():
            raise OSError(f'Cannot find file: {transactions_file}')

        self.debug_file: pathlib.Path = debug_file
        self.taskgraph_file: pathlib.Path = taskgraph_file
        self.transactions_file: pathlib.Path = transactions_file

        self.filenames_to_ignore : List[Union[str, Pattern[str]]] = filenames_to_ignore
        self.compiled_patterns_to_ignore = []
        if filenames_to_ignore:
            for pattern in filenames_to_ignore:
                if isinstance(pattern, re.Pattern):
                    self.compiled_patterns_to_ignore.append(pattern)
                else:
                    # Convert glob pattern to regex
                    regex_pattern = fnmatch.translate(pattern)
                    self.compiled_patterns_to_ignore.append(re.compile(regex_pattern))

        self.files_map = {}
        self.task_command_lines = {}
        self.task_runtimes = {}
        self.workers = {}
        self.task_workers = {}
        self.task_input_files = {}
        self.task_output_files = {}
        self.makespan = 0
        self.known_task_ids = []
        # TODO: Get the next TWO items from workflow.json (overkill?)
        self.wms_name = "TaskVine"
        self.wms_url = "http://ccl.cse.nd.edu/software/taskvine"
        self.wms_version = None

    def build_workflow(self, workflow_name: Optional[str] = None) -> Workflow:
        """
        Create workflow instance based on the workflow execution logs.

        :param workflow_name: The workflow name.
        :type workflow_name: Optional[str]

        :return: A workflow instance object.
        :rtype: Workflow
        """
        self.workflow_name = workflow_name

        # create base workflow instance object
        self.workflow = Workflow(name=self.workflow_name,
                                 description=self.description,
                                 runtime_system_version=None,  # Will be set later
                                 runtime_system_name=self.wms_name,
                                 runtime_system_url=self.wms_url)

        # Construct the task command-line array
        self._construct_task_command_lines()
        # sys.stderr.write(str(self.task_command_lines.keys()) + "\n")

        # At this point, the ONLY tasks we care about are the ones for which we have a command-line
        self.known_task_ids = sorted(self.task_command_lines.keys())

        # Construct file map
        self._construct_file_map()
        # sys.stderr.write("FILEMAP: " + str(self.files_map) + "\n")
        for file_key in self.files_map.keys():
            if not "size" in self.files_map[file_key]:
                sys.stderr.write(f"Warning: Could not determine size for file with key {file_key}: assuming zero bytes.\n")
                self.files_map[file_key]["size"] = 0
        sys.stderr.write(f"Identified {len(self.files_map)} valid files\n")

        # Construct the task runtimes
        self._construct_task_runtimes()
        # sys.stderr.write("TASK RUN TIMES: " + str(self.task_runtimes) + "\n")

        # Check whether every known task has a runtime, and if not forget it :(
        to_remove = []
        for task_id in self.known_task_ids:
            if task_id not in self.task_runtimes.keys():
                sys.stderr.write(f"Warning: Ignoring task {task_id} because runtime could not be determined.\n")
                to_remove.append(task_id)
        for victim in to_remove:
            self.known_task_ids.remove(victim)
        sys.stderr.write(f"Identified {len(self.known_task_ids)} valid tasks\n")

        # Construct the input and output file for each task
        self._construct_task_input_output_files()
        # print("Task input files: " + str(self.task_input_files))
        # print("Task output files: " + str(self.task_output_files))

        # Construct the workers for each task
        self._construct_task_workers()

        # Compute the overall workflow makespan
        self._compute_overall_makespan()

        # Construct the workflow
        self._construct_workflow()

        return self.workflow

    def _compute_overall_makespan(self) -> None:
        start_date = 0
        end_date = 0
        with open(self.transactions_file) as f:
            for line in f:
                if line[0] == "#":
                    continue
                date = int(line.split()[0])
                if start_date == 0:
                    start_date = date
                end_date = date
        self.makespan = float(end_date - start_date) / 1_000_000.0


    def _construct_task_workers(self) -> None:

        # Build the map of workers with keys and IP addresses
        with open(self.transactions_file) as f:
            for line in f:
                if " CONNECTION" in line and line[0] != "#":
                    # 1742250541844548 318561 WORKER worker-244c25b911574b77ca5f789b6fd7b080 CONNECTION 10.32.93.67:35470
                    tokens = line.split()
                    worker_key = tokens[3]
                    worker_ip = tokens[5].split(":")[0]
                    self.workers[worker_key] = {"ip": worker_ip}

        # Retrieve other relevant information
        with open(self.debug_file) as f:
            for line in f:
                if "running CCTools" in line:
                    # 2025/05/15 19:35:50.58 vine_manager[3129835]vine: d32cepyc235.crc.nd.edu (10.32.91.237:54604) running CCTools version 8.0.0 on Linux (operating system) with architecture x86_64 is ready
                    for worker_key in self.workers.keys():
                        if self.workers[worker_key]["ip"] + ":" in line:
                            line = line[line.find("vine: ") + len("vine: "):]
                            tokens = line.split()
                            hostname = tokens[0]
                            self.wms_version = "CCTools-version-" + tokens[5]
                            operating_system = tokens[7]
                            architecture = tokens[12]
                            self.workers[worker_key]["hostname"] = hostname
                            self.workers[worker_key]["operating_system"] = operating_system
                            self.workers[worker_key]["architecture"] = architecture

        # Create machine objects to replace the dictionaries
        for worker_key in self.workers.keys():
            hostname = self.workers[worker_key]["hostname"]
            if self.workers[worker_key]["operating_system"] == "Linux":
                operating_system = MachineSystem.LINUX
            else:
                operating_system = MachineSystem.UNKNOWN

            architecture = self.workers[worker_key]["architecture"]

            machine = Machine(
                name=hostname,
                cpu={
                    'coreCount': 0,
                    'speedInMHz': 0,
                },
                system=operating_system,
                architecture=architecture,
                memory=0,
                release="unknown"
            )
            self.workers[worker_key] = machine

        # Associate a worker to each task
        with open(self.transactions_file) as f:
            for line in f:
                # 1747338058682112 3129835 TASK 3421 RUNNING worker-2d726e856c7e5ce67e67aa882e1cbe75  FIRST_RESOURCES {"time_commit_start":[1747338058.680938,"s"],"time_commit_end":[1747338058.682075,"s"],"time_input_mgr":[0.001137,"s"],"size_input_mgr":[0.001008987426757812,"MB"],"memory":[5333,"MB"],"disk":[15391,"MB"],"gpus":[0,"gpus"],"cores":[1,"cores"]}
                if " TASK " in line and " RUNNING " in line and "worker-" in line:
                    tokens = line.split()
                    task_id = int(tokens[3])
                    worker_key = tokens[5]
                    if task_id in self.known_task_ids:
                        self.task_workers[task_id] = self.workers[worker_key]





    def _construct_task_command_lines(self) -> None:
        with open(self.debug_file) as f:
            for line in f:
                if "state change: READY (1) to RUNNING (2)" in line:
                    [task_index] = line[line.find("Task ") + len("Task "):].split()[0:1]
                    command_line = previous_line[previous_line.find("busy on '") + len("busy on '"):-2]
                    self.task_command_lines[int(task_index)] = command_line
                    # May not be full-proof in case of commands like "export A=b; executable ..." but
                    # may help.....
                    # executable = command_line.split()[0]
                    # self.filenames_to_ignore.add(executable)
                previous_line = line


    def _construct_file_map(self) -> None:

        filename_to_key_map = {}
        # One pass through the debug file to create the initial file key -> filename mapping
        with open(self.debug_file) as f:
            for line in f:
                if "__vine_env_task" in line: # Ignore that weird task/file
                    continue
                if "infile " in line :
                    # 2025/09/09 21:12:48.02 vine_manager[239]vine: tx to dab178765b01 (127.0.0.1:34382): infile file-rnd-fmtpwpiobiumeze blastall_00000016_outfile_0016 0
                    [file_key, filename] = line[line.find("infile ") + len("infile "):].split()[:2]
                elif "outfile " in line and "completed with outfile " not in line and "outfile =" not in line:
                    # 2025/09/30 18:37:19.74 vine_manager[1849324]vine: tx to d64cepyc028.crc.nd.edu (10.32.94.18:47558): outfile temp-rnd-pidiwheippcwbeu fde2b5eb-9713-423a-8bc6-f4f9263ad20b.pkl 0 3
                    [file_key, filename] = line[line.find("outfile ") + len("outfile "):].split()[:2]
                else:
                    continue
                should_ignore = any(pattern.search(filename) for pattern in self.compiled_patterns_to_ignore)
                if should_ignore:
                    continue
                # NOTE THAT THE FILENAME MAY NOT BE UNIQUE IN TASKVINE WORKFLOWS, SO
                # WE ADD THE KEY
                self.files_map[file_key] = {"filename": filename + "." + file_key}
                filename_to_key_map[filename] = file_key

        # Pass through the transactions file to get the file sizes
        with open(self.debug_file) as f:
            for line in f:
                if "): file " in line:
                    [file_key, file_size] = line[line.find("): file ") + len("): file "):].split()[0:2]
                else:
                    continue
                if file_key in self.files_map:
                    self.files_map[file_key]["size"] = int(file_size)


    def _construct_task_runtimes(self) -> None:
        task_start_times = {}
        task_end_times = {}

        # This method consists only of

        with open(self.transactions_file) as f:
            for line in f:
                if line[0] == "#":
                    continue
                if "RUNNING" in line:
                    [start_date, ignore, ignore, task_index] = line.split()[0:4]
                    if int(task_index) in self.known_task_ids:
                        task_start_times[int(task_index)] = int(start_date)
                elif "DONE" in line:
                    [end_date, ignore, ignore, task_index] = line.split()[0:4]
                    if int(task_index) in self.known_task_ids:
                        task_end_times[int(task_index)] = int(end_date)

        for task_index in task_start_times:
            if task_index in task_end_times:
                self.task_runtimes[task_index] = (
                    float(task_end_times[task_index] - task_start_times[task_index]) / 1_000_000.0)

    def _construct_task_input_output_files(self) -> None:

        # Initialize all entries
        for task_id in self.known_task_ids:
            self.task_input_files[task_id] = []
            self.task_output_files[task_id] = []

        with open(self.taskgraph_file) as f:
            for line in f:
                if "->" not in line:
                    continue
                if "file-task" in line:  # Ignoring what I think are taskvine internal/specific things
                    continue
                line = line[:-1]
                [source, ignore, destination] = line.split()
                # Remove quotes
                source = source [1:-1]
                destination = destination [1:-2]
                # Remove weird file- prefix
                source = source.replace("--", "-")  # Sometimes there is an unexpected "--"!!
                destination = destination.replace("--", "-")  # Sometimes there is an unexpected "--"!!
                if source.startswith("file-"):
                    source = source[len("file-"):]
                if destination.startswith("file-"):
                    destination = destination[len("file-"):]

                if "task-" in source and "file-" not in source:
                    try:
                        task_id = int(source.split("-")[1])
                    except ValueError as e:
                        raise Exception(f"The source was {source} and the split around '-' failed!")

                    if task_id not in self.task_runtimes:
                        continue
                    file_key = destination
                    if file_key not in self.files_map:
                        continue
                    output_file = self.files_map[file_key]["filename"]
                    self.task_output_files[task_id].append(output_file)
                elif "task" in destination and "file" not in destination:
                    try:
                        task_id = int(destination.split("-")[1])
                    except ValueError as e:
                        raise Exception(f"The destination was {destination} and the split around '-' failed!")
                    if task_id not in self.task_runtimes:
                        continue
                    file_key = source
                    if file_key not in self.files_map:
                        continue
                    input_file = self.files_map[file_key]["filename"]
                    self.task_input_files[task_id].append(input_file)
                else:
                    raise ValueError("Error in the taskgraph file")


    def _construct_workflow(self) -> None:
        # Create files and put them in a map
        file_object_map = {}
        for file_key in self.files_map:
            filename = self.files_map[file_key]["filename"]
            file_size = self.files_map[file_key]["size"]
            file_object_map[filename] = File(file_id=filename,
                                    size=file_size,
                                    logger=self.logger)

        # Create all tasks
        task_map = {}
        for task_id in self.known_task_ids:
            task_name = "Task_%d" % task_id
            task = Task(name=task_name,
                        task_id=task_name,
                        task_type=TaskType.COMPUTE,
                        runtime=self.task_runtimes[task_id],
                        program=self.task_command_lines[task_id].split()[0],
                        args=self.task_command_lines[task_id].split()[1:],
                        cores=1,
                        input_files=[file_object_map[filename] for filename in self.task_input_files[task_id]],
                        output_files=[file_object_map[filename] for filename in self.task_output_files[task_id]],
                        logger=self.logger)
            if task_id in self.task_workers.keys():
                task.machines = [self.task_workers[task_id]]
            else:
                sys.stderr.write(f"Warning: couldn't find a worker/machine associated to task {task_id}.")
            task_map[task_id] = task
            self.workflow.add_task(task)
            # sys.stderr.write(f"Added task {task_name}: {len(self.workflow.tasks)}\n")


        # Adding all edges, which is pretty inefficiently done for now, by looking at all pairs of tasks!
        for task1_id in self.task_runtimes:
            for task2_id in self.task_runtimes:
                if task1_id == task2_id:
                    continue
                task1_output_files = self.task_output_files[task1_id]
                task2_input_files = self.task_input_files[task2_id]
                has_intersection = bool(set(task1_output_files) & set(task2_input_files))
                if has_intersection:
                    self.workflow.add_dependency(task_map[task1_id].name, task_map[task2_id].name)
                    # sys.stderr.write(f"Added dependency {task_map[task1_id].name} -> {task_map[task2_id].name}\n")

        # Setting the makespan
        self.workflow.makespan = self.makespan

        # Setting the runtime system version (after the fact)
        self.workflow.runtime_system_version = self.wms_version