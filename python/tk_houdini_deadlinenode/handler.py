# Copyright (c) 2015 Shotgun Software Inc.
#
# CONFIDENTIAL AND PROPRIETARY
#
# This work is provided "AS IS" and subject to the Shotgun Pipeline Toolkit
# Source Code License included in this distribution package. See LICENSE.
# By accessing, using, copying or modifying this work you indicate your
# agreement to the Shotgun Pipeline Toolkit Source Code License. All rights
# not expressly granted therein are reserved by Shotgun Software Inc.

# built-ins
import os
import json

# houdini
import hou

# toolkit
import sgtk
from sgtk.platform.qt import QtCore, QtGui


class TkDeadlineNodeHandler(object):
    """Handle Tk Geometry node operations and callbacks."""


    ############################################################################
    # Class data

    TK_SUP_GEO_ROPS = ['sgtk_geometry']
    TK_OUT_GEO_ROPS = ['sopoutput']

    TK_SUP_RENDER_ROPS = ['sgtk_mantra', 'sgtk_arnold']
    TK_OUT_RENDER_ROPS = ['sgtk_vm_picture', 'ar_picture']
    TK_DISK_RENDER_ROPS = ['sgtk_soho_diskfile', 'sgtk_ass_diskfile']

    TK_DEFAULT_GEO_PRIORITY = 500
    TK_DEFAULT_RENDER_PRIORITY = 50

    ############################################################################
    # Instance methods

    def __init__(self, app):
        """Initialize the handler.
        
        :params app: The application instance. 
        
        """

        # keep a reference to the app for easy access to templates, settings,
        # logging methods, tank, context, etc.
        self._app = app

        # get deadline variables
        self._deadline_info = {}

        if os.environ.has_key('DEADLINE_PATH'):
            if hou.isUIAvailable():
                self._deadline_bin = os.path.join(os.environ['DEADLINE_PATH'], 'deadlinecommand')

                process = QtCore.QProcess(hou.qt.mainWindow())
                arguments = ["-prettyJSON", "-GetSubmissionInfo", "Pools", "Groups", "MaxPriority", 
                                "TaskLimit", "UserHomeDir", "RepoDir:submission/Houdini/Main", 
                                "RepoDir:submission/Integration/Main", "RepoDirNoCustom:draft", 
                                "RepoDirNoCustom:submission/Jigsaw"]

                process.start(self._deadline_bin, arguments)
                process.waitForFinished()
                self._deadline_info = json.loads(str(process.readAllStandardOutput()))["result"]
                self._app.log_debug('Retrieved deadline info {}'.format(self._deadline_info))
        else:
            self._app.log_error('Could not find deadline environment variable!')

    ############################################################################
    # methods and callbacks executed via the OTLs

    # called when the node is created.
    def setup_node(self, node):
        try:
            self._app.log_metric("Create", log_version=True)
        except:
            # ingore any errors. ex: metrics logging not supported
            pass

    def call_deadline_command(self, arguments):
        process = QtCore.QProcess(hou.qt.mainWindow())
        
        process.start(self._deadline_bin, arguments)
        process.waitForFinished()
        return str(process.readAllStandardOutput())

    def get_deadline_info(self):
        return self._deadline_info

    def submit_to_deadline(self, sgtk_dl_node):
        # get static data
        self._session_info = {
            'department': self._app.context.step['name'],
            'pool': sgtk_dl_node.parm('dl_pool').evalAsString(),
            'sec_pool': sgtk_dl_node.parm('dl_secondary_pool').evalAsString(),
            'group': sgtk_dl_node.parm('dl_group').evalAsString(),
            'chunk_size': sgtk_dl_node.parm('dl_chunk_size').evalAsString(),
            'dependencies': []
        }

        if sgtk_dl_node.parm('dl_dependencies').evalAsString() != '':
            self._session_info['dependencies'] = sgtk_dl_node.parm('dl_dependencies').evalAsString().split(',')

        # clear dependecies dict
        self._dl_submitted_ids = {}

        if not sgtk_dl_node.isLocked():
            dep_nodes = sgtk_dl_node.inputs()
            
            for render_node in dep_nodes:
                self._submit_node_tree_lookup(render_node)
        else:
            self._app.log_info('Sgtk Deadline node locked, will not submit any jobs!')

    ############################################################################
    # Private methods
    
    def _submit_node_tree_lookup(self, render_node):
        dep_job_ids = []
        
        if not render_node.isLocked():
            dep_nodes = render_node.inputs()
            for dep_node in dep_nodes:
                if dep_node.path() not in self._dl_submitted_ids.keys():
                    self._submit_node_tree_lookup(dep_node)
                    
                dep_job_ids.extend(self._dl_submitted_ids[dep_node.path()])
                
        if not render_node.isBypassed() and render_node.type().name() in (self.TK_SUP_GEO_ROPS + self.TK_SUP_RENDER_ROPS):
            if render_node.type().name() == 'sgtk_geometry':
                render_node.hm().pre_render(render_node)
            
            self._dl_submitted_ids[render_node.path()] = [self._submit_render_job(render_node, dep_job_ids)]
        else:
            self._dl_submitted_ids[render_node.path()] = dep_job_ids

    def _submit_render_job(self, node, dependencies):
        self._app.log_info(node.path())

        export_job = False
        if node.type().name() in self.TK_SUP_RENDER_ROPS:
            export_job = True

            disk_file = node.parm(self.TK_DISK_RENDER_ROPS[self.TK_SUP_RENDER_ROPS.index(node.type().name())]).evalAtFrame(0)
            output_file = node.parm(self.TK_OUT_RENDER_ROPS[self.TK_SUP_RENDER_ROPS.index(node.type().name())]).unexpandedString()
            self._app.log_info('Disk file ' + disk_file)
            self._app.log_info('Output file ' + output_file)
        else:
            output_file = node.parm(self.TK_OUT_GEO_ROPS[self.TK_SUP_GEO_ROPS.index(node.type().name())]).evalAsString()
            self._app.log_info('Output file ' + output_file)
        
        output_file = output_file.replace('$F4', '####')

        # Configure job name
        name_batch = hou.getenv('HIPNAME')
        name = '{} - {}'.format(hou.getenv('HIPNAME'), node.path())
        if node.type().name() == 'sgtk_geometry':
            version = node.parm('ver').evalAsInt()
            name = '{} v{}'.format(name, str(version).zfill(3))

        # Nozon important env vars from the running Houdini to the job's env (donat)
        env_vars = ['RLM_LICENSE',
                    'SOLIDANGLE_LICENSE',
                    'ADSKFLEX_LICENSE_FILE',
                    'HOUDINI_OTLSCAN_PATH',
                    'PYTHONPATH']

        # Create submission info file
        job_info_file = os.path.join(self._deadline_info["UserHomeDir"], "temp", "houdini_submit_info.job")
        with open(job_info_file, "w") as file_handle:
            file_handle.write("Plugin=Houdini\n")
            file_handle.write("Name=%s\n" % name)
            file_handle.write("Department=%s\n" % self._session_info['department'])
            file_handle.write("Pool=%s\n" % self._session_info['pool'])
            file_handle.write("SecondaryPool=%s\n" % self._session_info['sec_pool'])
            file_handle.write("Group=%s\n" % self._session_info['group'])
            file_handle.write("Priority=%s\n" % self.TK_DEFAULT_GEO_PRIORITY)
            file_handle.write("BatchName=%s\n" % name_batch)

            # LOOP THROUGH THE VARS AND ADD A LINE TO THE JOB FILE FOR EACH DEFINED ENVIRONMENT VARIABLE.
            env_index = 0
            for env_var in env_vars:
                #CHECK THAT THE ENVIRONMENT VARIABLE HAS BEEN SET (WE DON'T WANT TO PASS 'NONE' TYPE VALUES TO DEADLINE AS THIS CAN CAUSE PROBLEMS)
                if os.environ.get(env_var) is not None:
                    file_handle.write("EnvironmentKeyValue%d=%s=%s\n" % (env_index, env_var, os.environ.get(env_var)))
                    env_index += 1
            # Shotgun location
            if os.environ.get("TANK_CURRENT_PC") is not None:
                file_handle.write("EnvironmentKeyValue%d=%s=%s\n" % (env_index, "NOZ_TK_CONFIG_PATH", os.environ.get("TANK_CURRENT_PC")))
                env_index += 1

            # getting rid of tk-houdini in the Houdini_Path env var
            if os.environ.get("HOUDINI_PATH") is not None:
                hou_path = os.environ.get("HOUDINI_PATH")
                hou_path_list = hou_path.split(";")
                hou_path_list_no_sgtk = [x for x in hou_path_list if 'tk-houdini' not in x]
                hou_path_no_sgtk = ";".join(hou_path_list_no_sgtk)
                file_handle.write("EnvironmentKeyValue%d=%s=%s\n" % (env_index, "HOUDINI_PATH", hou_path_no_sgtk))
                env_index += 1

            # parse the PATH var to only take houdini stuff
            if os.environ.get("PATH") is not None: 
                e_path = os.environ.get("PATH")
                e_path_list = e_path.split(";")
                # we might miss stuff here !!!
                e_path_hou_only_list = [x for x in e_path_list if 'houdini' in x.lower()]
                e_path_hou_only = ";".join(e_path_hou_only_list)
                file_handle.write("EnvironmentKeyValue%d=%s=%s\n" % (env_index, "PATH", e_path_hou_only))
                env_index += 1

            if hou.getenv("HIPFILE") is not None:
                file_handle.write("EnvironmentKeyValue%d=%s=%s\n" % (env_index, "NOZ_HIPFILE", hou.getenv("HIPFILE")))
                env_index += 1

            dependencies.extend(self._session_info['dependencies'])
            self._app.log_info('Dependencies {}'.format(dependencies))
            if len(dependencies):
                file_handle.write("JobDependencies=%s\n" % ' '.join(dependencies))
            
            # Set correct Frame Range
            frame_range = self._get_frame_range(node)
            file_handle.write("Frames=%s-%s\n" % (frame_range[0], frame_range[1]))
            
            # Set Chunk size
            chunk_size = self._session_info['chunk_size']
            if node.parm('initsim') and node.parm('initsim').evalAsInt():
                chunk_size = frame_range[1] - frame_range[0] + 1
            elif node.type().name() == 'sgtk_geometry':
                if node.parm('types').evalAsString() == 'abc' and node.parm('abcSingleFile').evalAsInt():
                    chunk_size = frame_range[1] - frame_range[0] + 1
            
            file_handle.write("ChunkSize=%s\n" % chunk_size)
            
            # output
            if not export_job:
                file_handle.write("OutputFilename0=%s\n" % output_file)
            else:
                file_handle.write("OutputDirectory0=%s\n" % os.path.dirname(disk_file))

        pluginInfoFile = os.path.join(self._deadline_info["UserHomeDir"], "temp", "houdini_plugin_info.job")
        with open(pluginInfoFile, "w") as file_handle:
            if node.type().name() == 'sgtk_geometry':
                path = node.hm().app().handler.get_backup_file(node)
                file_handle.write("SceneFile=%s\n" % path)
            else:
                file_handle.write("SceneFile=%s\n" % hou.hipFile.path())

            ver = hou.applicationVersion()
            file_handle.write("Version=%s.%s\n" % (ver[0], ver[1]))
            file_handle.write("IgnoreInputs=%s\n" % True)
            file_handle.write("OutputDriver=%s\n" % node.path())
            file_handle.write("Build=None\n")

        process = QtCore.QProcess(hou.qt.mainWindow())
        process.start(self._deadline_bin, [job_info_file, pluginInfoFile])
        process.waitForFinished()

        render_job_id = self._get_job_id_from_submission(str(process.readAllStandardOutput()))
        self._app.log_info("\n".join([line.strip() for line in str(process.readAllStandardOutput()).split("\n") if line.strip()]))

        if export_job:
            export_type = node.type().name()

            export_job_info_file = os.path.join(self._deadline_info["UserHomeDir"], "temp", "export_job_info.job")
            export_plugin_info_file = os.path.join(self._deadline_info["UserHomeDir"], "temp", "export_plugin_info.job")

            with open(export_job_info_file, 'w') as file_handle:
                if "sgtk_mantra" in export_type:
                    file_handle.write("Plugin=Mantra\n")
                elif "sgtk_arnold" in export_type:
                    file_handle.write("Plugin=Arnold\n")

                # Shotgun location
                if os.environ.get("TANK_CURRENT_PC") is not None:
                    file_handle.write("EnvironmentKeyValue0=%s=%s\n" % ("NOZ_TK_CONFIG_PATH", os.environ.get("TANK_CURRENT_PC")))

                # Project Name
                file_handle.write("ExtraInfoKeyValue0=%s=%s\n" % ("ProjectDirectory", os.path.basename(os.environ.get("TANK_CURRENT_PC"))))

                file_handle.write("Name=%s\n" % name)
                file_handle.write("Department=%s\n" % self._session_info['department'])
                file_handle.write("Pool=%s\n" % self._session_info['pool'])
                file_handle.write("SecondaryPool=%s\n" % self._session_info['sec_pool'])
                file_handle.write("Group=arnold\n")
                file_handle.write("JobDependencies=%s\n" % render_job_id)
                file_handle.write("Priority=%s\n" % self.TK_DEFAULT_RENDER_PRIORITY)
                file_handle.write("IsFrameDependent=true\n")
                file_handle.write("Frames=%s-%s\n" % (frame_range[0], frame_range[1]))
                file_handle.write("ChunkSize=1\n")
                file_handle.write("OutputFilename0=%s\n" % output_file)
                file_handle.write("BatchName=%s\n" % name_batch)

            with open(export_plugin_info_file, 'w') as file_handle:
                if export_type == "sgtk_mantra":
                    file_handle.write("SceneFile=%s\n" % disk_file)
                    file_handle.write("CommandLineOptions=\n")

                    major_version, minor_version = hou.applicationVersion()[:2]
                    file_handle.write("Version=%s.%s\n" % (major_version, minor_version))
                elif export_type == "sgtk_arnold":
                    file_handle.write("InputFile=" + disk_file + "\n")
                    file_handle.write("CommandLineOptions=\n")
                    file_handle.write("Verbose=4\n")

            process = QtCore.QProcess(hou.qt.mainWindow())
            process.start(self._deadline_bin, [export_job_info_file, export_plugin_info_file])
            process.waitForFinished()

            export_job_id = self._get_job_id_from_submission(str(process.readAllStandardOutput()))
            self._app.log_info("\n".join([line.strip() for line in str(process.readAllStandardOutput()).split("\n") if line.strip()]))

            return export_job_id
        return render_job_id

    def _get_job_id_from_submission(self, submission_results):
        job_id = ""
        self._app.log_info('Submission Result %s' % submission_results)
        for line in submission_results.split():
            if line.startswith("JobID="):
                job_id = line.replace("JobID=", "").strip()
                break

        return job_id

    def _get_frame_range(self, node):
        if node.parm('trange').evalAsString() == 'normal':
            node_range = node.parmTuple('f').evalAsStrings()
            return (str(node_range[0]), str(node_range[1]), str(node_range[2]))
        else:
            return (0, 0, 1)

    # extract fields from current Houdini file using the workfile template
    def _get_hipfile_fields(self):
        work_file_path = hou.hipFile.path()

        work_fields = {}
        work_file_template = self._app.get_template("work_file_template")
        if (work_file_template and 
            work_file_template.validate(work_file_path)):
            work_fields = work_file_template.get_fields(work_file_path)

        return work_fields