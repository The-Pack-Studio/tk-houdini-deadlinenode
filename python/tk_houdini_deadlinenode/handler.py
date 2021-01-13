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
import sys

# houdini
import hou

# toolkit
import sgtk
from sgtk.platform.qt import QtCore


class TkDeadlineNodeHandler(object):
    """Handle Tk Deadline node operations and callbacks."""


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

        # setup deadline qprocess
        if os.environ.has_key('DEADLINE_PATH') and hou.isUIAvailable():
            self._process = QtCore.QProcess(hou.qt.mainWindow())
            self._process.finished.connect(self._dependecy_finished)

        # create deadline connection
        deadline_repo = os.path.join(self._app.get_setting("deadline_server_root"), "api", "python")
        if os.path.exists(deadline_repo):
            sys.path.append(deadline_repo)

            import Deadline.DeadlineConnect as Connect

            self._deadline_connect = Connect.DeadlineCon('192.168.100.14', 8082)

            # cache pools and groups
            self._deadline_pools = self._deadline_connect.Pools.GetPoolNames()
            self._deadline_groups = self._deadline_connect.Groups.GetGroupNames()
        else:
            self._app.log_error('Could not find deadline repo!')
        

    ############################################################################
    # methods and callbacks executed via the OTLs

    # called when the node is created.
    def setup_node(self, node):
        try:
            self._app.log_metric("Create", log_version=True)
        except:
            # ingore any errors. ex: metrics logging not supported
            pass

    def get_deadline_pools(self):
        return self._deadline_pools

    def get_deadline_groups(self):
        return self._deadline_groups

    def submit_to_deadline(self, node):
        firstname = self._app.context.user['name'].split(' ')[0]
        self._dl_node = node

        # get static data
        self._session_info = {
            'department': self._app.context.step['name'],
            'task': self._app.context.task['name'],
            'entity': self._app.context.entity['name'],
            'project': self._app.context.project['name'],
            'pool': self._dl_node.parm('dl_pool').evalAsString(),
            'sec_pool': self._dl_node.parm('dl_secondary_pool').evalAsString(),
            'group': self._dl_node.parm('dl_group').evalAsString(),
            'chunk_size': self._dl_node.parm('dl_chunk_size').evalAsString(),
            'username': firstname,
            'dependencies': []
        }

        if self._dl_node.parm('dl_dependencies').evalAsString() != '':
            self._session_info['dependencies'] = self._dl_node.parm('dl_dependencies').evalAsString().split(',')

        # clear dependecies dict
        self._dl_submitted_ids = {}

        if not self._dl_node.isLocked():
            for render_node in self._dl_node.inputs():
                self._submit_node_tree_lookup(render_node)

            hou.ui.setStatusMessage('Successfully sent job(s) to the farm!', hou.severityType.ImportantMessage)
            self._dl_node.parm('dl_dependencies').set('')
        else:
            self._app.log_info('Sgtk Deadline node locked, will not submit any jobs!')

    def deadline_dependencies(self, node):
        self._dl_node = node
        if os.environ.has_key('DEADLINE_PATH'):
            deadline_bin = os.path.join(os.environ['DEADLINE_PATH'], 'deadlinecommand')
            self._process.start(deadline_bin, ["-selectdependencies", node.parm('dl_dependencies').eval()])

    ############################################################################
    # Private methods
    
    def _dependecy_finished(self):
        output = str(self._process.readAllStandardOutput())
        output = output.replace("\r", "").replace("\n", "")

        if output != "Action was cancelled by user":
            self._dl_node.parm('dl_dependencies').set(output)

    def _submit_node_tree_lookup(self, render_node):
        dep_job_ids = []

        if render_node:
            if not render_node.isLocked():
                for dep_node in render_node.inputs():
                    if dep_node.path() not in self._dl_submitted_ids.keys():
                        self._submit_node_tree_lookup(dep_node)
                        
                    dep_job_ids.extend(self._dl_submitted_ids[dep_node.path()])
                    
            if not render_node.isBypassed() and render_node.type().name() in (self.TK_SUP_GEO_ROPS + self.TK_SUP_RENDER_ROPS):
                if render_node.type().name() in ['sgtk_geometry', 'sgtk_arnold']:
                    render_node.hm().pre_render(render_node)
                
                dep_id = self._submit_dl_job(render_node, dep_job_ids)
                self._dl_submitted_ids[render_node.path()] = [dep_id]
            else:
                self._dl_submitted_ids[render_node.path()] = dep_job_ids

    def _submit_dl_job(self, node, dependencies):
        export_job = False
        if node.type().name() in self.TK_SUP_RENDER_ROPS:
            export_job = True

            disk_file = node.parm(self.TK_DISK_RENDER_ROPS[self.TK_SUP_RENDER_ROPS.index(node.type().name())]).evalAtFrame(0)
            output_file = node.parm(self.TK_OUT_RENDER_ROPS[self.TK_SUP_RENDER_ROPS.index(node.type().name())]).unexpandedString()
        else:
            output_file = node.parm(self.TK_OUT_GEO_ROPS[self.TK_SUP_GEO_ROPS.index(node.type().name())]).evalAsString()
        
        output_file = output_file.replace('$F4', '####')

        # Configure job name and version
        name_batch = hou.getenv('HIPNAME')
        name = '{} - {}'.format(hou.getenv('HIPNAME'), node.path())

        work_file_path = hou.hipFile.path()
        work_file_template = self._app.get_template("work_file_template")
        if (work_file_template and work_file_template.validate(work_file_path)):
            version = work_file_template.get_fields(work_file_path)['version']
        
        # Override for sgtk_geometry
        if node.type().name() in ['sgtk_geometry', 'sgtk_arnold']:
            version = node.parm('ver').evalAsInt()
            name = '{} v{}'.format(name, str(version).zfill(3))

        # Create submission info file
        job_info_file = {
            "Plugin": "Houdini",
            "UserName": self._session_info['username'],
            "Name": name,
            "Department": self._session_info['department'],
            "Pool": self._session_info['pool'],
            "SecondaryPool": self._session_info['sec_pool'],
            "Group": self._session_info['group'],
            "Priority": self.TK_DEFAULT_GEO_PRIORITY,
            "BatchName": name_batch,
            "ExtraInfo0": self._session_info['task'],
            "ExtraInfo1": self._session_info['project'],
            "ExtraInfo2": self._session_info['entity'],
            "ExtraInfo3": version,
            }

        # Nozon important env vars from the running Houdini to the job's env (donat)
        env_vars = ['RLM_LICENSE',
                    'SOLIDANGLE_LICENSE',
                    'ADSKFLEX_LICENSE_FILE',
                    'HOUDINI_OTLSCAN_PATH',
                    'PYTHONPATH']

        # LOOP THROUGH THE VARS AND ADD A LINE TO THE JOB FILE FOR EACH DEFINED ENVIRONMENT VARIABLE.
        env_index = 0
        for env_var in env_vars:
            #CHECK THAT THE ENVIRONMENT VARIABLE HAS BEEN SET (WE DON'T WANT TO PASS 'NONE' TYPE VALUES TO DEADLINE AS THIS CAN CAUSE PROBLEMS)
            if os.environ.get(env_var) is not None:
                job_info_file['EnvironmentKeyValue%d' % env_index] = "%s=%s" % (env_var, os.environ.get(env_var))
                env_index += 1
        # Shotgun location
        if os.environ.get("TANK_CURRENT_PC") is not None:
            job_info_file['EnvironmentKeyValue%d' % env_index] = "%s=%s" % ("NOZ_TK_CONFIG_PATH", os.environ.get("TANK_CURRENT_PC"))
            env_index += 1

        # getting rid of tk-houdini in the Houdini_Path env var
        if os.environ.get("HOUDINI_PATH") is not None:
            hou_path = os.environ.get("HOUDINI_PATH")
            hou_path_list = hou_path.split(";")
            hou_path_list_no_sgtk = [x for x in hou_path_list if 'tk-houdini' not in x]
            hou_path_no_sgtk = ";".join(hou_path_list_no_sgtk)
            job_info_file['EnvironmentKeyValue%d' % env_index] = "%s=%s" % ("HOUDINI_PATH", hou_path_no_sgtk)
            env_index += 1

        # parse the PATH var to only take houdini stuff
        if os.environ.get("PATH") is not None: 
            e_path = os.environ.get("PATH")
            e_path_list = e_path.split(";")
            # we might miss stuff here !!!
            e_path_hou_only_list = [x for x in e_path_list if 'houdini' in x.lower()]
            e_path_hou_only = ";".join(e_path_hou_only_list)
            job_info_file['EnvironmentKeyValue%d' % env_index] = "%s=%s" % ("PATH", e_path_hou_only)
            env_index += 1

        job_info_file['EnvironmentKeyValue%d' % env_index] = "%s=%s" % ("NOZ_HIPFILE", hou.hipFile.path())

        dependencies.extend(self._session_info['dependencies'])
        for index, dependency in enumerate(dependencies):
            job_info_file['JobDependency{}'.format(str(index))] = dependency

        # Set correct Frame Range
        frame_range = self._get_frame_range(node)
        job_info_file['Frames'] = "%s-%s" % (frame_range[0], frame_range[1])
        
        # Set Chunk size
        chunk_size = self._session_info['chunk_size']
        if node.parm('initsim') and node.parm('initsim').evalAsInt():
            chunk_size = frame_range[1] - frame_range[0] + 1
        elif node.type().name() == 'sgtk_geometry':
            if node.parm('types').evalAsString() == 'abc' and node.parm('abcSingleFile').evalAsInt():
                chunk_size = frame_range[1] - frame_range[0] + 1
        
        job_info_file['ChunkSize'] = chunk_size
        
        # output
        if export_job:
            job_info_file['OutputDirectory0'] = os.path.dirname(disk_file)
        else:
            job_info_file['OutputFilename0'] = output_file

        ver = hou.applicationVersion()
        plugin_info_file = {
            "Version": "%s.%s" % (ver[0], ver[1]),
            "IgnoreInputs": True,
            "OutputDriver": node.path(),
            "Build": "None",
        }

        if node.type().name() == 'sgtk_geometry':
            path = node.hm().app().handler.get_backup_file(node)
            plugin_info_file["SceneFile"] = path
        else:
            plugin_info_file["SceneFile"] = hou.hipFile.path()

        render_job_id = self._deadline_connect.Jobs.SubmitJob(job_info_file, plugin_info_file)["_id"]

        # export job
        if export_job:
            export_type = node.type().name()
            
            # job info file
            export_job_info_file = {
                "Name": name,
                "UserName": self._session_info['username'],
                "Department": self._session_info['department'],
                "Pool": self._session_info['pool'],
                "SecondaryPool": self._session_info['sec_pool'],
                "Group": "arnold",
                "JobDependencies": render_job_id,
                "Priority": self.TK_DEFAULT_RENDER_PRIORITY,
                "IsFrameDependent": True,
                "Frames": "%s-%s" % (frame_range[0], frame_range[1]),
                "ChunkSize": 1,
                "OutputFilename0": os.path.basename(output_file),
                "OutputDirectory0": os.path.dirname(output_file),
                "BatchName": name_batch,
                "ExtraInfoKeyValue0": "%s=%s" % ("ProjectDirectory", os.path.basename(self._app.sgtk.pipeline_configuration.get_path())),
                "ExtraInfo0": self._session_info['task'],
                "ExtraInfo1": self._session_info['project'],
                "ExtraInfo2": self._session_info['entity'],
                "ExtraInfo3": "{} - {} v{} - {}".format(self._session_info['entity'], self._session_info['task'], str(version).zfill(3), self._session_info['username'])
                }

            # Add extra values for renders to enable post jobs on deadline
            if node.type().name() in ['sgtk_geometry', 'sgtk_arnold']:
                publish_info = {'publish_name': name,
                            'publish_type': 'Rendered Image', 
                            'publish_version': version, 
                            'publish_comment': '',
                            'publish_dependencies_paths': [], 
                            'publish_dependencies_ids': []
                            }
                publish_info = json.dumps(publish_info)

                export_job_info_file["ExtraInfoKeyValue1"] = "context=%s" % self._app.context.serialize(with_user_credentials=False, use_json=True)
                export_job_info_file["ExtraInfoKeyValue2"] = "PublishInfo=%s" % publish_info
                export_job_info_file["ExtraInfoKeyValue3"] = "ShotgunEvent_createVersion=True"
                
                export_job_info_file["ExtraInfoKeyValue4"] = "ProjectScriptFolder=%s" % os.path.join(self._app.sgtk.pipeline_configuration.get_config_location(), "hooks", "tk-multi-publish2", "nozonpub")
                export_job_info_file["ExtraInfoKeyValue5"] = "NozCreateSGMovie=True"
                export_job_info_file["ExtraInfoKeyValue6"] = "UploadSGMovie=True"
                export_job_info_file["ExtraInfoKeyValue7"] = "FrameRate=%s" % hou.fps()
                export_job_info_file["ExtraInfoKeyValue8"] = "NozMovSettingsPreset=3d"
                
                export_job_info_file["ExtraInfoKeyValue9"] = "EntityType=%s" % self._session_info['task']
                export_job_info_file["ExtraInfoKeyValue10"] = "ProjectId=%i" % self._app.context.project['id']
                export_job_info_file["ExtraInfoKeyValue11"] = "TaskId=%i" % self._app.context.task['id']
                export_job_info_file["ExtraInfoKeyValue12"] = "EntityId=%i"% self._app.context.entity['id']

                export_job_info_file["ExtraInfo5"] = export_job_info_file["UserName"]

            if "sgtk_mantra" in export_type:
                export_job_info_file["Plugin"] = "Mantra"
            elif "sgtk_arnold" in export_type:
                export_job_info_file["Plugin"] = "Arnold"

            # Shotgun location
            export_job_info_file["EnvironmentKeyValue0"] = "NOZ_TK_CONFIG_PATH=%s" % self._app.sgtk.pipeline_configuration.get_path()

            # plugin info file
            export_plugin_info_file = { "CommandLineOptions": "" }
            if export_type == "sgtk_mantra":
                export_plugin_info_file["SceneFile"] = disk_file

                major_version, minor_version = hou.applicationVersion()[:2]
                export_plugin_info_file["Version"] = "%s.%s" % (major_version, minor_version)
            elif export_type == "sgtk_arnold":
                export_plugin_info_file["InputFile"] = disk_file
                export_plugin_info_file["Verbose"] = 4

            export_job_id = self._deadline_connect.Jobs.SubmitJob(export_job_info_file, export_plugin_info_file)["_id"]

            return export_job_id
        return render_job_id
    
    def _get_frame_range(self, node):
        if node.parm('trange').evalAsString() == 'normal':
            node_range = node.parmTuple('f').evalAsFloats()
            return (int(node_range[0]), int(node_range[1]), int(node_range[2]))
        else:
            return (0, 0, 1)