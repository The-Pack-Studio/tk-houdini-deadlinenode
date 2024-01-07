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
import platform

# houdini
import hou

# toolkit
import sgtk
from sgtk.platform.qt import QtCore


class TkDeadlineNodeHandler(object):
    """Handle Tk Deadline node operations and callbacks."""


    ############################################################################
    # Class data

    TK_DEFAULT_GEO_PRIORITY = 99
    TK_DEFAULT_SIM_PRIORITY = 100
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
        if 'DEADLINE_PATH' in os.environ and hou.isUIAvailable():
            self._process = QtCore.QProcess(hou.qt.mainWindow())
            self._process.finished.connect(self._dependecy_finished)

        fw = sgtk.platform.get_framework("tk-framework-deadline")
        self._deadline_connect = fw.deadline_connection()        

        # Create a pool with the name of the project, if necessary
        # project_pool = self._app.context.project['name'].replace(" ", "_")
        # if not project_pool in self._deadline_connect.Pools.GetPoolNames():
        #     pool_create = self._deadline_connect.Pools.AddPool(project_pool)
        #     if pool_create == "Success":
        #         self._app.log_debug(f"Created new deadline pool: {project_pool}")


        # TODO : set the 'correct' project pool and sec pool on the Houdini node

        # cache pools and groups
        self._deadline_pools = self._deadline_connect.Pools.GetPoolNames()
        self._deadline_groups = self._deadline_connect.Groups.GetGroupNames()

        self.nozmov_app = self._app.engine.apps.get("tk-multi-nozmov")
        if not self.nozmov_app:
            self._app.log_info('ERROR: tk-multi-nozmov inside tk-houdini-deadlinenode problem: no preview movies will be created!')
        self.nozmov_preset = self._app.get_setting("Nozon Preview Movie Preset")


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

        # Sometimes shotgun does not have a task weirdly enough (bug maybe)
        if self._app.context.task:
            task_name = self._app.context.task['name']
            task_id = self._app.context.task['id']
        else:
            task_name = self._app.context.step['name']
            task_id = self._app.context.step['id']

        # get static data
        self._session_info = {
            'department': self._app.context.step['name'],
            'task_name': task_name,
            'task_id': task_id,
            'entity_name': self._app.context.entity['name'],
            'entity_id': self._app.context.entity['id'],
            'entity_type': self._app.context.entity['type'],
            'project_name': self._app.context.project['name'],
            'project_id': self._app.context.project['id'],
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

    def deadline_dependencies(self, node):   #### is this used somewhere in the OTL ??????????????????
        self._dl_node = node
        if 'DEADLINE_PATH' in os.environ:
            deadline_bin = os.path.join(os.environ['DEADLINE_PATH'], 'deadlinecommand')
            self._process.start(deadline_bin, ["-selectdependencies", node.parm('dl_dependencies').eval()])

    ############################################################################
    # Private methods
    
    def _dependecy_finished(self):
        output = self._process.readAllStandardOutput()
        output = str(output, 'UTF-8')
        output = output.rstrip()

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
                    
            if not render_node.isBypassed() and render_node.type().name() in ['sgtk_geometry', 'sgtk_arnold', 'sgtk_mantra', 'shotgrid_arnold_usd_rop']:
                if render_node.type().name() in ['sgtk_geometry', 'sgtk_arnold', 'shotgrid_arnold_usd_rop']: 
                    render_node.hm().pre_render(render_node)
                
                dep_id = self._submit_dl_job(render_node, dep_job_ids)
                self._dl_submitted_ids[render_node.path()] = [dep_id]
            else:
                self._dl_submitted_ids[render_node.path()] = dep_job_ids

    def _submit_dl_job(self, node, dependencies):

        export_job = False
        if node.type().name() in ["sgtk_arnold", "sgtk_mantra", "shotgrid_arnold_usd_rop"]:
            export_job = True

            if node.type().name() == "sgtk_arnold":
                # source_file = ass
                source_file = node.parm("sgtk_ass_diskfile").evalAtFrame(0)
                image_paths = [node.parm("ar_picture").unexpandedString()]

            if node.type().name() == "sgtk_mantra":
                source_file = node.parm("sgtk_soho_diskfile").evalAtFrame(0)
                image_paths = [node.parm("sgtk_vm_picture").unexpandedString()]

            if node.type().name() == "shotgrid_arnold_usd_rop":

                usdapp = self._app.engine.apps.get("tk-houdini-usdarnoldnode")
                source_file = usdapp.get_usd_output_path(node)
                image_paths = usdapp.get_image_output_paths(node)

        elif node.type().name() == "sgtk_geometry":
            output_file = node.parm("sopoutput").evalAsString()
            output_file = output_file.replace('$F4', '####')

        # Configure job name and version
        work_file_path = hou.hipFile.path()
        work_file_template = self._app.get_template("work_file_template")
        if (work_file_template and work_file_template.validate(work_file_path)):
            version = work_file_template.get_fields(work_file_path)['version']

        batch_name = 'Houdini - {} - {} - {} - v{}'.format(self._session_info['project_name'], self._session_info['entity_name'], self._session_info['task_name'], str(version).zfill(3))
        name = '{} - {}'.format(hou.getenv('HIPNAME'), node.path())
        
        # Override version by value stored in 'ver' parm
        if node.type().name() in ['sgtk_geometry', 'sgtk_arnold', 'shotgrid_arnold_usd_rop']:
            version = node.parm('ver').evalAsInt()
            name = '{} v{}'.format(name, str(version).zfill(3))

        # Different priority when it is a sim
        priority = self.TK_DEFAULT_GEO_PRIORITY
        if node.parm('initsim') and node.parm('initsim').evalAsInt():
            priority = self.TK_DEFAULT_SIM_PRIORITY
            if priority > 100:
                priority = 100

        # Create submission info file
        job_info_file = {
            "Plugin": "Houdini",
            "UserName": self._session_info['username'],
            "Name": name,
            "Department": self._session_info['department'],
            "Pool": self._session_info['pool'],
            "SecondaryPool": self._session_info['sec_pool'],
            "Group": self._session_info['group'],
            "Priority": priority,
            "IsFrameDependent": True,
            "MachineName": platform.node(),
            "BatchName": batch_name,
            "ExtraInfo0": self._session_info['task_name'],
            "ExtraInfo1": self._session_info['project_name'],
            "ExtraInfo2": self._session_info['entity_name'],
            "ExtraInfo3": version,
            "ExtraInfo4": "",
            "ExtraInfo5": self._session_info['username'],
            }

        # Nozon important env vars from the running Houdini to the job's env (donat)
        env_vars = ['RLM_LICENSE',
                    'SOLIDANGLE_LICENSE',
                    'ADSKFLEX_LICENSE_FILE',
                    'HOUDINI_OTLSCAN_PATH',
                    'PYTHONPATH']

        # generator for 'EnvironmentKeyValueXX'
        EnvironmentKeyValueJob = self.EnvironmentKeyValueGenerator()
        # LOOP THROUGH THE VARS AND ADD A LINE TO THE JOB FILE FOR EACH DEFINED ENVIRONMENT VARIABLE.
        for env_var in env_vars:
            #CHECK THAT THE ENVIRONMENT VARIABLE HAS BEEN SET (WE DON'T WANT TO PASS 'NONE' TYPE VALUES TO DEADLINE AS THIS CAN CAUSE PROBLEMS)
            if os.environ.get(env_var) is not None:
                job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % (env_var, os.environ.get(env_var))
        # Shotgun location
        if os.environ.get("TANK_CURRENT_PC") is not None:
            job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("NOZ_TK_CONFIG_PATH", os.environ.get("TANK_CURRENT_PC"))
        # getting rid of tk-houdini in the Houdini_Path env var
        if os.environ.get("HOUDINI_PATH") is not None:
            hou_path = os.environ.get("HOUDINI_PATH")
            hou_path_list = hou_path.split(";")
            hou_path_list_no_sgtk = [x for x in hou_path_list if 'tk-houdini' not in x]
            hou_path_no_sgtk = ";".join(hou_path_list_no_sgtk)
            job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("HOUDINI_PATH", hou_path_no_sgtk)
        # parse the PATH var to only take houdini stuff
        if os.environ.get("PATH") is not None: 
            e_path = os.environ.get("PATH")
            e_path_list = e_path.split(";")
            # we might miss stuff here !!!
            e_path_hou_only_list = [x for x in e_path_list if 'houdini' in x.lower()]
            e_path_hou_only = ";".join(e_path_hou_only_list)
            job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("PATH", e_path_hou_only)
        job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("HOUDINI_PACKAGE_DIR", os.environ.get("HOUDINI_PACKAGE_DIR"))
        job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("NOZ_HIPFILE", hou.hipFile.path())
        job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("context", self._app.context.serialize(with_user_credentials=False, use_json=True))
        job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("OCIO", hou.Color.ocio_configPath())


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
            if node.parm('types').evalAsString() == 'usd' and node.parm('usdSingleFile').evalAsInt(): ############### ????
                chunk_size = frame_range[1] - frame_range[0] + 1
        elif node.type().name() == 'shotgrid_arnold_usd_rop':
            if not node.parm("fileperframe").eval():
                chunk_size = frame_range[1] - frame_range[0] + 1


        job_info_file['ChunkSize'] = chunk_size
        
        # output
        if export_job:
            job_info_file['OutputDirectory0'] = os.path.dirname(source_file)
        else:
            job_info_file['OutputFilename0'] = output_file

        hou_ver = hou.applicationVersion()
        plugin_info_file = {
            "Version": "%s.%s" % (hou_ver[0], hou_ver[1]),
            "IgnoreInputs": True,
            "OutputDriver": node.path(),
            "Build": "None",
        }

        if node.type().name() == 'sgtk_geometry':
            path = node.hm().app().handler.get_backup_file(node)
            plugin_info_file["SceneFile"] = path
        else:
            plugin_info_file["SceneFile"] = hou.hipFile.path()

        # submit to deadline
        render_job_id = self._deadline_connect.Jobs.SubmitJob(job_info_file, plugin_info_file)["_id"]

        # export job
        if export_job:

            # generator for 'ExtraInfoKeyValueXX'
            ExtraInfoKeyValueExportJob = self.ExtraInfoKeyValueGenerator()

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
                "MachineName": platform.node(),
                "Frames": "%s-%s" % (frame_range[0], frame_range[1]),
                "ChunkSize": 1,
                "BatchName": batch_name,
                "ExtraInfo0": self._session_info['task_name'],
                "ExtraInfo1": self._session_info['project_name'],
                "ExtraInfo2": self._session_info['entity_name'],
                "ExtraInfo3": "{} - {} v{} - {}".format(self._session_info['entity_name'], self._session_info['task_name'], str(version).zfill(3), self._session_info['username']),
                "ExtraInfo4": "",
                "ExtraInfo5": self._session_info['username'],
                next(ExtraInfoKeyValueExportJob): "ProjectDirectory=%s" % os.path.basename(self._app.sgtk.pipeline_configuration.get_path()),
                }

            for index, image_path in enumerate(image_paths):
                image_path = image_path.replace("%04d", "####")
                export_job_info_file["OutputFilename{}".format(index)] = os.path.basename(image_path)  
                export_job_info_file["OutputDirectory{}".format(index)] = os.path.dirname(image_path)  

            # Add extra values for renders to enable post jobs on deadline
            if node.type().name() in ['sgtk_geometry', 'sgtk_arnold', 'shotgrid_arnold_usd_rop']:
                publish_info = {'name': name,
                                'published_file_type': 'Rendered Image', 
                                'version_number': version, 
                                'comment': '',
                                'dependency_paths': [], 
                                'dependency_ids': []
                                }
                publish_info = json.dumps(publish_info)

                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "context=%s" % self._app.context.serialize(with_user_credentials=False, use_json=True)
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "PublishInfo=%s" % publish_info
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "ProjectScriptFolder=%s" % os.path.join(self._app.sgtk.pipeline_configuration.get_config_location(), "hooks", "tk-multi-publish2", "nozonpub")
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "EntityType=%s" % self._session_info['entity_type']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "EntityId=%s" % self._session_info['entity_id']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "EntityName=%s" % self._session_info['entity_name']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "ProjectId=%i" % self._app.context.project['id']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "TaskId=%i" % self._session_info['task_id']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "FrameRate=%s" % hou.fps()
                

                if self.nozmov_app:
                    export_job_info_file["EventOptIns"] = "NozMov2EventPlugin"
                    movie_path = self.nozmov_app.calc_output_filepath(image_paths[0], self.nozmov_preset, publish_hook=self._app)
                    nozmov = [ {"preset_name":self.nozmov_preset, "path": movie_path, "first_frame": frame_range[0],
                                            "last_frame": frame_range[1], "upload": True, "add_audio": None} ]
                    nozmovs = {"NozMov0": nozmov}
                    nozmovs = json.dumps(nozmovs)
                    #### Note : leaving colorspace information undefined
                    # TODO : find if there's a way in Houdini to find the render colorspace of the hip
                    export_job_info_file[next(ExtraInfoKeyValueExportJob)] = f"NozMovs={nozmovs}"
                    export_job_info_file[next(ExtraInfoKeyValueExportJob)] = f"NozMovDeadlineEventScript={self.nozmov_app.get_setting('deadline_event_script')}"
                    export_job_info_file[next(ExtraInfoKeyValueExportJob)] = f"NozMovDeadlinePluginScript={self.nozmov_app.get_setting('deadline_plugin_script')}"


            if "sgtk_mantra" in node.type().name():
                export_job_info_file["Plugin"] = "Mantra"
            elif "sgtk_arnold" in node.type().name():
                export_job_info_file["Plugin"] = "Arnold"
            elif "shotgrid_arnold_usd_rop" in node.type().name():
                export_job_info_file["Plugin"] = "ArnoldUSD"

            # Shotgun location
            export_job_info_file["EnvironmentKeyValue0"] = "NOZ_TK_CONFIG_PATH=%s" % self._app.sgtk.pipeline_configuration.get_path()

            # plugin info file
            export_plugin_info_file = { "CommandLineOptions": "" }
            if node.type().name() == "sgtk_mantra":
                export_plugin_info_file["SceneFile"] = source_file
                major_version, minor_version = hou.applicationVersion()[:2]
                export_plugin_info_file["Version"] = "%s.%s" % (major_version, minor_version)
            elif node.type().name() in ["sgtk_arnold", "shotgrid_arnold_usd_rop"]:
                export_plugin_info_file["InputFile"] = source_file
                export_plugin_info_file["Verbose"] = 4

            export_job_id = self._deadline_connect.Jobs.SubmitJob(export_job_info_file, export_plugin_info_file)["_id"]

            return export_job_id
        return render_job_id
    
    def _get_frame_range(self, node):
        if node.parm('trange').evalAsString() == 'normal': # = 'Render Specific Frame Range'
            node_range = node.parmTuple('f').evalAsFloats()
            return (int(node_range[0]), int(node_range[1]), int(node_range[2]))
        elif node.parm('trange').evalAsString() == 'off': # = 'Render Current Frame'
            curr_frame = int(hou.frame())
            return (curr_frame, curr_frame, 1)
        else:
            return (0, 0, 1)


    def ExtraInfoKeyValueGenerator(self):
        # Utility function to generate ExtraInfoKeyValueX strings
        count = 0
        while True:
            yield "ExtraInfoKeyValue{:d}".format(count)
            count += 1

    def EnvironmentKeyValueGenerator(self):
        # Utility function to generate ExtraInfoKeyValueX strings
        count = 0
        while True:
            yield "EnvironmentKeyValue{:d}".format(count)
            count += 1