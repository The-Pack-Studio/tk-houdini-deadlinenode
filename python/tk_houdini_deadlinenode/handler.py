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
from datetime import datetime

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

        self.deadline_houdini_group = self._app.get_setting("deadline_houdini_group")
        self.deadline_arnold_group = self._app.get_setting("deadline_arnold_group")
        self.deadline_husk_group = self._app.get_setting("deadline_husk_group")

        # Create a pool with the name of the project
        self.project_pool = self._app.context.project['name'].replace(" ", "_")
        if not self.project_pool in self._deadline_connect.Pools.GetPoolNames():
            pool_create = self._deadline_connect.Pools.AddPool(self.project_pool)
            if pool_create == "Success":
                self._app.log_debug(f"Created new deadline pool: {self.project_pool}")
        self.secondary_pool = fw.default_pool()    

        self._deadline_houdini_render_script  = self._app.get_setting("deadline_houdini_render_script")

        self.local_rendering = self._app.get_setting("local_rendering")

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


    def submit_to_deadline(self, node):

        username = "unknownuser"
        userInfo = sgtk.util.get_current_user(self._app.sgtk)
        if userInfo:
            username = userInfo["login"]

        self._dl_node = node

        # Sometimes shotgun does not have a task weirdly enough (bug maybe)
        if self._app.context.task:
            task_name = self._app.context.task['name']
            task_id = self._app.context.task['id']
        else:
            hou.ui.setStatusMessage("No task in the context of the deadlinenode app! Aborting submit", hou.severityType.Error)
            self._app.log_error('No task in the apps context : %s' % self._app.context.to_dict() )

            return None

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
            'chunk_size': self._dl_node.parm('dl_chunk_size').evalAsString(),
            'username': username,
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
                image_path = node.parm("ar_picture").unexpandedString()
                image_path = image_path.replace('$F4', '####')
                image_paths = [image_path]

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
        hip_name, version = None, None
        work_file_path = hou.hipFile.path()
        work_file_template = self._app.get_template("work_file_template")
        if (work_file_template and work_file_template.validate(work_file_path)):
            work_fields = work_file_template.get_fields(work_file_path)
            hip_name = work_fields.get('name')
            version = work_fields.get('version')

        batch_name_elements = [ self._session_info['project_name'],
                                self._session_info['entity_name'],
                                self._session_info['task_name'],
                                hip_name,
                                'v{:03d}'.format(version),
                                "({})".format(datetime.today().strftime("%Y-%m-%d")),
                                    ]

        batch_name = "Houdini " + " - ".join([x for x in batch_name_elements if x])

        name = '{} - {}'.format(hou.getenv('HIPNAME'), node.path())
        
        # Override version by value stored in 'ver' parm
        if node.type().name() in ['sgtk_geometry', 'sgtk_arnold', 'shotgrid_arnold_usd_rop']:
            name = '{} v{:03d}'.format(name, node.parm('ver').evalAsInt())

        version_info_elements = [   self._session_info['entity_name'],
                                    self._session_info['task_name'],
                                    hip_name,
                                    node.name(),
                                    'v{:03d}'.format(node.parm('ver').evalAsInt()),
                                        ]

        version_info = " ".join([x for x in version_info_elements if x])


        # Different priority when it is a sim
        priority = self.TK_DEFAULT_GEO_PRIORITY
        if node.parm('initsim') and node.parm('initsim').evalAsInt():
            priority = self.TK_DEFAULT_SIM_PRIORITY
            if priority > 100:
                priority = 100

        # Create submission info file
        job_info_file = {
            "Plugin": "Houdini2",
            "UserName": self._session_info['username'],
            "Name": name,
            "Department": self._session_info['department'],
            "Pool": self.project_pool,
            "SecondaryPool": self.secondary_pool,
            "Group": self.deadline_houdini_group,
            "Priority": priority,
            "IsFrameDependent": True,
            "MachineName": platform.node(),
            "BatchName": batch_name,
            "ExtraInfo0": self._session_info['task_name'],
            "ExtraInfo1": self._session_info['project_name'],
            "ExtraInfo2": self._session_info['entity_name'],
            "ExtraInfo3": version_info,
            "ExtraInfo4": "",
            "ExtraInfo5": self._session_info['username'],
            }


        # generator for 'EnvironmentKeyValueXX'
        EnvironmentKeyValueJob = self.EnvironmentKeyValueGenerator()
        # LOOP THROUGH THE VARS AND ADD A LINE TO THE JOB FILE FOR EACH DEFINED ENVIRONMENT VARIABLE.
        for env_var in ['ADSKFLEX_LICENSE_FILE', 'HOUDINI_OTLSCAN_PATH', 'PYTHONPATH']:
            #CHECK THAT THE ENVIRONMENT VARIABLE HAS BEEN SET (WE DON'T WANT TO PASS 'NONE' TYPE VALUES TO DEADLINE AS THIS CAN CAUSE PROBLEMS)
            if os.environ.get(env_var) is not None:
                job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % (env_var, os.environ.get(env_var))
        # Shotgun location
        if os.environ.get("TANK_CURRENT_PC") is not None:
            job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("NOZ_TK_CONFIG_PATH", os.environ.get("TANK_CURRENT_PC"))
        # getting rid of tk-houdini in the Houdini_Path env var
        if os.environ.get("HOUDINI_PATH") is not None:
            hou_path = os.environ.get("HOUDINI_PATH")
            hou_path_list = hou_path.split(os.pathsep)
            hou_path_list_no_sgtk = [x for x in hou_path_list if 'tk-houdini' not in x]
            hou_path_no_sgtk = os.pathsep.join(hou_path_list_no_sgtk)
            job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("HOUDINI_PATH", hou_path_no_sgtk)
        # parse the PATH var to only take houdini stuff
        if os.environ.get("PATH") is not None: 
            e_path = os.environ.get("PATH")
            e_path_list = e_path.split(os.pathsep)
            # we might miss stuff here !!!
            e_path_hou_only_list = [x for x in e_path_list if 'houdini' in x.lower()]
            e_path_hou_only = os.pathsep.join(e_path_hou_only_list)
            job_info_file[next(EnvironmentKeyValueJob)] = "%s=%s" % ("PATH", e_path_hou_only)
        # Shotgrid global debug flag
        if os.environ.get("TK_DEBUG") != None:
            job_info_file[next(EnvironmentKeyValueJob)] = "{}={}".format("TK_DEBUG", os.environ.get("TK_DEBUG"))
        # Usd asset resolver variables
        if os.environ.get("USD_ASSET_RESOLVER") != None:
            job_info_file[next(EnvironmentKeyValueJob)] = "{}={}".format("USD_ASSET_RESOLVER", os.environ.get("USD_ASSET_RESOLVER"))
        if os.environ.get("PXR_PLUGINPATH_NAME") != None:
            job_info_file[next(EnvironmentKeyValueJob)] = "{}={}".format("PXR_PLUGINPATH_NAME", os.environ.get("PXR_PLUGINPATH_NAME"))

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
            usdnode = node.node('usd')
            if not usdnode.parm("fileperframe").eval():
                chunk_size = frame_range[1] - frame_range[0] + 1


        job_info_file['ChunkSize'] = chunk_size
        
        # output
        if export_job:
            job_info_file['OutputDirectory0'] = os.path.dirname(source_file)
        else:
            job_info_file['OutputFilename0'] = output_file

        hou_ver = hou.applicationVersion()
        plugin_info_file = {
            "Version": "%s.%s.%s" % (hou_ver[0], hou_ver[1], hou_ver[2]),
            "IgnoreInputs": True,
            "OutputDriver": node.path(),
            "Build": "None",
            "LocalRendering": self.local_rendering,
        }

        if node.type().name() in ['sgtk_geometry', 'sgtk_arnold', 'shotgrid_arnold_usd_rop']:
            path = node.hm().app().handler.get_backup_file(node)
            plugin_info_file["SceneFile"] = path
        else: 
            plugin_info_file["SceneFile"] = hou.hipFile.path()

        plugin_info_file["Script"] = self._deadline_houdini_render_script

        # submit to deadline
        render_job_id = self._deadline_connect.Jobs.SubmitJob(job_info_file, plugin_info_file)["_id"]

        # export job
        if export_job:

            group = None
            if node.type().name() == 'sgtk_arnold':
                group = self.deadline_arnold_group
            elif node.type().name() == 'shotgrid_arnold_usd_rop':
                lop_node_path = node.parm('loppath').eval()
                lop_node = hou.node(lop_node_path)
                if lop_node.parm('render_using'):
                    render_using = lop_node.parm('render_using').evalAsString()
                    if render_using == 'use_husk':
                        group = self.deadline_husk_group
                else:
                    group = self.deadline_arnold_group


            # generator for 'ExtraInfoKeyValueXX'
            ExtraInfoKeyValueExportJob = self.ExtraInfoKeyValueGenerator()

            # job info file
            export_job_info_file = {
                "Name": name,
                "UserName": self._session_info['username'],
                "Department": self._session_info['department'],
                "Pool": self.project_pool,
                "SecondaryPool": self.secondary_pool,
                "Group": group,
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
                "ExtraInfo3": version_info,
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
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "ProjectName=%s" % self._app.context.project['name']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "EntityType=%s" % self._session_info['entity_type']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "EntityId=%s" % self._session_info['entity_id']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "EntityName=%s" % self._session_info['entity_name']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "ProjectId=%i" % self._app.context.project['id']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "TaskId=%i" % self._session_info['task_id']
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "FrameRate=%s" % hou.fps()
                export_job_info_file[next(ExtraInfoKeyValueExportJob)] = "VersionName=%s" % version_info

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
                lop_node_path = node.parm('loppath').eval()
                lop_node = hou.node(lop_node_path)
                if lop_node.parm('render_using'):
                    render_using = lop_node.parm('render_using').evalAsString()
                    if render_using == 'use_husk':
                        export_job_info_file["Plugin"] = "Husk"
                else:
                    export_job_info_file["Plugin"] = "ArnoldUSD"


            # Environment Variables on export job
            # Copying all the env vars from the main job to the export job :
            # mandatory for husk jobs although for the other types of job only NOZ_TK_CONFIG_PATH is strictly necessary
            ExEnvKeyValueJob = self.EnvironmentKeyValueGenerator()
            for job_key, job_value in job_info_file.items():
                if "EnvironmentKeyValue" in job_key:
                    export_job_info_file[next(ExEnvKeyValueJob)] = job_value


            # plugin info file
            export_plugin_info_file = {}

            major_version, minor_version, build_version = hou.applicationVersion()

            if node.type().name() == "sgtk_mantra":
                export_plugin_info_file["SceneFile"] = source_file
                export_plugin_info_file["Version"] = "%s.%s" % (major_version, minor_version)

            elif node.type().name() ==  "sgtk_arnold":
                export_plugin_info_file["InputFile"] = source_file
                export_plugin_info_file["Verbose"] = 4

            elif node.type().name() == "shotgrid_arnold_usd_rop":
                export_plugin_info_file["InputFile"] = source_file
                export_plugin_info_file["Version"] = "%s.%s.%s" % (major_version, minor_version, build_version)
                export_plugin_info_file["Verbose"] = 2
                export_plugin_info_file["Renderer"] = "Arnold"
                export_plugin_info_file["ResolverContextFile"] = source_file

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