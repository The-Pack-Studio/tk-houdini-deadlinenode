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
        accept_types = ['sgtk_geometry', 'sgtk_mantra', 'sgtk_arnold']
        dep_job_ids = []
        
        if not render_node.isLocked():
            dep_nodes = render_node.inputs()
            for dep_node in dep_nodes:
                if dep_node.path() not in self._dl_submitted_ids.keys():
                    self._submit_node_tree_lookup(dep_node)
                    
                dep_job_ids.extend(self._dl_submitted_ids[dep_node.path()])
                
        if not render_node.isBypassed() and render_node.type().name() in accept_types:
            if render_node.type().name() == 'sgtk_geometry':
                render_node.hm().pre_render(render_node)
           
            self._dl_submitted_ids[render_node.path()] = self._submit_render_job(render_node, dep_job_ids)
        else:
            self._dl_submitted_ids[render_node.path()] = dep_job_ids

    def _submit_render_job(self, node, dependencies):
        self._app.log_info(node.path())
        self._app.log_info('Dependencies {}'.format(dependencies))
        return [node.path()]

    # extract fields from current Houdini file using the workfile template
    def _get_hipfile_fields(self):
        work_file_path = hou.hipFile.path()

        work_fields = {}
        work_file_template = self._app.get_template("work_file_template")
        if (work_file_template and 
            work_file_template.validate(work_file_path)):
            work_fields = work_file_template.get_fields(work_file_path)

        return work_fields