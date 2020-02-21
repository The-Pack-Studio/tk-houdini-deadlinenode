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

    def SubmitToDeadline(self, node):
        self._app.log_info('Submit to deadline not implemented yet!')

    ############################################################################
    # Private methods
            
    # extract fields from current Houdini file using the workfile template
    def _get_hipfile_fields(self):
        work_file_path = hou.hipFile.path()

        work_fields = {}
        work_file_template = self._app.get_template("work_file_template")
        if (work_file_template and 
            work_file_template.validate(work_file_path)):
            work_fields = work_file_template.get_fields(work_file_path)

        return work_fields