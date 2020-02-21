# Copyright (c) 2015 Shotgun Software Inc.
# 
# CONFIDENTIAL AND PROPRIETARY
# 
# This work is provided "AS IS" and subject to the Shotgun Pipeline Toolkit 
# Source Code License included in this distribution package. See LICENSE.
# By accessing, using, copying or modifying this work you indicate your 
# agreement to the Shotgun Pipeline Toolkit Source Code License. All rights 
# not expressly granted therein are reserved by Shotgun Software Inc.

"""
Deadline Output node App for use with Toolkit's Houdini engine.
"""

import sgtk


class TkDeadlineNodeApp(sgtk.platform.Application):
    """The Deadline Output Node."""

    def init_app(self):
        """Initialize the app."""

        tk_houdini_deadline = self.import_module("tk_houdini_deadlinenode")
        self.handler = tk_houdini_deadline.TkDeadlineNodeHandler(self)
