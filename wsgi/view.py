#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2012-2013, Anders Lowinger, Abundo AB
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the <organization> nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
Template engine
"""

import os
import sys
import io
import collections

import wsgi.common


class Tokenizer:
    def __init__(self, filename):
        self.f = open(filename, "r")
        self.line = ""
        self.line_row = 0
        self.line_col = 0

        self._unget = ""

    def get_char(self):
        if self._unget != "":
            c = self._unget[0]
            self._unget = self._unget[1:]
            return c
        while True:
            if self.line_col < len(self.line):
                c = self.line[self.line_col]
                self.line_col += 1
                return c
            self.line = self.f.readline()
            self.line_row += 1
            if self.line is None or self.line == "":
                return None
            self.line_col = 0

    def get_chars(self, count, skipspace=True):
        if skipspace:
            self.skip_space()
        s = ""
        for i in range(0, count):
            s += self.get_char()
        return s

    def peek_char(self):
        c = self.get_char()
        if c is None:
            return None
        self.unget(c)
        return c

    def unget(self, s):
        self._unget = s + self._unget

    def skip_space(self):
        while True:
            c = self.get_char()
            if c == " ":
                continue
            self.unget(c)
            return

    def get_token(self):
        self.skip_space()
        token = ""
        while True:
            c = self.get_char()
            if c is not None:
                if c.isalnum():
                    token += c
                else:
                    self.unget(c)
                    break
        return token

    def begins_with(self, token):
        s = self.get_chars(len(token))
        if s == token:
            return True
        self.unget(s)

    def expect_token(self, token):
        s = self.get_chars(len(token))
        if s == token:
            return
        self.error("Expected %s" % token, 403)

    def get_until(self, token):
        s = ""
        while True:
            c = self.get_char()
            if c is None:
                raise wsgi.common.WsgiError("Missing token %s" % token)
            s += c
            if s.endswith(token):
                return s[:-len(token)]

    def get_string(self):
        self.skip_space()
        s = ""
        start_c = self.get_char()
        if start_c != "'" and start_c != '"':
            self.error("Missing quote \" or ' at start of string", 403)
        while True:
            c = self.get_char()
            if c is None:
                self.error("Missing quote %s at end of string" % start_c, 403)

            if c == start_c:
                # check if end of string?
                if self.peek_char() == start_c:
                    # just a quoted quote character, continue
                    s += self.get_char()
                    continue
                break
            s += c
        return s

    def error(self, msg, errcode=403):
        print("Compile error line number %d" % self.line_row)
        print("line: %s" % self.line, end="")
        print("  at: %s^" % (" " * self.line_col))
        raise wsgi.common.WsgiError(msg, errcode)


class CompileView:
    def __init__(self):
        self.blocks = collections.OrderedDict()     # key is block name
        self.block = None                           # current block
        self.extends = ""
        self.level = 0  # recursion depth

    def add_block(self, name):
        self.block = io.StringIO()
        self.blocks[name] = self.block

    def out_text(self, s):
        # write
        if not self.in_print:
            self.block.write("    " * self.indent)
            self.block.write('out("')
            self.in_print = True
        for c in s:
            if c == '"':
                self.block.write("\\")
            self.block.write(c)

    def end_out_text(self):
        if self.in_print:
            self.block.write("\")\n")
            self.in_print = False

    def out_python(self, s):
        # write python code to output
        self.end_out_text()
        self.block.write("    " * self.indent)
        self.block.write(s)

    def error(self, msg, errcode=403):
        print("Compile error line number %d" % self.line_no)
        print("line: %s" % self.line, end="")
        print("  at: %s^" % (" " * self.ix))
        raise wsgi.common.WsgiError(msg, errcode)

    def compile_block(self, blockname):
        """
        Note: reentrant method
        Each block in the dynamic page is a separate method. This makes it
        possible to override and/or call the inherited method
        """
        if blockname:
            log.debug("  Compiling block %s" % blockname)
        else:
            log.debug("  Compiling block <default>")
        self.add_block(blockname)
        self.indent = 1
        self.in_print = False
        self.level += 1
        while True:
            c = self.tokenizer.get_char()
            if c is None:
                break
            if c == "{":
                c = self.tokenizer.get_char()
                if c == "%":
                    # python code, or command
                    token = self.tokenizer.get_token()
                    if token == "extends":
                        # we inherit from another class
                        # todo, what to do if we already found an extend ?
                        self.extends = self.tokenizer.get_string()
                        self.tokenizer.expect_token("%}")
                        log.debug("  extends %s" % self.extends)
                        cv = CompileView()
                        cv.compileFile(self.extends)
                        log.debug("  Done compiling view '%s'" % self.extends)
                        continue
                    elif token == "block":
                        # compile a named block
                        blockname = self.tokenizer.get_string()
                        self.tokenizer.expect_token("%}")
                        self.out_python("self.block_%s()\n" % blockname)
                        self.end_out_text()
                        indent = self.indent
                        block = self.block
                        self.compile_block(blockname)   # warning, recursion
                        self.indent = indent
                        self.block = block
                        continue
                    elif token == "endblock":
                        # we are done with this block
                        # todo, check if we are inside a block?
                        # log.debug("endblock")
                        self.tokenizer.expect_token("%}")
                        self.end_out_text()
                        self.level -= 1
                        if self.level > 0:
                            return
                        continue
                    elif token == "end":
                        # end of indent
                        # log.debug("end")
                        if self.indent < 2:
                            raise wsgi.common.WsgiError("Too many 'end', no matching 'begin' (colon)", 403)
                        self.indent -= 1
                        self.tokenizer.expect_token("%}")
                        self.end_out_text()
                        continue
                    # assume everything else is python code, just copy
                    # if it ends with a colon, increase indent
                    self.tokenizer.unget(token)
                    lines = self.tokenizer.get_until("%}")
                    for line in lines.split("\n"):
                        line = line.strip()
                        self.out_python(line)
                        self.out_python("\n")
                        self.end_out_text()
                        if line and line[-1] == ":":
                            self.indent += 1
                    continue
                elif c == "{":
                    # expression, print it
                    line = self.tokenizer.get_until("}}")
                    line = line.strip()
                    self.out_python("out_safe(%s)\n" % line)
                else:
                    self.out_text("{")
                    self.out_text(c)
            else:
                if c == "\n":
                    self.out_text("\\n")
                    self.end_out_text()
                else:
                    self.out_text(c)
        self.end_out_text()

    def save(self, module_name, module_file):
        """
        Save the compiled template to disk
        """
        self.module_file = open(module_file, "w")
        f = self.module_file   # less typing
        f.write("#!/usr/bin/env python3\n")
        f.write("\n")
        f.write("# This file is generated, do not edit changes will be lost\n")
        f.write("\n")
        f.write("import sys\n")
        f.write("import html\n")
        f.write("\n")
        f.write("def out(msg):\n")
        f.write("    if msg: sys.stdout.write( str(msg) )\n")
        f.write("def out_safe(msg):\n")
        f.write("    if msg: sys.stdout.write( html.escape( str(msg) ) )\n")
        f.write("\n")
        tmp = ""
        if self.extends:
            tmp = "%s" % os.path.splitext(self.extends)[0].replace("/", ".")
            import_cls = tmp.rsplit(".", 1)
            # import_cls[-1] = import_cls[-1].capitalize()
            f.write("import %s\n" % ".".join(import_cls))
            f.write("\n")
        f.write("class %s" % (module_name.capitalize()))
        if tmp:
            f.write("(%s.%s)" % (tmp, import_cls[-1].capitalize()))
        f.write(":\n")
        f.write("\n")
        
        f.write("    def __init__(self, **kwargs):\n")
        f.write("        for key in kwargs.keys():\n")
        f.write("            globals()[key]=kwargs[key]\n")
        if self.extends:
            f.write("        super().__init__(**kwargs)\n")
        else:
            f.write("        super().__init__()\n")
        f.write("\n")

        if self.extends == "":
            # todo, only call blocks which has not been called by
            # super() constructor
            tmp = self.blocks[""].getvalue()
            if tmp:
                for line in tmp.split("\n"):
                    f.write("    %s\n" % line)
            else:
                f.write("        pass\n")
            f.write("\n")

        for name, blockio in self.blocks.items():
            if name == "":
                continue
            f.write("    def block_%s(self):\n" % name)
            tmp = blockio.getvalue()
            if tmp:
                for line in tmp.split("\n"):
                    f.write("    %s\n" % line)
            else:
                f.write("        pass\n")

        f.write("if __name__ == '__main__':\n")
        f.write("    %s()\n" % module_name)
        f.write("\n")
        # f.write("sys.modules[__name__] = %s()\n" % module_name.capitalize())
        self.module_file.close()

    def compileFile(self, view_filename_rel, **kwargs):
        """
        Compile a .html file -> a python module
        """

        view_filename = "%s/%s" % (app.view_dir, view_filename_rel)

        if not os.path.exists(view_filename):
            raise wsgi.common.WsgiError("No such view %s" % view_filename, 404)

        module_dir, module_name = os.path.split(view_filename_rel)
        module_name = os.path.splitext(module_name)[0]
        module_file = os.path.join(app.view_code_dir, module_dir, module_name) + ".py"
        module_full_dir = os.path.dirname(module_file)
        if not os.path.exists(module_full_dir):
            os.makedirs(module_full_dir)

        if not app._reload:
            if os.path.exists(module_file):
                if os.path.getmtime(view_filename) <= os.path.getmtime(module_file):
                    return module_name, module_file

        # no module_name exist, or it is too old
        # compile the view to a python module

        log.debug("Compiling view '%s' to module '%s'" %
                  (view_filename, module_file))
        self.blocks.clear()

        self.tokenizer = Tokenizer(view_filename)
        self.line_no = 0
        self.compile_block("")

        self.save(module_name, module_file)

        return module_name, module_file


def render(view_filename, request, response, **kwargs):
    """
    Compile the view to a python module
    Import and run the module to generate output
    """
    log.debug("view.render(view_filename='%s')" % (view_filename))
    compile_view = CompileView()
    module_name, module_file = compile_view.compileFile(view_filename, **kwargs)

    # We now have a python module, import it
    # We add the view_code_dir as the first entry in pythonpath,
    # which makes it possible for compiled modules to find each other
    with wsgi.common.AddSysPath(app.view_code_dir):
        mod = wsgi.common.importFile(module_file)
    mod.log = log
    mod.db = app.db

    cls = getattr(mod, module_name.capitalize())
    
    # we ignore the object instance
    cls(request=request, response=response, **kwargs)


if __name__ == "__main__":
    global log
    import logging as log
    compile_view = CompileView()
    module_name, module_name_with_path = compile_view.compileFile(sys.argv[1])
