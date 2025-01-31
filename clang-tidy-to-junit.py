#!/usr/bin/env python3
#
# MIT License
#
# Copyright (c) 2018 PSPDFKit
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import sys
import collections
import re
import logging
import itertools
from xml.sax.saxutils import escape

# Create a `ErrorDescription` tuple with all the information we want to keep.
ErrorDescription = collections.namedtuple(
    'ErrorDescription', 'file line column error error_identifier description')


class ClangTidyConverter:
    # All the errors encountered.
    errors = []

    # Parses the error.
    # Group 1: file path
    # Group 2: line
    # Group 3: column
    # Group 4: error message
    # Group 5: error identifier
    error_regex = re.compile(
        r"^([\w\/\.\-\ ]+):(\d+):(\d+): (.+) (\[[\w\-,\.]+\])$")

    # This identifies the main error line (it has a [the-warning-type] at the end)
    # We only create a new error when we encounter one of those.
    main_error_identifier = re.compile(r'\[[\w\-,\.]+\]$')

    def __init__(self, basename):
        self.basename = basename

    # <?xml version="1.0" encoding="utf-8"?>
    # <testsuites>
    #   <testsuite errors="0" failures="0" hostname="B-12D5Q05P-2006.local" name="pytest" skipped="0" tests="1" time="0.108" timestamp="2022-04-13T20:07:10.936335">
    #       <testcase classname="tmp.demo.test.test_sample" file="tmp/demo/test/test_sample.py" line="5" name="test_answer" time="0.002">
    #       </testcase>
    #   </testsuite>
    # </testsuites>
    def print_junit_file(self, output_file):
        # Write the header.
        output_file.write("""<?xml version="1.0" encoding="UTF-8" ?>
<testsuites id="1" name="Clang-Tidy" tests="{error_count}" errors="{error_count}" failures="0" time="0">""".format(
            error_count=len(self.errors)))

        sorted_errors = sorted(self.errors, key=lambda x: x.file)

        # Iterate through the errors, grouped by file.
        for file, errorIterator in itertools.groupby(sorted_errors, key=lambda x: x.file):
            errors = list(errorIterator)
            error_count = len(errors)

            # Each file gets a test-suite
            output_file.write(
                """\n    <testsuite errors="{error_count}" failures="0" name="{file}" tests="{error_count}" time="0">\n"""
                .format(error_count=error_count, file=file))
            for error in errors:
                filename = str(error.file).replace("/home/jenkins/agent/aci/codeWorkspace/", "")
                # Write each error as a test case.
                output_file.write("""
        <testcase id="{id}" classname="{identifier}" file="{file}" line="{line}" name="{name}:{line}" time="0">
            <failure message="{message}">
{htmldata}
            </failure>
        </testcase>""".format(id="[{}/{}] {}".format(error.line, error.column, error.error_identifier),
                              identifier=error.error_identifier, file=filename, line=error.line,
                              name=filename,
                              message=escape(error.error, entities={"\"": "&quot;"}),
                              htmldata=escape(error.description)))
            output_file.write("\n    </testsuite>\n")
        output_file.write("</testsuites>\n")

    def process_error(self, error_array):
        if len(error_array) == 0:
            return

        result = self.error_regex.match(error_array[0])
        if result is None:
            logging.warning(
                'Could not match error_array to regex: %s', error_array)
            return

        # We remove the `basename` from the `file_path` to make prettier filenames in the JUnit file.
        file_path = result.group(1).replace(self.basename, "")
        error = ErrorDescription(file_path, int(result.group(2)), int(
            result.group(3)), result.group(4), result.group(5), "\n".join(error_array[1:]))
        self.errors.append(error)

    def convert(self, output_file):

        input_file = open(file=self.basename, mode='r')
        # Collect all lines related to one error.
        current_error = []
        for line in input_file:
            # If the line starts with a `/`, it is a line about a file.
            if line[0] == '/':
                # Look if it is the start of a error
                if self.main_error_identifier.search(line, re.M):
                    # If so, process any `current_error` we might have
                    self.process_error(current_error)
                    # Initialize `current_error` with the first line of the error.
                    current_error = [line]
                else:
                    # Otherwise, append the line to the error.
                    current_error.append(line)
            elif len(current_error) > 0:
                # If the line didn't start with a `/` and we have a `current_error`, we simply append
                # the line as additional information.
                current_error.append(line)
            else:
                pass

        # If we still have any current_error after we read all the lines,
        # process it.
        if len(current_error) > 0:
            self.process_error(current_error)

        # Print the junit file.
        self.print_junit_file(output_file)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Usage: %s base-filename-path", sys.argv[0])
        logging.error(
            "  base-filename-path: Removed from the filenames to make nicer paths.")
        sys.exit(1)
    converter = ClangTidyConverter(sys.argv[1])
    converter.convert(sys.stdout)
