# -*- coding: utf-8 -*-
import re
import glob
import os

from pyparsing import (
    Regex, Keyword, Literal, White, Word, alphanums, CharsNotIn, Forward, Group,
    Optional, OneOrMore, ZeroOrMore, pythonStyleComment, lineno, LineStart, LineEnd
)

from amplify.agent.context import context
from amplify.agent.util.escape import prep_raw


__author__ = "Mike Belov"
__copyright__ = "Copyright (C) Nginx, Inc. All rights reserved."
__credits__ = ["Mike Belov", "Andrei Belov", "Ivan Poluyanov", "Oleg Mamontov", "Andrew Alexeev", "Grant Hulegaard"]
__license__ = ""
__maintainer__ = "Mike Belov"
__email__ = "dedm@nginx.com"


tokens_cache = {}


IGNORED_DIRECTIVES = [
    'ssl_certificate_key',
    'ssl_client_certificate',
    'ssl_password_file',
    'ssl_stapling_file',
    'ssl_trusted_certificate',
    'auth_basic_user_file',
    'secure_link_secret'
]


def set_line_number(string, location, tokens):
    if len(tokens) == 1:
        line_number = lineno(location, string)
        tokens_cache[tokens[0]] = line_number
        tokens.line_number = line_number
    else:
        for item in tokens:
            tokens.line_number = tokens_cache.get(item)


class NginxConfigParser(object):
    """
    Nginx config parser based on https://github.com/fatiherikli/nginxparser
    Parses single file into json structure
    """

    max_size = 20*1024*1024  # 20 mb

    # line starts/ends
    line_start = LineStart().suppress()
    line_end = LineEnd().suppress()

    # constants
    left_brace = Literal("{").suppress()
    left_parentheses = Literal("(").suppress()
    right_brace = Literal("}").suppress()
    right_parentheses = Literal(")").suppress()
    semicolon = Literal(";").suppress()
    space = White().suppress()
    singleQuote = Literal("'").suppress()
    doubleQuote = Literal('"').suppress()

    # keys
    if_key = Keyword("if").setParseAction(set_line_number)
    set_key = Keyword("set").setParseAction(set_line_number)
    rewrite_key = Keyword("rewrite").setParseAction(set_line_number)
    perl_set_key = Keyword("perl_set").setParseAction(set_line_number)
    log_format_key = Keyword("log_format").setParseAction(set_line_number)
    alias_key = Keyword("alias").setParseAction(set_line_number)
    return_key = Keyword("return").setParseAction(set_line_number)
    error_page_key = Keyword("error_page").setParseAction(set_line_number)
    map_key = Keyword("map").setParseAction(set_line_number)
    server_name_key = Keyword("server_name").setParseAction(set_line_number)
    sub_filter_key = Keyword("sub_filter").setParseAction(set_line_number)

    # lua keys
    start_with_lua_key = Regex(r'lua_\S+').setParseAction(set_line_number)
    contains_by_lua_key = Regex(r'\S+_by_lua\S*').setParseAction(set_line_number)

    key = (
        ~map_key & ~alias_key & ~perl_set_key &
        ~if_key & ~set_key & ~rewrite_key & ~server_name_key & ~sub_filter_key
    ) + Word(alphanums + '$_:%?"~<>\/-+.,*()[]"' + "'").setParseAction(set_line_number)

    # values
    value = Regex(r'[^{};]*"[^\";]+"[^{};]*|[^{};]*\'[^\';]+\'|[^{};]+(?!${.+})').setParseAction(set_line_number)
    quotedValue = Regex(r'"[^;]+"|\'[^;]+\'').setParseAction(set_line_number)
    rewrite_value = CharsNotIn(";").setParseAction(set_line_number)
    any_value = CharsNotIn(";").setParseAction(set_line_number)
    if_value = Regex(r'\(.*\)').setParseAction(set_line_number)
    language_include_value = CharsNotIn("'").setParseAction(set_line_number)
    strict_value = CharsNotIn("{};").setParseAction(set_line_number)
    sub_filter_value = Regex(r"\'(.|\n)+?\'",).setParseAction(set_line_number)

    # map values
    map_value_one = Regex(r'\'([^\']|\s)*\'').setParseAction(set_line_number)
    map_value_two = Regex(r'"([^"]|\s)*\"').setParseAction(set_line_number)
    map_value_three = Regex(r'((\\\s|[^{};\s])*)').setParseAction(set_line_number)
    map_value = (map_value_one | map_value_two | map_value_three)

    # modifier for location uri [ = | ~ | ~* | ^~ ]
    modifier = Literal("=") | Literal("~*") | Literal("~") | Literal("^~")

    # rules
    assignment = (
        key + Optional(space) + Optional(value) +
        Optional(space) + Optional(value) + Optional(space) + semicolon

    ).setParseAction(set_line_number)

    set = (
        set_key + Optional(space) + any_value + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    rewrite = (
        rewrite_key + Optional(space) + rewrite_value + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    perl_set = (
        perl_set_key + Optional(space) + key + Optional(space) +
        singleQuote + language_include_value + singleQuote + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    lua_content = (
        (start_with_lua_key | contains_by_lua_key) + Optional(space) +
        singleQuote + language_include_value + singleQuote + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    alias = (
        alias_key + space + any_value + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    return_ = (
        (return_key | error_page_key) + space + value + Optional(space) + Optional(any_value) + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    log_format = (
        log_format_key + Optional(space) + strict_value + Optional(space) + any_value + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    server_name = (
        server_name_key + space + any_value + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    sub_filter = (
        sub_filter_key + space + sub_filter_value + space + sub_filter_value + Optional(space) + semicolon
    ).setParseAction(set_line_number)

    # script
    map_block = Forward()
    map_block << Group(
        Group(
            map_key + space + map_value + space + map_value + Optional(space)
        ).setParseAction(set_line_number) +
        left_brace +
        Group(
            ZeroOrMore(
                Group(map_value + Optional(space) + Optional(map_value) + Optional(space) + semicolon)
            ).setParseAction(set_line_number)
        ) +
        right_brace
    )

    block = Forward()
    block << Group(
        (
            Group(
                key + Optional(space + modifier) + Optional(space) +
                Optional(value) + Optional(space) +
                Optional(value) + Optional(space)
            ) |
            Group(if_key + space + if_value + Optional(space))
        ).setParseAction(set_line_number) +
        left_brace +
        Group(
            ZeroOrMore(
                 Group(log_format) | Group(lua_content) | Group(perl_set) |
                 Group(set) | Group(rewrite) | Group(alias) | Group(return_) |
                 Group(assignment) | Group(server_name) | Group(sub_filter) |
                 map_block | block
            ).setParseAction(set_line_number)
        ).setParseAction(set_line_number) +
        right_brace
    )

    script = OneOrMore(
        Group(log_format) | Group(perl_set) | Group(lua_content) | Group(alias) | Group(return_) |
        Group(assignment) | Group(set) | Group(rewrite) | Group(sub_filter) |
        map_block | block
    ).ignore(pythonStyleComment)

    INCLUDE_RE = re.compile(r'[^#]*include\s+(?P<include_file>.*);')
    SSL_CERTIFICATE_RE = re.compile(r'[^#]*ssl_certificate\s+(?P<cert_file>.*);')

    def __init__(self, filename='/etc/nginx/nginx.conf'):
        global tokens_cache
        tokens_cache = {}

        self.filename = filename
        self.folder = '/'.join(self.filename.split('/')[:-1])  # stores path to folder with main config
        self.files = {}  # to prevent cycle files and line indexing
        self.broken_files = set()  # to prevent reloading broken files
        self.index = []  # stores index for all sections (points to file number and line number)
        self.ssl_certificates = []
        self.errors = []
        self.tree = {}

    def parse(self):
        self.tree = self.__logic_parse(self.__pyparse(self.filename))

    @staticmethod
    def get_file_info(filename):
        """
        Returns file size, mtime and permissions
        :param filename: str filename
        :return: int, int, str - size, mtime, permissions
        """
        size, mtime, permissions = 0, 0, '0000'

        try:
            size = os.path.getsize(filename)
            mtime = int(os.path.getmtime(filename))
            permissions = oct(os.stat(filename).st_mode & 0777)
        except Exception, e:
            exception_name = e.__class__.__name__
            message = 'failed to stat %s due to: %s' % (filename, exception_name)
            context.log.debug(message, exc_info=True)

        return size, mtime, permissions

    def resolve_local_path(self, path):
        """
        Resolves local path
        :param path: str path
        :return: absolute path
        """
        result = path.replace('"', '')
        if not result.startswith('/'):
            result = '%s/%s' % (self.folder, result)
        return result

    def collect_all_files(self, include_ssl_certs=False):
        """
        Tries to collect all included files and ssl certs and return
        them as dict with mtimes, sizes and permissions.
        Later this dict will be used to determine if a config was changed or not.

        We don't use md5 or other hashes, because it takes time and we should be able
        to run these checks every 20 seconds or so

        :param include_ssl_certs: bool - include ssl certs  or not
        :return: {} of files
        """
        result = {}

        # collect all files
        def lightweight_include_search(include_files):
            for filename in include_files:
                if filename in result:
                    continue
                result[filename] = None
                try:
                    for line in open(filename):
                        if 'include' in line:
                            gre = self.INCLUDE_RE.match(line)
                            if gre:
                                new_includes = self.find_includes(gre.group('include_file'))
                                lightweight_include_search(new_includes)
                        elif include_ssl_certs and 'ssl_certificate' in line:
                            gre = self.SSL_CERTIFICATE_RE.match(line)
                            if gre:
                                cert_filename = self.resolve_local_path(gre.group('cert_file'))
                                result[cert_filename] = None
                except Exception as e:
                    exception_name = e.__class__.__name__
                    message = 'failed to read %s due to: %s' % (filename, exception_name)
                    context.log.debug(message, exc_info=True)

        lightweight_include_search(self.find_includes(self.filename))

        # get mtimes, sizes and permissions
        for filename in result.iterkeys():
            size, mtime, permissions = self.get_file_info(filename)
            result[filename] = '%s_%s_%s' % (size, mtime, permissions)

        return result

    def find_includes(self, path):
        """
        Takes include path and returns all included files
        :param path: str path
        :return: [] of str file names
        """
        # resolve local paths
        path = self.resolve_local_path(path)

        # load all files
        result = []
        if '*' in path:
            for filename in glob.glob(path):
                result.append(filename)
        else:
            result.append(path)

        return result

    def __pyparse(self, path):
        """
        Loads and parses all files

        :param path: file path (can contain *)
        """
        result = {}
        for filename in self.find_includes(path):
            if filename in self.broken_files:
                continue
            elif filename not in self.files:
                file_index = len(self.files)
                self.files[filename] = {
                    'index': file_index,
                    'lines': 0,
                    'size': 0,
                    'mtime': 0,
                    'permissions': ''
                }
            else:
                file_index = self.files[filename]['index']

            try:
                size, mtime, permissions = self.get_file_info(filename)

                self.files[filename]['size'] = size
                self.files[filename]['mtime'] = mtime
                self.files[filename]['permissions'] = permissions

                if size > self.max_size:
                    self.errors.append('failed to read %s due to: too large, %s bytes' % (filename, size))
                    continue

                source = open(filename).read()
                lines_count = source.count('\n')
                self.files[filename]['lines'] = lines_count
            except Exception as e:
                exception_name = e.__class__.__name__
                message = 'failed to read %s due to: %s' % (filename, exception_name)
                self.errors.append(message)
                self.broken_files.add(filename)
                del self.files[filename]
                context.log.error(message)
                context.log.debug('additional info:', exc_info=True)
                continue

            # Replace windows line endings with unix ones.
            source = source.replace('\r\n', '\n')

            # check that file contains some information (not commented)
            all_lines_commented = True
            for line in source.split('\n'):
                line = line.replace(' ',  '')
                if line and not line.startswith('#'):
                    all_lines_commented = False
                    break

            if all_lines_commented:
                continue

            # replace \' with " because otherwise we cannot parse it
            slash_quote = '\\' + "'"
            source = source.replace(slash_quote, '"')

            try:
                parsed = self.script.parseString(source, parseAll=True)
            except Exception as e:
                exception_name = e.__class__.__name__
                message = 'failed to parse %s due to %s' % (filename, exception_name)
                self.errors.append(message)
                context.log.error(message)
                context.log.debug('additional info:', exc_info=True)
                continue

            result[file_index] = list(parsed)

        return result

    def __logic_parse(self, files, result=None):
        """
        Parses input files and updates result dict

        :param files: dict of files from pyparsing
        :return: dict of config tree
        """
        if result is None:
            result = {}

        for file_index, rows in files.iteritems():
            while len(rows):
                row = rows.pop(0)
                row_as_list = row.asList()
                
                if isinstance(row_as_list[0], list):
                    # this is a new key
                    key_bucket, value_bucket = row
                    key = key_bucket[0]

                    if len(key_bucket) == 1:
                        # simple key, with one param
                        subtree_indexed = self.__idx_save(
                            self.__logic_parse({file_index: row[1]}),
                            file_index, row.line_number
                        )
                        if key == 'server':
                            # work with servers
                            if key in result:
                                result[key].append(subtree_indexed)
                            else:
                                result[key] = [subtree_indexed]
                        else:
                            result[key] = subtree_indexed
                    else:
                        # compound key (for locations and upstreams for example)

                        # remove all redundant spaces
                        parts = filter(lambda x: x, ' '.join(key_bucket[1:]).split(' '))
                        sub_key = ' '.join(parts)

                        subtree_indexed = self.__idx_save(
                            self.__logic_parse({file_index: row[1]}),
                            file_index, row.line_number
                        )

                        if key in result:
                            result[key][sub_key] = subtree_indexed
                        else:
                            result[key] = {sub_key: subtree_indexed}
                else:
                    # can be just an assigment, without value
                    if len(row) >= 2:
                        key, value = row[0], ''.join(row[1:])
                    else:
                        key, value = row[0], ''

                    # transform multiline values to single one
                    if """\'""" in value or """\n""" in value:
                        value = re.sub(r"\'\s*\n\s*\'", '', value)
                        value = re.sub(r"\'", "'", value)

                    if key in IGNORED_DIRECTIVES:
                        continue  # Pass ignored directives.
                    elif key == 'log_format':
                        # work with log formats
                        gwe = re.match("([\w\d_-]+)\s+'(.+)'", value)
                        if gwe:
                            format_name, format_value = gwe.group(1), gwe.group(2)

                            indexed_value = self.__idx_save(format_value, file_index, row.line_number)
                            # Handle odd Python auto-escaping of raw strings when packing/unpacking.
                            indexed_value = (prep_raw(indexed_value[0]), indexed_value[1])

                            if key in result:
                                result[key][format_name] = indexed_value
                            else:
                                result[key] = {format_name: indexed_value}
                    elif key == 'include':
                        indexed_value = self.__idx_save(value, file_index, row.line_number)

                        if key in result:
                            result[key].append(indexed_value)
                        else:
                            result[key] = [indexed_value]

                        included_files = self.__pyparse(value)
                        self.__logic_parse(included_files, result=result)
                    elif key in ('access_log', 'error_log'):
                        # Handle access_log and error_log edge cases
                        if value == '':
                            continue  # skip log directives that are empty

                        if '$' in value and ' if=$' not in value:
                            continue  # skip directives that are use nginx variables and it's not if

                        # Otherwise handle normally (see ending else below).
                        indexed_value = self.__idx_save(value, file_index, row.line_number)
                        self.__simple_save(result, key, indexed_value)
                    elif key == 'ssl_certificate':
                        if value == '':
                            continue  # skip empty values

                        if '$' in value and ' if=$' not in value:
                            continue  # skip directives that are use nginx variables and it's not if

                        self.ssl_certificates.append(self.resolve_local_path(value))  # Add value to ssl_certificates

                        # save config value
                        indexed_value = self.__idx_save(value, file_index, row.line_number)
                        self.__simple_save(result, key, indexed_value)
                    else:
                        indexed_value = self.__idx_save(value, file_index, row.line_number)
                        self.__simple_save(result, key, indexed_value)

        return result

    def __idx_save(self, value, file_index, line):
        new_index = len(self.index)
        self.index.append((file_index, line))
        return value, new_index

    def __simple_save(self, result, key, indexed_value):
        """
        We ended up having duplicate code when adding key-value pairs to our parsing dictionary (
        when handling access_log and error_log directives).

        This prompted us to refactor this process out to a separate function.  Because dictionaries are passed by
        reference in Python, we can alter the value dictionary in this local __func__ scope and have it affect the dict
        in the parent.

        :param result: dict Passed and altered by reference from the parent __func__ scope
        :param key:
        :param indexed_value:
        (No return since we are altering a pass-by-reference dict)
        """
        # simple key-value
        if key in result:
            stored_value = result[key]
            if isinstance(stored_value, list):
                result[key].append(indexed_value)
            else:
                result[key] = [stored_value, indexed_value]
        else:
            result[key] = indexed_value
    
    def simplify(self, tree=None):
        """
        returns tree without index references
        can be used for debug/pretty output

        :param tree: - dict of tree
        :return: dict of self.tree without index positions
        """
        result = {}

        if tree is None:
            tree = self.tree

        if isinstance(tree, dict):
            for key, value in tree.iteritems():
                if isinstance(value, dict):
                    result[key] = self.simplify(tree=value)
                elif isinstance(value, tuple):
                    subtree, reference = value
                    if isinstance(subtree, dict):
                        result[key] = self.simplify(tree=subtree)
                    elif isinstance(subtree, list):
                        result[key] = map(lambda x: self.simplify(tree=x), subtree)
                    else:
                        result[key] = subtree
                elif isinstance(value, list):
                    result[key] = map(lambda x: self.simplify(tree=x), value)
        elif isinstance(tree, tuple):
            subtree, reference = tree
            if isinstance(subtree, dict):
                return self.simplify(tree=subtree)
            elif isinstance(subtree, list):
                return map(lambda x: self.simplify(tree=x), subtree)
            else:
                return subtree
        elif isinstance(tree, list):
            return map(lambda x: self.simplify(tree=x), tree)

        return result
