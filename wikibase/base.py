"""
Wikibase database backend for Django.
"""

import sys
from typing import Iterable

from django.db import utils
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.models import Model
from django.utils.encoding import smart_str
from django.utils.functional import cached_property
from six import reraise

from wikibase.wdb import WbDatabase, charset_map

from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .ir.cmd import Cmd
from .schema import DatabaseSchemaEditor
from .validation import DatabaseValidation
from .wb_operations import DatabaseOperations

DatabaseError = WbDatabase.DatabaseError
IntegrityError = WbDatabase.IntegrityError
OperationalError = WbDatabase.OperationalError


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mast.eu.spb.ru'

    # This dictionary maps Field objects to their associated Wikibase column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    #
    # Any format strings starting with "qn_" are quoted before being used in the
    # output (the "qn_" prefix is stripped before the lookup is performed.

    data_types = {
        'AutoField': 'integer',
        'BigAutoField': 'bigint',
        'BinaryField': 'blob sub_type 0',
        'BooleanField': 'smallint',  # for wikibase 3 it changes in init_connection_state
        'CharField': 'varchar(%(max_length)s)',
        'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
        'DateField': 'date',
        'DateTimeField': 'timestamp',
        'DecimalField': 'decimal(%(max_digits)s, %(decimal_places)s)',
        'DurationField': 'bigint',
        'FileField': 'varchar(%(max_length)s)',
        'FilePathField': 'varchar(%(max_length)s)',
        'FloatField': 'double precision',
        'IntegerField': 'integer',
        'BigIntegerField': 'bigint',
        'IPAddressField': 'char(15)',
        'GenericIPAddressField': 'char(39)',
        # for wikibase 3 it changes in init_connection_state
        'NullBooleanField': 'smallint',
        'OneToOneField': 'integer',
        'PositiveIntegerField': 'integer',
        'PositiveSmallIntegerField': 'smallint',
        'SlugField': 'varchar(%(max_length)s)',
        'SmallIntegerField': 'smallint',
        'TextField': 'blob sub_type 1',
        'TimeField': 'time',
        'UUIDField': 'char(32)',
    }

    data_type_check_constraints = {
        # for wikibase 3 it changes in init_connection_state
        'BooleanField': '%(qn_column)s IN (0,1)',
        'NullBooleanField': '(%(qn_column)s IN (0,1)) OR (%(qn_column)s IS NULL)',
        'PositiveIntegerField': '%(qn_column)s >= 0',
        'PositiveSmallIntegerField': '%(qn_column)s >= 0',
    }

    operators = {
        'exact': '= %s',
        'iexact': '= UPPER(%s)',
        'contains': "LIKE %s ESCAPE'\\'",
        # 'CONTAINING %s', #case is ignored
        'icontains': "LIKE UPPER(%s) ESCAPE'\\'",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        # 'STARTING WITH %s', #looks to be faster than LIKE
        'startswith': "LIKE %s ESCAPE'\\'",
        'endswith': "LIKE %s ESCAPE'\\'",
        # 'STARTING WITH UPPER(%s)',
        'istartswith': "LIKE UPPER(%s) ESCAPE'\\'",
        'iendswith': "LIKE UPPER(%s) ESCAPE'\\'",
        'regex': "SIMILAR TO %s",
        'iregex': "SIMILAR TO %s",  # Case Sensitive depends on collation
    }

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    #
    # Note: we use str.format() here for readability as '%' is used as a wildcard for
    # the LIKE operator.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    Database = WbDatabase
    SchemaEditorClass = DatabaseSchemaEditor

    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        self._prefixes = None
        self._server_version = None
        self._db_charset = None
        self.encoding = None

        opts = self.settings_dict["OPTIONS"]
        RC = WbDatabase.ISOLATION_LEVEL_READ_COMMITED
        self.isolation_level = opts.get('isolation_level', RC)

        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = DatabaseValidation(self)

    # #### Backend-specific methods for creating connections and cursors #####

    def get_connection_params(self):
        """Returns a dict of parameters suitable for get_new_connection."""
        settings_dict = self.settings_dict
        if settings_dict['URL'] == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the URL value (for example https://www.wikidata.org/w). "
                "URL value is a prefix for the api.php")

        # The port param is not used by fdb. It must be setting by url string
        url = settings_dict['URL']
        conn_params = {
            'charset': 'utf-8',
        }
        conn_params['url'] = url % settings_dict
        if settings_dict['BOT_USERNAME']:
            conn_params['bot_username'] = settings_dict['BOT_USERNAME']
        if settings_dict['BOT_PASSWORD']:
            conn_params['bot_password'] = settings_dict['BOT_PASSWORD']
        if 'ROLE' in settings_dict:
            conn_params['role'] = settings_dict['ROLE']
        options = settings_dict['OPTIONS'].copy()
        conn_params.update(options)

        self._db_charset = conn_params.get('charset')
        self.encoding = charset_map.get(self._db_charset, 'utf_8')

        return conn_params

    def get_new_connection(self, conn_params):
        """Opens a connection to the database."""
        return WbDatabase.connect(**conn_params)

    def init_connection_state(self):
        """Initializes the database connection settings."""
        # if int(self.ops.mediawiki_version[3]) >= 3:
        #     self.data_types['BooleanField'] = 'boolean'
        #     self.data_types['NullBooleanField'] = 'boolean'
        #     self.data_type_check_constraints['BooleanField'] = '%(qn_column)s IN (False,True)'
        #     self.data_type_check_constraints['NullBooleanField'] = '(%(qn_column)s IN (False,True)) OR (%(qn_column)s IS NULL)'
        ...

    def create_cursor(self, name=None):
        """Creates a cursor. Assumes that a connection is established."""
        cursor = self.connection.cursor()
        return WikibaseCursorWrapper(cursor, self.encoding)

    # ##### Foreign key constraints checks handling #####

    def disable_constraint_checking(self):
        """
        Backends can implement as needed to temporarily disable foreign key
        constraint checking. Should return True if the constraints were
        disabled and will need to be reenabled.
        """
        self.disable_constraints()
        return True

    def enable_constraint_checking(self):
        """
        Backends can implement as needed to re-enable foreign key constraint
        checking.
        """
        self.enable_constraints()

    def disable_constraints(self):
        """
        Disables restrictions such as foreign keys, checks and unique.

        .. important::

           The server does not support explicit disabling of restrictions,
           therefore, implementation is made using additional tables.

        Note:
           If there is an error when disable restrictions an exception is thrown.
        """
        # # create_django_constraint = """
        # #     create table django$constraint (
        # #         django$constraint_name varchar(31) not null constraint pk_djangocopyconstraint primary key,
        # #         django$constraint_type varchar(11) not null,
        # #         django$relation_name varchar(31) not null,
        # #         django$index_name varchar(31),
        # #         django$const_name_uq varchar(31),
        # #         django$update_rule varchar(11),
        # #         django$delete_rule varchar(11),
        # #         django$constraint_source blob sub_type 1
        # #     )
        # # """
        # create_django_constraint_cmd = Cmd('create_django_constraint')
        # # create_django_constraint_segment = """
        # #     create table django$constraint_segment (
        # #         django$constraint_name varchar(31) not null,
        # #         django$field_name varchar(31) not null,
        # #         django$position integer not null,
        # #         constraint pk_djangocopyconstraintsegment primary key (django$constraint_name, django$field_name)
        # #     )
        # # """
        # create_django_constraint_segment_cmd = Cmd(
        #     'create_django_constraint_segment')
        # # save_constraints = """
        # #     merge into django$constraint dc
        # #     using (select
        # #         trim(trailing from c.rdb$constraint_name) as constraint_name,
        # #         trim(trailing from c.rdb$constraint_type) as constraint_type,
        # #         trim(trailing from c.rdb$relation_name) as relation_name,
        # #         nullif(trim(trailing from c.rdb$index_name), '') as index_name,
        # #         t.rdb$trigger_source as constraint_source,
        # #         nullif(trim(trailing from r.rdb$const_name_uq), '') as const_name_uq,
        # #         nullif(trim(trailing from r.rdb$update_rule), '') as update_rule,
        # #         nullif(trim(trailing from r.rdb$delete_rule), '') as delete_rule
        # #     from
        # #     rdb$relation_constraints c left join
        # #         (select * from rdb$check_constraints p
        # #         where p.rdb$trigger_name = (select first 1 rdb$trigger_name from rdb$check_constraints o
        # #         where p.rdb$constraint_name = o.rdb$constraint_name)) h on c.rdb$constraint_name = h.rdb$constraint_name
        # #         left join rdb$triggers t on t.rdb$trigger_name = h.rdb$trigger_name
        # #         left join rdb$ref_constraints r on r.rdb$constraint_name = c.rdb$constraint_name
        # #     where c.rdb$constraint_type in ('FOREIGN KEY', 'CHECK', 'UNIQUE')
        # #     and
        # #     c.rdb$relation_name not starting with 'RDB$') rc
        # #     on dc.django$constraint_name = rc.constraint_name
        # #     when matched then
        # #         update set
        # #             dc.django$constraint_type = rc.constraint_type,
        # #             dc.django$relation_name = rc.relation_name,
        # #             dc.django$index_name = rc.index_name,
        # #             dc.django$constraint_source = rc.constraint_source,
        # #             dc.django$const_name_uq = rc.const_name_uq,
        # #             dc.django$update_rule = rc.update_rule,
        # #             dc.django$delete_rule = rc.delete_rule
        # #     when not matched then
        # #         insert (dc.django$constraint_name,
        # #             dc.django$constraint_type,
        # #             dc.django$relation_name,
        # #             dc.django$index_name,
        # #             dc.django$constraint_source,
        # #             dc.django$const_name_uq,
        # #             dc.django$update_rule,
        # #             dc.django$delete_rule)
        # #         values (rc.constraint_name,
        # #             rc.constraint_type,
        # #             rc.relation_name,
        # #             rc.index_name,
        # #             rc.constraint_source,
        # #             rc.const_name_uq,
        # #             update_rule,
        # #             rc.delete_rule)
        # # """
        # save_constraints_cmd = Cmd('save_constraints')
        # # save_segment_constraints = """
        # #     merge into django$constraint_segment dcs
        # #     using (
        # #         select c.rdb$constraint_name as constraint_name,
        # #             s.rdb$field_name as field_name,
        # #             s.rdb$field_position as field_position
        # #         from rdb$relation_constraints c, rdb$index_segments s
        # #         where s.rdb$index_name = c.rdb$index_name
        # #         and c.rdb$constraint_type in ('FOREIGN KEY', 'CHECK', 'UNIQUE')
        # #         and c.rdb$relation_name not starting with 'RDB$') cs
        # #     on dcs.django$constraint_name = cs.constraint_name
        # #         and dcs.django$field_name = cs.field_name
        # #     when matched then
        # #         update set
        # #             dcs.django$position = cs.field_position
        # #     when not matched then
        # #         insert (django$constraint_name,
        # #             django$field_name,
        # #             django$position)
        # #         values (
        # #             cs.constraint_name,
        # #             cs.field_name,
        # #             cs.field_position)
        # # """
        # save_segment_constraints_cmd = Cmd('save_segment_constraints')
        # # select_drop_constraints = """
        # #     select trim(trailing from rdb$constraint_name) as constr_name,
        # #         trim(trailing from c.rdb$relation_name) as table_name
        # #     from rdb$relation_constraints c
        # #     where c.rdb$constraint_type in ('FOREIGN KEY', 'CHECK', 'UNIQUE')
        # #     and
        # #     c.rdb$relation_name not starting with 'RDB$'
        # #     order by c.rdb$constraint_type
        # # """
        # select_drop_constraints_cmd = Cmd('select_drop_constraints')
        # editor = self.schema_editor()
        # if not self.table_exists("django$constraint"):
        #     editor.execute(create_django_constraint_cmd)
        # if not self.table_exists("django$constraint_segment"):
        #     editor.execute(create_django_constraint_segment_cmd)
        # editor.execute(save_constraints_cmd)
        # editor.execute(save_segment_constraints_cmd)
        # constraints = self.get_drop_constraints(select_drop_constraints_cmd)
        # for hm in constraints:
        #     # sql = """
        #     #     alter table "%(table)s" drop constraint "%(constraint)s"
        #     # """ % {
        #     #     'table': hm['TABLE_NAME'],
        #     #     'constraint': hm['CONSTR_NAME']
        #     # }
        #     drop_constraint_cmd = Cmd('drop_constraint', data={
        #                               'table_name': hm['TABLE_NAME'], 'constraint_name': hm['CONSTR_NAME']})
        #     editor.execute(drop_constraint_cmd)
        editor = self.schema_editor()
        editor.execute(Cmd('disable_constraints'))

    def enable_constraints(self):
        """
        Enables restrictions that have been disabled by calling the method `disable_constraint_checking`.

        .. important::

           The server does not support explicit disabling of restrictions,
           therefore, implementation is made using additional tables.

        Note:
           If there is an error when enable restrictions an exception is thrown.
        """
        # editor = self.schema_editor()
        # select_django_constraint = """
        # select django$constraint_name as constraint_name,
        #     django$constraint_type as constraint_type,
        #     django$relation_name as relation_name,
        #     django$index_name as index_name,
        #     django$constraint_source as constraint_source,
        #     django$const_name_uq as const_name_uq,
        #     django$update_rule as update_rule,
        #     django$delete_rule as delete_rule
        # from django$constraint
        # order by django$constraint_type desc"""
        # select_django_constraint_cmd = Cmd('select_django_constraint')
        # constraints = self.get_drop_constraints(select_django_constraint_cmd)
        # for hm in constraints:
        #     if not self.table_exists(hm['RELATION_NAME'].strip()):
        #         continue
        #     create_string = "alter table \"" + hm['RELATION_NAME'] + "\" add "
        #     if not hm['CONSTRAINT_NAME'].startswith("RDB$"):
        #         create_string += "constraint " + hm['CONSTRAINT_NAME']
        #     if hm['CONSTRAINT_TYPE'].casefold() == "CHECK".casefold():
        #         create_string += " " + hm['CONSTRAINT_SOURCE']
        #     elif hm['CONSTRAINT_TYPE'].casefold() == "FOREIGN KEY".casefold():
        #         # select_relation = """select coalesce(trim(trailing from rdb$relation_name), '')
        #         #     from rdb$relation_constraints where rdb$constraint_name = '%s'
        #         # """ % hm['CONST_NAME_UQ']
        #         select_relation_cmd = Cmd('select_relation_by_foreigh_key', data={
        #                                   'foreign_key': hm['CONST_NAME_UQ']})
        #         table = ''
        #         with self.cursor() as cursor:
        #             cursor.execute(select_relation_cmd)
        #             res = cursor.fetchone()
        #             if not res:
        #                 continue
        #             table = '"' + res[0].strip() + '"'
        #         # select_django_constraint_segment = """
        #         #     select django$field_name as field_name from django$constraint_segment s
        #         #     where s.django$constraint_name = '%s'
        #         #     order by django$position
        #         # """ % hm['CONSTRAINT_NAME']
        #         select_django_constraint_segment_cmd = Cmd('select_django_constraint_segment_by_foreigh_key', data={
        #                                                    'constraint_name': hm['CONSTRAINT_NAME']})
        #         segments = []
        #         with self.cursor() as cursor:
        #             cursor.execute(select_django_constraint_segment_cmd)
        #             for row in cursor.fetchall():
        #                 map = {}
        #                 for i, desc in enumerate(cursor.cursor.cursor.description):
        #                     map[desc[0]] = row[i]
        #                 segments.append(map)
        #         field_list = []
        #         for field in segments:
        #             field_list.append('"' + field['FIELD_NAME'].strip() + '"')
        #         create_string += " foreign key(" + ','.join(str(x) for x in field_list) +\
        #                          ") references " + table + "("
        #         # select_index_segment = """
        #         #     select trim(trailing from rdb$field_name) as field_name from rdb$index_segments s
        #         #     join rdb$relation_constraints r on r.rdb$index_name = s.rdb$index_name
        #         #     where r.rdb$constraint_name = '%s'
        #         #     order by rdb$field_position
        #         # """ % hm['CONST_NAME_UQ']
        #         select_index_segment_cmd = Cmd('select_index_segment_by_unique_constraint_name', data={
        #                                        'constraint_name': hm['CONST_NAME_UQ']})
        #         index_segments = []
        #         with self.cursor() as cursor:
        #             cursor.execute(select_index_segment_cmd)
        #             for row in cursor.fetchall():
        #                 index_segments.append('"' + row[0].strip() + '"')
        #         create_string += ','.join(str(x) for x in index_segments) + ")"
        #         if hm['UPDATE_RULE'].casefold() != "RESTRICT".casefold():
        #             create_string += " on update " + hm['UPDATE_RULE']
        #         if hm['DELETE_RULE'].casefold() != "RESTRICT".casefold():
        #             create_string += " on delete " + hm['DELETE_RULE']
        #         if not hm['INDEX_NAME'].startswith("RDB$"):
        #             create_string += " using index " + hm['INDEX_NAME']
        #     elif hm['CONSTRAINT_TYPE'].casefold() == "UNIQUE".casefold():
        #         # select_django_constraint_segment_cmd = """
        #         #     select django$field_name as field_name from django$constraint_segment s
        #         #     where s.django$constraint_name = '%s'
        #         #     order by django$position
        #         # """ % hm['CONSTRAINT_NAME']
        #         select_django_constraint_segment_cmd = Cmd('select_django_constraint_segment_by_constraint_name', data={
        #                                                    'constraint_name': hm['CONSTRAINT_NAME']})
        #         segments = []
        #         with self.cursor() as cursor:
        #             cursor.execute(select_django_constraint_segment_cmd)
        #             for row in cursor.fetchall():
        #                 map = {}
        #                 for i, desc in enumerate(cursor.cursor.cursor.description):
        #                     map[desc[0]] = row[i]
        #                 segments.append(map)
        #         field_list = []
        #         for field in segments:
        #             field_list.append('"' + field['FIELD_NAME'].strip() + '"')
        #         create_string += " unique(" + ','.join(str(x)
        #                                                for x in field_list) + ")"
        #     try:
        #         editor.execute(create_string)
        #     except Exception as e:
        #         print(e)
        # try:
        #     editor.execute("delete from django$constraint_segment")
        #     editor.execute("delete from django$constraint")
        # except Exception as e:
        #     print(e)
        editor = self.schema_editor()
        editor.execute(Cmd('enable_constraints'))

    def table_exists(self, table_name):
        """
        Checks whether a table with a specified name exists.

        Args:
            table_name (str): Table name for checking
        """
        # sql = """
        #     select null from rdb$relations where rdb$system_flag=0 and rdb$view_blr is null and rdb$relation_name='%s'
        # """ % str(table_name).upper()
        table_exists_cmd = Cmd('table_exists', data={
                               'table_name': str(table_name).upper()})
        value = None
        with self.cursor() as cursor:
            cursor.execute(table_exists_cmd)
            value = cursor.fetchone()
        return True if value else False

    def get_drop_constraints(self, cmd):
        """
        Returns objects that should be dropped when restrictions are disabled.

        Args:
            query (str): SQL query to execute

        .. important::

           The server does not support explicit disabling of restrictions,
           therefore, implementation is made using additional tables.

        Note:
           If there is an error when execute query an exception is thrown.
        """
        value = []
        with self.cursor() as cursor:
            cursor.execute(cmd)
            for row in cursor.fetchall():
                map = {}
                for i, desc in enumerate(cursor.cursor.cursor.description):
                    map[desc[0]] = row[i]
                value.append(map)
        return value

    # #### Backend-specific transaction management methods #####

    def _set_autocommit(self, autocommit):
        """
        Backend-specific implementation to enable or disable autocommit.

        FDB doesn't support auto-commit feature directly, but developers may
        achieve the similar result using explicit transaction start, taking
        advantage of default_action and its default value (commit).
        See:
        http://www.wikibasesql.org/file/documentation/drivers_documentation/python/fdb/usage-guide.html#auto-commit

        Pay attention at _close() method below
        """
        pass

    # #### Backend-specific wrappers for PEP-249 connection methods #####

    def _close(self):
        if self.connection is not None:
            with self.wrap_database_errors:
                if self.autocommit is True:
                    self.connection.commit()
                return self.connection.close()

    # #### Connection termination handling #####

    def is_usable(self):
        """
        Tests if the database connection is usable.
        This function may assume that self.connection is not None.
        """
        try:
            cur = self.connection.cursor()
            cur.execute('SELECT 1 FROM RDB$DATABASE')
        except DatabaseError:
            return False
        else:
            return True

    @cached_property
    def server_version(self):
        """
        Access method for engine_version property.
        engine_version return a full version in string format
        (ie: 'WI-V6.3.5.4926 Wikibase 1.5' )
        """
        if not self._server_version:
            if not self.connection:
                self.cursor()
            self._server_version = self.connection.db_info(
                WbDatabase._MEDIAWIKI_VERSION)
        return self._server_version

    @cached_property
    def prefixes(self):
        if not self._prefixes:
            if not self.connection:
                self.cursor()
            self._prefixes = self.connection.prefixes()
        return self._prefixes

    def use_models(self, models: Iterable[Model]):
        if not self.connection:
            self.cursor()
        self.connection.check_models(models)


class WikibaseCursorWrapper(object):
    """
    Django uses "format" style placeholders, but wikibase uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    """
    codes_for_integrityerror = (-803, -625, -530)

    def __init__(self, cursor, encoding):
        self.cursor = cursor
        self.encoding = encoding

    def execute(self, cmd, params=None):
        if params is None:
            params = []
        try:
            c = self.compile_cmd(cmd, len(params))
            return self.cursor.execute(c, params)
        except WbDatabase.IntegrityError as e:
            reraise(utils.IntegrityError, utils.IntegrityError(
                *self.error_info(e, cmd, params)), sys.exc_info()[2])
        except WbDatabase.DatabaseError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            # fdb: raise exception as tuple with (error_msg, sqlcode, error_code)
            code = self.get_sql_code(e)
            if code in self.codes_for_integrityerror:
                reraise(utils.IntegrityError, utils.IntegrityError(
                    *self.error_info(e, cmd, params)), sys.exc_info()[2])
            raise

    def executemany(self, cmd, param_list):
        try:
            q = self.compile_cmd(cmd, len(param_list[0]))
            return self.cursor.executemany(q, param_list)
        except WbDatabase.IntegrityError as e:
            reraise(utils.IntegrityError, utils.IntegrityError(
                *self.error_info(e, cmd, param_list[0])), sys.exc_info()[2])
        except WbDatabase.DatabaseError as e:
            # Map some error codes to IntegrityError, since they seem to be
            # misclassified and Django would prefer the more logical place.
            # fdb: raise exception as tuple with (error_msg, sqlcode, error_code)
            code = self.get_sql_code(e)
            if code in self.codes_for_integrityerror:
                reraise(utils.IntegrityError, utils.IntegrityError(
                    *self.error_info(e, cmd, param_list[0])), sys.exc_info()[2])
            raise

    def compile_cmd(self, cmd, num_params):
        """
            Check and compile
        """
        # if num_params == 0:
        #     return smart_str(query, self.encoding)
        # return smart_str(query % tuple("?" * num_params), self.encoding)
        return cmd

    def get_sql_code(self, e):
        try:
            sql_code = e.args[1]
        except IndexError:
            sql_code = None
        return sql_code

    def error_info(self, e, q, p):
        # fdb: raise exception as tuple with (error_msg, sqlcode, error_code)
        # just when it uses exception_from_status function. Ticket #44.
        try:
            error_msg = e.args[0]
        except IndexError:
            error_msg = ''

        try:
            sql_code = e.args[1]
        except IndexError:
            sql_code = None

        try:
            error_code = e.args[2]
        except IndexError:
            error_code = None

        if q:
            sql_text = q % tuple(p)
        else:
            sql_text = q
        return tuple([error_msg, sql_code, error_code, {'sql': sql_text, 'params': p}])

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            return getattr(self.cursor, attr)

    def __iter__(self):
        return iter(self.cursor)
