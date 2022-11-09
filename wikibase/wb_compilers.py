from json import dumps
from json.encoder import JSONEncoder
from re import DOTALL, MULTILINE
from typing import Any, Iterable, Optional, Tuple

from django.core.exceptions import EmptyResultSet, FieldError
from django.db import DatabaseError, NotSupportedError
from django.db.models.expressions import Col, OrderBy
from django.db.models.fields.related_lookups import (RelatedExact,
                                                     RelatedGreaterThan,
                                                     RelatedGreaterThanOrEqual,
                                                     RelatedIn, RelatedIsNull,
                                                     RelatedLessThan,
                                                     RelatedLessThanOrEqual)
from django.db.models.lookups import Exact
from django.db.models.sql import compiler
from django.db.models.sql.constants import MULTI
from django.db.models.sql.datastructures import BaseTable, Join
from django.db.models.sql.query import Query
from django.db.models.sql.where import WhereNode
from django.db.transaction import TransactionManagementError
from django.utils.regex_helper import _lazy_re_compile

# The izip_longest was renamed to zip_longest in py3
try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest

from .ir.cmd import Cmd
from .ir.django_model import DjangoModel
from .ir.django_property import DjangoProperty
from .loggable import Loggable

_BUILT_IN_TYPES = {int, float, str, bool}


class SQLCompiler(compiler.SQLCompiler, Loggable):

    _select_data = {
        'models': None,
        'sparql': None
    }

    def __init__(self, query, connection, using, elide_empty=True):
        super().__init__(query, connection, using)
        self.ordering_parts = _lazy_re_compile(
            r'^(?:ASC|DESC)\((.+)\).*',  # The first group is the field alias
            MULTILINE | DOTALL,
        )

    def get_select(self):
        result, class_info, annotations = super().get_select()
        # check linked columns
        foreign_keys = [column_info for column_info in result if type(
            column_info[0].target).__name__ == 'ForeignKey']
        if foreign_keys:
            source = result[:]
            result.clear()  # Mutation
            for column_info in source:
                if column_info in foreign_keys:
                    # Unpack foreign reference
                    expressions = []
                    for concrete_field in column_info[0].field.model._meta.concrete_fields:
                        concrete_field_alias = concrete_field.column
                        expressions.append(
                            (Col(concrete_field_alias, concrete_field), (f'?{concrete_field_alias}', []), None))
                    result.extend(expressions)
                else:
                    result.append(column_info)
        return result, class_info, annotations

    def get_from_clause(self):
        result = []
        params = []
        for alias in tuple(self.query.alias_map):
            if not self.query.alias_refcount[alias]:
                continue
            try:
                from_clause = self.query.alias_map[alias]
            except KeyError:
                # Extra tables can end up in self.tables, but not in the
                # alias_map if they aren't in a join. That's OK. We skip them.
                continue
            clause_sql, clause_params = self.compile(from_clause)
            if isinstance(clause_sql, Iterable):
                result.extend(clause_sql)
            else:
                result.append(clause_sql)
            params.extend(clause_params)
        for t in self.query.extra_tables:
            alias, _ = self.query.table_alias(t)
            # Only add the alias if it's not already present (the table_alias()
            # call increments the refcount, so an alias refcount of one means
            # this is the only reference).
            if alias not in self.query.alias_map or self.query.alias_refcount[alias] == 1:
                result.append(', %s' % self.quote_name_unless_alias(alias))
        return result, params

    def resolve_columns(self, row, fields=()):
        # We need to convert values from database to correct django field representation.
        # For instance, if we defined a BooleanField field, django-wikibase do create a
        # smallint field into DB. When retrieving this field value, it's converted to
        # BooleanField again.
        index_start = len(self.query.extra_select)
        values = []
        for value, field in zip_longest(row[index_start:], fields):
            v = self.query.convert_values(
                value, field, connection=self.connection)
            values.append(v)
        return row[:index_start] + tuple(values)

    def compile(self, expression_node) -> Tuple:
        type_of_expression_node = type(expression_node)
        wb_database_connection = self.connection.connection

        if type_of_expression_node is Col:
            django_table_name = expression_node.field.model._meta.db_table
            if not(django_table_name in self.query.alias_refmap):
                self.query.alias_refmap[django_table_name] = []
            if not(expression_node.field.column in self.query.alias_refmap[django_table_name]):
                self.query.alias_refmap[django_table_name].append(
                    expression_node.field.column)
            # DjangoProperty(expression_node.field), []
            return f'?{expression_node.field.column}', []

        if type_of_expression_node is BaseTable:
            expressions = [wb_database_connection.expression_instance_of(
                expression_node.table_name)]
            for property_name in self.query.alias_refmap[expression_node.table_name]:
                expressions.append(wb_database_connection.expression_has_property(
                    expression_node.table_name, property_name))
            self.query.alias_refmap[expression_node.table_name].clear()
            return expressions, []

        if type_of_expression_node is WhereNode:
            sql, params = expression_node.as_sql(self, self.connection)
            return sql.replace(' AND ', ' && ').replace(' OR ', ' || '), params

        if type_of_expression_node is Exact:
            lhs_sql, params = self.compile(expression_node.lhs)
            rhs_sql, rhs_params = self.compile(expression_node.rhs)
            if rhs_params:
                params.extend(rhs_params)
            return f'{lhs_sql} = {rhs_sql}', params

        if type_of_expression_node in _BUILT_IN_TYPES:
            return wb_database_connection.sparql_parameter_value(expression_node), None

        if type_of_expression_node is Join:
            expressions = [
                wb_database_connection.expression_instance_of(
                    expression_node.parent_alias),
                wb_database_connection.expression_instance_of(
                    expression_node.table_alias)
            ]
            # Add a join condition for each pair of joining columns.
            for lhs_col, rhs_col in expression_node.join_cols:
                expressions.append(wb_database_connection.expression_has_property(
                    expression_node.parent_alias, lhs_col))
                expressions.append(wb_database_connection.expression_has_property(
                    expression_node.table_alias, rhs_col))
                expressions.append(
                    f'FILTER(str(?{lhs_col}) = str(?{rhs_col}))')
            return expressions, []

        if type_of_expression_node is OrderBy:
            order_by: OrderBy = expression_node
            sql, params = order_by.as_sql(
                self, self.connection, template='%(ordering)s(%(expression)s)')
            return sql, params

        if type_of_expression_node is RelatedIn:
            related_in: RelatedIn = expression_node
            if not related_in.rhs:
                return self.compile(related_in.lhs)
            sql, params = related_in.as_sql(self, self.connection)
            return sql, params

        if type_of_expression_node is RelatedExact:
            related_exact: RelatedExact = expression_node
            if not related_exact.rhs:
                return self.compile(related_exact.lhs)
            sql, params = related_exact.as_sql(self, self.connection)
            return sql, params

        if type_of_expression_node is RelatedGreaterThan:
            related_greater_than: RelatedGreaterThan = expression_node
            if not related_greater_than.rhs:
                return self.compile(related_greater_than.lhs)
            sql, params = related_greater_than.as_sql(self, self.connection)
            return sql, params

        if type_of_expression_node is RelatedGreaterThanOrEqual:
            related_greater_than_or_equal: RelatedGreaterThanOrEqual = expression_node
            if not related_greater_than_or_equal.rhs:
                return self.compile(related_greater_than_or_equal.lhs)
            sql, params = related_greater_than_or_equal.as_sql(self, self.connection)
            return sql, params

        if type_of_expression_node is RelatedIsNull:
            related_is_null: RelatedIsNull = expression_node
            if not related_is_null.rhs:
                return self.compile(related_is_null.lhs)
            sql, params = related_is_null.as_sql(self, self.connection)
            return sql, params

        if type_of_expression_node is RelatedLessThan:
            related_less_than: RelatedLessThan = expression_node
            if not related_less_than.rhs:
                return self.compile(related_less_than.lhs)
            sql, params = related_less_than.as_sql(self, self.connection)
            return sql, params

        if type_of_expression_node is RelatedLessThanOrEqual:
            related_less_than_or_equal: RelatedLessThanOrEqual = expression_node
            if not related_less_than_or_equal.rhs:
                return self.compile(related_less_than_or_equal.lhs)
            sql, params = related_less_than_or_equal.as_sql(self, self.connection)
            return sql, params

        raise NotImplementedError(
            f'Sorry, I can\'t perform compile for the node expression {expression_node}')

    def as_sql(self, with_limits=True, with_col_aliases=False):
        refcounts_before = self.query.alias_refcount.copy()
        self.query.alias_refmap = {f: [] for f in refcounts_before}
        models = {self.query.model}
        self.connection.use_models(models)
        try:
            if self.query.model:
                self.query.base_table = self.query.model._meta.db_table
            extra_select, order_by, group_by = self.pre_sql_setup()
            for_update_part = None
            # Is a LIMIT/OFFSET clause needed?
            with_limit_offset = with_limits and (
                self.query.high_mark is not None or self.query.low_mark)
            combinator = self.query.combinator
            features = self.connection.features

            result = self.connection.prefixes[:]

            if combinator:
                if not getattr(features, 'supports_select_{}'.format(combinator)):
                    raise NotSupportedError(
                        '{} is not supported on this database backend.'.format(combinator))
                result, params = self.get_combinator_sql(
                    combinator, self.query.combinator_all)
            else:
                distinct_fields, distinct_params = self.get_distinct()
                # This must come after 'select', 'ordering', and 'distinct'
                # (see docstring of get_from_clause() for details).
                from_, f_params = self.get_from_clause()
                try:
                    sparql_filter, w_params = self.compile(
                        self.where) if self.where is not None else ('', [])
                except EmptyResultSet:
                    if self.elide_empty:
                        raise
                    # Use a predicate that's always False.
                    sparql_filter, w_params = '0 = 1', []
                having, h_params = self.compile(
                    self.having) if self.having is not None else ("", [])
                result.append('SELECT')
                params = []

                if self.query.distinct:
                    distinct_result, distinct_params = self.connection.ops.distinct_sql(
                        distinct_fields,
                        distinct_params,
                    )
                    result += distinct_result
                    params += distinct_params

                out_cols = []
                col_idx = 1
                for col, (s_sql, s_params), alias in self.select + extra_select:
                    # if alias:
                    #     s_sql = '%s AS %s' % (
                    #         s_sql, self.connection.ops.quote_name(alias))
                    # elif with_col_aliases:
                    #     s_sql = '%s AS %s' % (
                    #         s_sql,
                    #         self.connection.ops.quote_name('col%d' % col_idx),
                    #     )
                    #     col_idx += 1
                    params.extend(s_params)
                    # out_cols.append(s_sql)

                    # update models
                    models.add(col.field.model)
                    out_cols.append(s_sql)

                # result += [', '.join(out_cols), 'FROM', *from_]
                deduplicated_from_clause = list(dict.fromkeys(from_))
                result += [' '.join(out_cols),
                           'WHERE {', '\n . '.join(deduplicated_from_clause)]
                params.extend(f_params)

                if self.query.select_for_update and self.connection.features.has_select_for_update:
                    if self.connection.get_autocommit():
                        raise TransactionManagementError(
                            'select_for_update cannot be used outside of a transaction.')

                    if with_limit_offset and not self.connection.features.supports_select_for_update_with_limit:
                        raise NotSupportedError(
                            'LIMIT/OFFSET is not supported with '
                            'select_for_update on this database backend.'
                        )
                    nowait = self.query.select_for_update_nowait
                    skip_locked = self.query.select_for_update_skip_locked
                    of = self.query.select_for_update_of
                    no_key = self.query.select_for_no_key_update
                    # If it's a NOWAIT/SKIP LOCKED/OF/NO KEY query but the
                    # backend doesn't support it, raise NotSupportedError to
                    # prevent a possible deadlock.
                    if nowait and not self.connection.features.has_select_for_update_nowait:
                        raise NotSupportedError(
                            'NOWAIT is not supported on this database backend.')
                    elif skip_locked and not self.connection.features.has_select_for_update_skip_locked:
                        raise NotSupportedError(
                            'SKIP LOCKED is not supported on this database backend.')
                    elif of and not self.connection.features.has_select_for_update_of:
                        raise NotSupportedError(
                            'FOR UPDATE OF is not supported on this database backend.')
                    elif no_key and not self.connection.features.has_select_for_no_key_update:
                        raise NotSupportedError(
                            'FOR NO KEY UPDATE is not supported on this '
                            'database backend.'
                        )
                    for_update_part = self.connection.ops.for_update_sql(
                        nowait=nowait,
                        skip_locked=skip_locked,
                        of=self.get_select_for_update_of_arguments(),
                        no_key=no_key,
                    )

                if for_update_part and self.connection.features.for_update_after_from:
                    result.append(for_update_part)

                if sparql_filter:
                    result.append(' . FILTER (%s)' % sparql_filter)
                    params.extend(w_params)

                result.append('}')

                grouping = []
                for g_sql, g_params in group_by:
                    grouping.append(g_sql)
                    params.extend(g_params)
                if grouping:
                    if distinct_fields:
                        raise NotImplementedError(
                            'annotate() + distinct(fields) is not implemented.')
                    order_by = order_by or self.connection.ops.force_no_ordering()
                    result.append('GROUP BY %s' % ', '.join(grouping))
                    if self._meta_ordering:
                        order_by = None
                if having:
                    result.append('HAVING %s' % having)
                    params.extend(h_params)

            if hasattr(self.query, 'explain_info'):
                result.insert(0, self.connection.ops.explain_query_prefix(
                    self.query.explain_info.format,
                    **self.query.explain_info.options
                ))

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                result.append('ORDER BY %s' % ' '.join(ordering))

            if with_limit_offset:
                result.append(self.connection.ops.limit_offset_sql(
                    self.query.low_mark, self.query.high_mark))

            if for_update_part and not self.connection.features.for_update_after_from:
                result.append(for_update_part)

            if self.query.subquery and extra_select:
                # If the query is used as a subquery, the extra selects would
                # result in more columns than the left-hand side expression is
                # expecting. This can happen when a subquery uses a combination
                # of order_by() and distinct(), forcing the ordering expressions
                # to be selected as well. Wrap the query in another subquery
                # to exclude extraneous selects.
                sub_selects = []
                sub_params = []
                for index, (select, _, alias) in enumerate(self.select, start=1):
                    if not alias and with_col_aliases:
                        alias = 'col%d' % index
                    if alias:
                        sub_selects.append("%s.%s" % (
                            self.connection.ops.quote_name('subquery'),
                            self.connection.ops.quote_name(alias),
                        ))
                    else:
                        select_clone = select.relabeled_clone(
                            {select.alias: 'subquery'})
                        subselect, subparams = select_clone.as_sql(
                            self, self.connection)
                        sub_selects.append(subselect)
                        sub_params.extend(subparams)
                return 'SELECT %s FROM (%s) subquery' % (
                    ', '.join(sub_selects),
                    ' '.join(result),
                ), tuple(sub_params + params)

        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)

        cmd_data = {**self._select_data, **{
            'models': [DjangoModel(model) for model in models],
            'sparql': '\n'.join(result)
        }}
        return Cmd('select', cmd_data), params


class SQLInsertCompiler(compiler.SQLInsertCompiler, Loggable):

    _add_items_data = {
        'model': None,
        'fields': None,
    }

    def as_sql(self, with_limits=True, with_col_aliases=False):
        # sql, params = super(compiler.SQLInsertCompiler, self).as_sql(
        #     with_limits=with_limits, with_col_aliases=with_col_aliases)
        self.debug('SQL insert compiler, as_sql, with limits %s, with col aliases %s',
                   with_limits, with_col_aliases)
        cmd_data = {**self._add_items_data, **{
            'model': DjangoModel(self.query.fields[0].model),
            'fields': [DjangoProperty(f) for f in self.query.fields]
        }}

        return [(Cmd('add_items', cmd_data),
                 [{f['property_name']: getattr(o, f['attribute_name']) for f in cmd_data['fields']} for o in self.query.objs])]


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, Loggable):

    _remove_items_data = {
        'sparql': None
    }

    def as_sql(self, with_limits=True, with_col_aliases=False):
        sql, params = super(compiler.SQLDeleteCompiler, self).as_sql(
            with_limits=False, with_col_aliases=with_col_aliases)
        self.debug('SQL delete compiler, as_sql, with limits %s, with col aliases %s',
                   with_limits, with_col_aliases)
        cmd_data = {**self._remove_items_data, **{'sparql': sql}}
        return [(Cmd('remove_items', cmd_data), (params))]


class SQLUpdateCompiler(SQLCompiler, Loggable):

    _set_items_data = {
        'model': None,
        'where': None
    }

    def _get_primary_key_value(self, where_node: WhereNode) -> Optional[Any]:
        for child in where_node.children:
            if isinstance(child, Exact) and child.lhs.field.primary_key:
                return child.rhs
        return None

    def as_sql(self, with_limits=True, with_col_aliases=False):
        # sql, params = super(compiler.SQLUpdateCompiler, self).as_sql(
        #     with_limits=False, with_col_aliases=with_col_aliases)
        self.debug('SQL update compiler, as_sql, with limits %s, with col aliases %s',
                   with_limits, with_col_aliases)
        self.pre_sql_setup()
        refcounts_before = self.query.alias_refcount.copy()
        self.query.alias_refmap = {f: [] for f in refcounts_before}

        primary_key_value = self._get_primary_key_value(self.query.where)

        sparql = None
        if primary_key_value is None:
            query = Query(self.query.model)
            query.add_fields(['id'])
            query.where = self.query.where
            sparql = query.as_sql(self, self.connection)

        cmd_data = {**self._set_items_data, **{
            'model': DjangoModel(self.query.model),
            'where': sparql
        }}

        values = [{f.attname: v for f, _, v in self.query.values}]
        for value in values:
            if self.query.model._meta.pk and primary_key_value:
                value[self.query.model._meta.pk.name] = primary_key_value

        return Cmd('set_items', cmd_data), values

    def execute_sql(self, result_type):
        """
        Execute the specified update. Return the number of rows affected by
        the primary update query. The "primary update query" is the first
        non-empty query that is executed. Row counts for any subsequent,
        related queries are not available.
        """
        cursor = super().execute_sql(result_type)
        try:
            rows = cursor.rowcount if cursor else 0
            is_empty = cursor is None
        finally:
            if cursor:
                cursor.close()
        for query in self.query.get_related_updates():
            aux_rows = query.get_compiler(self.using).execute_sql(result_type)
            if is_empty and aux_rows:
                rows = aux_rows
                is_empty = False
        return rows

    def pre_sql_setup(self):
        """
        If the update depends on results from other tables, munge the "where"
        conditions to match the format required for (portable) SQL updates.

        If multiple updates are required, pull out the id values to update at
        this point so that they don't change as a result of the progressive
        updates.
        """
        refcounts_before = self.query.alias_refcount.copy()
        # Ensure base table is in the query
        self.query.get_initial_alias()
        count = self.query.count_active_tables()
        if not self.query.related_updates and count == 1:
            return
        query = self.query.chain(klass=Query)
        query.select_related = False
        query.clear_ordering(True)
        query.extra = {}
        query.select = []
        query.add_fields([query.get_meta().pk.name])
        super().pre_sql_setup()

        must_pre_select = count > 1 and not self.connection.features.update_can_self_select

        # Now we adjust the current query: reset the where clause and get rid
        # of all the tables we don't need (since they're in the sub-select).
        self.query.where = self.query.where_class()
        if self.query.related_updates or must_pre_select:
            # Either we're using the idents in multiple update queries (so
            # don't want them to change), or the db backend doesn't support
            # selecting from the updating table (e.g. MySQL).
            idents = []
            for rows in query.get_compiler(self.using).execute_sql(MULTI):
                idents.extend(r[0] for r in rows)
            self.query.add_filter(('pk__in', idents))
            self.query.related_ids = idents
        else:
            # The fast path. Filters and updates in one query.
            self.query.add_filter(('pk__in', query))
        self.query.reset_refcounts(refcounts_before)



class SQLAggregateCompiler(compiler.SQLAggregateCompiler, Loggable):

    _agg_items_data = {
        'sparql': None
    }

    def as_sql(self, with_limits=True, with_col_aliases=False):
        sql, params = super(compiler.SQLAggregateCompiler, self).as_sql(
            with_limits=False, with_col_aliases=with_col_aliases)
        self.debug('SQL aggregate compiler, as_sql, with limits %s, with col aliases %s',
                   with_limits, with_col_aliases)
        cmd_data = {**self._agg_items_data, **{'sparql': sql}}
        return [(Cmd('agg_items', cmd_data), (params))]
