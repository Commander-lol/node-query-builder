/** @module src/database/QueryBuilder */

const hex = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f']
/**
 * Generate a hexadecimal string of the given length in a synchronous and insecure manner.
 * The return value of this function should not be used in a secure context, but can be
 * used to easily generate a unique identifier in scenarios where security is not important
 * and synchronous execution is a requirement
 *
 * @param {number} length The number of characters to generate. Unlike
 * the {@link module:core/utils/crypto.secureHexString|secureHexString} function, the length
 * specified is the exact length of the output string.
 *
 * @returns {string} A string of hexadecimal characters. Not guaranteed to be secure or insecure
 */
function insecureHexString(length) {
	const buffer = []
	for (let i = 0; i < length; i += 1) {
		const char = hex[Math.floor(Math.random() * hex.length)]
		buffer.push(char)
	}
	return buffer.join('')
}

/**
 * @class
 * @abstract
 * @classdesc The base type for all SQL objects. Specifies the interface that can be used to serialize an object into
 * a valid database query. Typically, providing an object derived from this class as a parameter in the QueryBuilder
 * will cause the QueryBuilder to use that type's custom serialization instead of the standard interpretation for that
 * field type
 */
class Sql {
	/**
	 * Generate an SQL string based on the SQL object type. Any parameters will have replacements generated for them,
	 * which will be interpolated into the return value of this function instead of the parameter itself
	 *
	 * @returns {string} A valid SQL string with replacement placeholders
	 */
	toSql() { return '' }

	/**
	 * Get the replacements generated for this type's SQL string, based on the parameters it was constructed with. These
	 * should be passable directly into the `Sequelize.query` options object
	 *
	 * @returns {{}} An Object mapping replacement placeholder names to concrete values that will be used in the query
	 */
	getReplacements() { return { } }
}

// --- Utilities

/**
 * Processes a list of objects that could either be {@link module:src/database/QueryBuilder~Sql|Sql} objects or some
 * non-important value. The result is an object containing all of the replacements for the values in the list
 *
 * @param {Array.<*|module:src/database/QueryBuilder~Sql>} list The array of values to merge
 * @returns {{}} An object containing all of the replacements for `Sql` objects in the list, having ignored any values
 * not of that type
 */
function sqlListToReplacements(list) {
	return list
		.map(item => {
			if (item instanceof Sql) {
				return item.getReplacements()
			}
			return null
		})
		.filter(Boolean)
		.reduce((reps, current) => Object.assign({}, reps, current), {})
}

/**
 * Processes a list of objects that could either be {@link module:src/database/QueryBuilder~Sql|Sql} objects or literal
 * SQL strings. The result is a serialized string containing the query values of an sql or string objects joined together
 *
 * @param {Array.<string|module:src/database/QueryBuilder~Sql>} list The array of values to serialize
 * @param {string} [concatBy=, ] The value to use when joining the sections of the SQL list
 * @returns {string} The serialized SQL string
 */
function concatPossibleSqlList(list, concatBy = ', ') {
	return list.map(item => {
		if (item instanceof Sql) {
			return item.toSql()
		}
		return item
	}).join(concatBy)
}
// ---

// --- Base Types
/**
 * @class
 * @classdesc Represents a raw sql string that will not be processed in any way, other than to join any parts provided.
 * This type is useful for circumventing any custom processing for query parts that cannot be represented faithfully by
 * other `Sql` constructs
 *
 * @extends module:src/database/QueryBuilder~Sql
 */
class Raw extends Sql {
	constructor(...parts) {
		super()
		this._parts = parts
	}

	toSql() {
		return this._parts.map(part => {
			if (part instanceof Sql) {
				return part.toSql()
			}
			return part
		}).join(' ')
	}

	getReplacements() {
		return this._parts
			.map(part => {
				if (part instanceof Sql) {
					return part.getReplacements()
				}
				return null
			})
			.filter(Boolean)
			.reduce((reps, current) => Object.assign({}, reps, current), {})
	}
}

/**
 * @class
 * @classdesc Represents an SQL column name. Column names typically receive different handling by SQL engines to
 * differentiate them from values because they usually appear in the same positions in a query. The Column name
 * will be wrapped with quotes, preventing the names from clashing with built in keywords - namespacing is also
 * supported, so table and column names will be wrapped separately. The asterisk character should never be wrapped
 * with quotes, and will receive special handling when encountered as a column name
 *
 * @extends module:src/database/QueryBuilder~Sql
 */
class Column extends Sql {
	constructor(name) {
		super()
		if (name === '*') {
			this._name = name
		} else {
			const parts = name.split('.')
			this._name = parts.map(part => {
				if (part === '*') {
					return '*'
				}
				return `"${ part }"`
			}).join('.')
		}
	}
	toSql() { return this._name }
}

class Literal extends Sql {
	constructor(value) {
		super()
		this._ident = `lit${ insecureHexString(8) }`
		this._replacements = { [this._ident]: value }
	}

	toSql() {
		return `:${ this._ident }`
	}

	getReplacements() {
		return this._replacements
	}
}

/**
 * @class
 * @classdesc Represents a data cast from a value to another type. No check is actually made by the QueryBuilder to ensure
 * that the `lvalue` is compatible with the type being cast to
 *
 * @extends module:src/database/QueryBuilder~Sql
 */
class Cast extends Sql {
	constructor(statement, type) {
		super()
		this._replacements = {}
		if (statement instanceof Sql) {
			this._statement = statement
			Object.assign(this._replacements, statement.getReplacements())
		} else {
			this._statement = new Column(statement)
		}

		this._type = type
	}

	toSql() { return `${ this._statement.toSql() }::${ this._type }` }
	getReplacements() { return this._replacements }
}

/**
 * @class
 * @abstract
 * @classdesc The base class for representing a series of related conditional queries that should be grouped together.
 * This class doesn't include any operators, and as such should not be used directly. Instead, it provides the common
 * serialization logic for all child joinder types
 *
 * @extends module:src/database/QueryBuilder~Sql
 */
class Joinder extends Sql {
	/**
	 * Get the joinder term to use in the SQL query. This would be a value such as `AND` for the condition
	 * `(foo AND bar AND baz)`
	 * @returns {string}
	 * @protected
	 */
	_term() { return '' }

	constructor(...conditions) {
		super()
		this._conditions = conditions
	}

	toSql() {
		if (this._conditions.length < 2) {
			const [condition] = this._conditions
			if (condition instanceof Sql) {
				return condition.toSql()
			}
			return condition
		}
		return `(${ concatPossibleSqlList(this._conditions, ` ${ this._term() } `) })`
	}

	getReplacements() {
		return this._conditions
			.map(sql => {
				if (sql instanceof Sql) {
					return sql.getReplacements()
				}
				return null
			})
			.filter(Boolean)
			.reduce((reps, current) => Object.assign({}, reps, current), {})
	}
}

class And extends Joinder {
	_term() {
		return 'AND'
	}
}

class Or extends Joinder {
	_term() {
		return 'OR'
	}
}

class Fn extends Sql { // Called Fn to avoid conflicting with existing 'Function' constructor type
	constructor(name, ...args) {
		super()
		this._name = name
		this._replacements = {}
		this._args = args.map(arg => {
			if (arg instanceof Sql) {
				Object.assign(this._replacements, arg.getReplacements())
				return arg
			}
			const ident = `func${ insecureHexString(8) }`
			this._replacements[ident] = arg
			return `:${ ident }`
		})
	}

	toSql() {
		const buffer = [this._name, '(']
		const args = []
		this._args.forEach(arg => {
			if (arg instanceof Sql) {
				args.push(arg.toSql())
			} else {
				args.push(arg)
			}
		})

		buffer.push(args.join(', '))
		buffer.push(')')

		return buffer.join('')
	}

	getReplacements() {
		return this._replacements
	}
}

/**
 * @class
 * @classdesc Represents the literal value `NULL`. This class is used to prevent any alternative processing of a
 * javascript literal `null`, for example in the `rvalue` position of a `Where` object when performing a
 * `WHERE column IS NULL` query
 */
class Null extends Sql {
	/**
	 * @returns {string} The literal string `NULL`
	 */
	toSql() {
		return 'NULL'
	}
}
// ---

// --- Query Types
/**
 * @class
 * @classdesc Represents an SQL `WHERE` clause. The default assumption is a simple condition, where you are checking
 * a database column against a static value; the static value will be assigned to a replacement. This class also supports
 * the more complex use cases of a `WHERE` clause. e.g. a function call as the entire clause of the form `WHERE my_func()`
 *
 * @extends module:src/database/QueryBuilder~Sql
 */
class Where extends Sql {
	/**
	 * Create a new Where object, representing a filter for the query
	 *
	 * @param {string|module:src/database/QueryBuilder~Sql} left The left hand clause of the condition. Where a string is provided, it will be interpreted
	 * as a column name. If an Sql object is provided, it will be serialised as-is
	 *
	 * @param {*|module:src/database/QueryBuilder~Sql} [right = null] The right hand value of the condition. When a regular value is provided, it will be
	 * interpreted as a literal value and will generate a replacement. If an Sql object is provided, it will be serialised
	 * as-is
	 *
	 * @param {string} [operator = =] The operator to use for the condition. This should be any of the SQL operators that
	 * evaluate to a boolean value (e.g. =, >, &&, etc) for the data types involved in the `WHERE` clause. Providing an
	 * explicit null value
	 */
	constructor(left, right = null, operator = '=') {
		super()
		this._prefix = ''
		this._suffix = ''
		this._replacements = {}

		if (left instanceof Sql) {
			this._left = left
			Object.assign(this._replacements, this._left.getReplacements())
		} else {
			this._left = new Column(left)
		}

		if (right != null) {
			if (right instanceof Sql) {
				this._right = right
				Object.assign(this._replacements, this._right.getReplacements())
			} else {
				const ident = `where${ insecureHexString(8) }`
				this._replacements[ident] = right
				this._right = `:${ ident }`
			}
		}

		this._operator = operator
	}

	/**
	 * @inheritdoc
	 */
	getReplacements() { return this._replacements }

	/**
	 * @inheritdoc
	 */
	toSql() {
		const buffer = [this._prefix]
		const replacements = {}
		if (this._left instanceof Sql) {
			Object.assign(replacements, this._left.getReplacements())
			buffer.push(this._left.toSql())
		} else if (this._left != null) {
			buffer.push(this._left)
		}

		if (this._operator != null) {
			buffer.push(` ${ this._operator } `)
		}

		if (this._right instanceof Sql) {
			Object.assign(replacements, this._right.getReplacements())
			buffer.push(this._right.toSql())
		} else if (this._right != null) {
			buffer.push(this._right)
		}
		buffer.push(this._suffix)

		return buffer.join('')
	}
}

/**
 * @class
 * @classdesc Represents a column to be selected in the query. Using an explicit Select object instead of
 * passing a string to the QueryBuilder's `select` method will allow you to correctly alias the column with
 * another name while also using other `Sql` derived objects to specify the selected column
 *
 * @extends module:src/database/QueryBuilder~Sql
 */
class Select extends Sql {
	/**
	 * Create a new `Select` object with an optional alias.
	 *
	 * @param {string|module:src/database/QueryBuilder~Sql} column The column to select from the database. If a string
	 * is provided, it will be processed as a {@link module:src/database/QueryBuilder~Column|Column}, and if an Sql
	 * object is provided, it will be serialized as-is
	 *
	 * @param {string} [as = null] The alias for this column. If not provided, the default column name will be used.
	 * For simple `SELECT`s, this will be equal to the column name, but for more complicated selects (i.e. selects
	 * involving function calls or aggregates) the default column name is an amalgamation of the component parts. This
	 * may be less than ideal for accessing the results and, in those cases, providing an alias is recommended
	 */
	constructor(column, as = null) {
		super()
		if (column instanceof Sql) {
			this._column = column
		} else {
			this._column = new Column(column)
		}

		this._as = as
	}

	toSql() {
		const buffer = [this._column.toSql()]

		if (this._as != null) {
			buffer.push('as')
			buffer.push(this._as)
		}

		return buffer.join(' ')
	}

	getReplacements() {
		return this._column.getReplacements()
	}
}
// ---

// -- Relational Types
class SubSelect extends Sql {
	constructor(builderFn, name = null) {
		super()
		this._name = name == null ? null : new Column(name)
		const subBuilder = new QueryBuilder() // eslint-disable-line no-use-before-define
		this._sql = builderFn(subBuilder)
		if (this._sql == null || typeof this._sql !== 'string') {
			throw new TypeError('Nested select function must return select string')
		}
		this._replacements = subBuilder.getReplacements()
	}

	toSql() {
		const buffer = [`(${ this._sql })`]
		if (this._name != null) {
			buffer.push('AS')
			buffer.push(this._name.toSql())
		}
		return buffer.join(' ')
	}
	getReplacements() { return this._replacements }
}

class Union extends Sql {
	constructor(builderFns, type = null) {
		super()
		this._type = type
		this._subs = []
		this._replacements = []

		builderFns.forEach(builder => {
			const qb = new QueryBuilder() // eslint-disable-line no-use-before-define
			const sql = builder(qb)
			if (sql == null || typeof sql !== 'string') {
				throw new TypeError('Nested select function must return select string')
			}
			this._subs.push(sql)
			this._replacements.push(qb.getReplacements())
		})
	}

	toSql() {
		let joiner = ['UNION', this._type].filter(Boolean).join(' ')
		joiner = ` ${ joiner } ` // Adds spaces
		return this._subs.join(joiner)
	}

	getReplacements() {
		return this._replacements.reduce((acc, c) => ({ ...acc, ...c }), {})
	}
}

class Join extends Sql {

	prefix() { return '' }
	suffix() { return '' }

	constructor(table, condition = null, tableAlias = null) {
		if (!(condition == null || condition instanceof Sql)) {
			throw new TypeError('Must provide a valid WHERE clause for join condition')
		}
		super()

		this._replacements = {}
		if (tableAlias != null) {
			this._alias = new Column(tableAlias)
		} else {
			this._alias = null
		}

		if (table instanceof Sql) {
			this._table = table
			Object.assign(this._replacements, table.getReplacements())
		} else {
			this._table = new Column(table)
		}

		if (condition != null) {
			this._condition = condition
			Object.assign(this._replacements, condition.getReplacements())
		} else {
			this._condition = null
		}
	}

	toSql() {
		const buffer = [
			this.prefix(),
			'JOIN',
			this.suffix(),
			this._table,
			this._alias,
			this._condition == null ? null : 'ON',
			this._condition,
		].filter(Boolean)

		return concatPossibleSqlList(buffer, ' ')
	}

	getReplacements() {
		return this._replacements
	}
}

class LateralCrossJoin extends Join {
	prefix() { return 'CROSS' }
	suffix() { return 'LATERAL' }
}
class LateralLeftJoin extends Join {
	prefix() { return 'LEFT' }
	suffix() { return 'LATERAL' }
}
class OuterJoin extends Join {
	prefix() { return 'OUTER' }
}
class LeftOuterJoin extends Join {
	prefix() { return 'LEFT OUTER' }
}
class FullOuterJoin extends Join {
	prefix() { return 'FULL OUTER' }
}
class InnerJoin extends Join {
	prefix() { return 'INNER' }
}
// ---

// --- Conditional Types
class When extends Sql {
	constructor(condition, select) {
		if (!(condition instanceof Where) || !(select instanceof SubSelect)) {
			throw new TypeError('When clause must be constructed from a WHERE and a SUBSELECT clause')
		}
		super()

		this._replacements = {}
		this._condition = condition
		Object.assign(this._replacements, condition.getReplacements())

		this._select = select
		Object.assign(this._replacements, select.getReplacements())
	}

	toSql() {
		return `WHEN ${ this._condition.toSql() } THEN ${ this._select.toSql() }`
	}
	getReplacements() {
		return this._replacements
	}
}

class Else extends Sql {
	constructor(select) {
		if (!(select instanceof SubSelect)) {
			throw new TypeError('Else clause must be constructed from a SUBSELECT clause')
		}
		super()
		this._select = select
		this._replacements = select.getReplacements()
	}

	toSql() {
		return `ELSE ${ this._select.toSql() }`
	}

	getReplacements() {
		return this._replacements
	}
}

class Case extends Sql {
	static Else(...args) { return new Else(...args) }
	static When(...args) { return new When(...args) }
	constructor(name, whenBranches, elseBranch = null) {
		super()

		this._name = new Column(name)
		this._replacements = {}
		this._whens = whenBranches
		Object.assign(this._replacements, sqlListToReplacements(whenBranches))

		this._else = elseBranch
		if (elseBranch != null) {
			Object.assign(this._replacements, elseBranch.getReplacements())
		}
	}

	toSql() {
		const buffer = [
			'CASE',
			concatPossibleSqlList(this._whens, ' '),
		]

		if (this._else != null) {
			buffer.push(this._else.toSql())
		}

		buffer.push('END AS')
		buffer.push(this._name.toSql())

		return buffer.join(' ')
	}
}
// ---

/**
 * @borrows Where as QueryBuilder.Where
 */
class QueryBuilder {
	/**
	 * Create a new Where object, specifying a conditional clause for filtering the rows found by the query
	 *
	 * @returns {Where} A Where clause object
	 */
	static Where(...args) { return new Where(...args) }
	static Column(...args) { return new Column(...args) }
	static Literal(value) { return new Literal(value) }
	static Raw(...args) { return new Raw(...args) }
	static Select(...args) { return new Select(...args) }
	static SubSelect(builderFn, name) { return new SubSelect(builderFn, name) }

	static Fn(...args) { return new Fn(...args) }
	static Null() { return new Null() }
	static Cast(...args) { return new Cast(...args) }
	static And(...args) { return new And(...args) }
	static Or(...args) { return new Or(...args) }

	static Case(...args) { return new Case(...args) }
	static When(...args) { return Case.When(...args) }
	static Else(...args) { return Case.Else(...args) }

	static Join(...args) { return new Join(...args) }
	static LateralCrossJoin(...args) { return new LateralCrossJoin(...args) }
	static LateralLeftJoin(...args) { return new LateralLeftJoin(...args) }
	static InnerJoin(...args) { return new InnerJoin(...args) }
	static OuterJoin(...args) { return new OuterJoin(...args) }
	static LeftOuterJoin(...args) { return new LeftOuterJoin(...args) }
	static FullOuterJoin(...args) { return new FullOuterJoin(...args) }

	static Union(...args) { return new Union(...args) }
	static UnionAll(...fns) { return new Union(fns, 'ALL') }
	static UnionDistinct(...fns) { return new Union(fns) } // Distinct == default

	constructor() {
		this._where = []
		this._join = []
		this._groupBy = []
		this._order = []
		this._select = []
		this._limit = null
		this._offset = null

		this._from = null

		this._replacements = {}
	}

	table(name) {
		this._from = name
		return this
	}

	_processSelectedField(field) {
		if (field == null) {
			return null
		}
		if (typeof field === 'string') {
			return new Column(field)
		}
		if (field.hasOwnProperty('fnCall')) {
			Object.assign(this._replacements, field.replacements || {})
			return field.fnCall
		}
		if (field instanceof Sql) {
			return field
		}
		return String(field)
	}

	_generateWhere() {
		if (this._where.length < 1) {
			return ''
		}

		return `WHERE ${ new And(...this._where).toSql() }`
	}
	_generateJoin() {
		return concatPossibleSqlList(this._join, ' ')
	}
	_generateFrom() {
		if (this._from == null) {
			return ''
		}
		if (this._from instanceof Sql) {
			return `FROM ${ this._from.toSql() }`
		}
		return `FROM "${ this._from }"`
	}
	_generateLimit() {
		if (this._limit == null) {
			return ''
		}
		return `LIMIT ${ this._limit }`
	}
	_generateOffset() {
		if (this._offset == null) {
			return ''
		}
		return `OFFSET ${ this._offset }`
	}
	_generateGroupBy() {
		if (this._groupBy.length < 1) {
			return ''
		}

		return `GROUP BY ${ concatPossibleSqlList(this._groupBy) }`
	}
	_generateOrder() {
		if (this._order.length < 1) {
			return ''
		}

		return `ORDER BY ${ concatPossibleSqlList(this._order) }`
	}

	property(...fields) {
		this._select = this._select.concat(fields.map(f => this._processSelectedField(f)))
		return this
	}

	where(left, right = null, operator = '=') {
		if (left instanceof Where) {
			this._where.push(left)
		} else {
			this._where.push(new Where(left, right, operator))
		}
		return this
	}

	limit(number) {
		this._limit = number
		return this
	}

	offset(number) {
		this._offset = number
		return this
	}

	join(...args) {
		if (args.length === 1) {
			const [join] = args
			if (!(join instanceof Join)) {
				throw new TypeError('Cannot use non-join object to directly create JOIN')
			}
			this._join.push(join)
		} else {
			this._join.push(new Join(...args))
		}
		return this
	}

	delete() {
		if (this._where.length < 1) {
			// While *technically* you can have a delete without a filter, that doesn't sound
			// like a very good idea, now does it?
			throw new Error('Can not create delete statement without at least one condition')
		}
		return `DELETE ${ this._generateFrom() } ${ this._generateWhere() }`.trim()
	}

	select(...fields) {
		this._select = this._select.concat(fields.map(f => this._processSelectedField(f)))
		const select = concatPossibleSqlList(this._select)
		const buffer = [
			'SELECT',
			select,
			this._generateFrom(),
			this._generateJoin(),
			this._generateWhere(),
			this._generateGroupBy(),
			this._generateOrder(),
			this._generateLimit(),
			this._generateOffset(),
		].filter(Boolean)

		return buffer.join(' ').trim()
	}


	order(...clauses) {
		this._order = this._order.concat(clauses)
		return this
	}

	groupBy(...clauses) {
		this._groupBy = this._groupBy.concat(clauses)
		return this
	}

	/**
	 * A shortcut for adding a paranoid record check to a query. Paranoid models use a `deleted` timestamp
	 * to mark rows as deleted, instead of deleting a row from the database
	 *
	 * @param {string} [fieldName = deleted_at] The name of the column containing the timestamp. Will usually be
	 * `deleted_at`, but in some cases this my need to be customised
	 */
	paranoid(fieldName = 'deleted_at') {
		this.where(fieldName, new Null(), 'IS')
		return this
	}

	getReplacements() {
		return Object.assign(
			{},
			this._replacements,
			sqlListToReplacements(this._select),
			sqlListToReplacements(this._join),
			sqlListToReplacements(this._where),
			sqlListToReplacements(this._order)
		)
	}
}

module.exports = QueryBuilder
