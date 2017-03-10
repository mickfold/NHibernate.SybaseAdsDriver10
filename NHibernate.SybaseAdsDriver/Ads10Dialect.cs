namespace NHibernate.SybaseAdsDriver
{
    using NHibernate;
    using Dialect.Function;
    using Dialect.Schema;
    using SqlCommand;
    using System;
    using System.Data;
    using System.Data.Common;
    using Environment = Cfg.Environment;

    public class Ads10Dialect : Dialect.Dialect
    {
        public Ads10Dialect()
        {
            DefaultProperties[Environment.ConnectionDriver] = "Advantage.Nhibernate.Driver.AdsClientDriver";
            DefaultProperties[Environment.PrepareSql] = "false";

            RegisterCharacterTypeMappings();
            RegisterNumericTypeMappings();
            RegisterDateTimeTypeMappings();
            RegisterLargeObjectTypeMappings();
            RegisterFunctions();
            RegisterKeywords();
        }

        protected void RegisterCharacterTypeMappings()
        {
            // DBF and ADT table data types
            RegisterColumnType(DbType.AnsiStringFixedLength, 65534, "CHAR($l)");
            RegisterColumnType(DbType.AnsiString, 254, "VARCHAR($l)");
            RegisterColumnType(DbType.String, 254, "NVARCHAR($l)");

            // ADT only table data types
            RegisterColumnType(DbType.AnsiStringFixedLength, 65530, "CHARACTER($1)");
            RegisterColumnType(DbType.AnsiStringFixedLength, 65530, "CICHARACTER($1)");
            RegisterColumnType(DbType.AnsiString, 2147483647, "MEMO($1)");
            RegisterColumnType(DbType.AnsiString, 65000, "VARCHAR($1)");
            RegisterColumnType(DbType.StringFixedLength, 32500, "NCHAR($1)");
            RegisterColumnType(DbType.String, 32500, "NVARCHAR($l)");
            RegisterColumnType(DbType.String, 2147483647, "NMEMO($1)");
        }

        protected void RegisterNumericTypeMappings()
        {
            // DBF and ADT table data types
            RegisterColumnType(DbType.Boolean, "LOGICAL");
            RegisterColumnType(DbType.Decimal, 32, "NUMERIC($s, $s)");
            RegisterColumnType(DbType.Double, "DOUBLE");
            RegisterColumnType(DbType.Int32, "INTEGER");
            RegisterColumnType(DbType.Currency, "MONEY");

            // ADT only table data types
            RegisterColumnType(DbType.Int16, "SHORTINT");
            RegisterColumnType(DbType.Currency, "CURDOUBLE");
            RegisterColumnType(DbType.UInt32, "ROWVERSION");
        }

        protected void RegisterDateTimeTypeMappings()
        {
            // ADT and DBF table data types
            RegisterColumnType(DbType.Date, "DATE");
            RegisterColumnType(DbType.Date, "SHORTDATE");
            RegisterColumnType(DbType.Time, "TIME");
            RegisterColumnType(DbType.DateTime, "TIMESTAMP");

            // ADT table data types
            RegisterColumnType(DbType.DateTime, "MODTIME");
        }

        protected void RegisterLargeObjectTypeMappings()
        {
            // ADT and DBF table data types
            RegisterColumnType(DbType.Binary, "BINARY");

            // ADT only table data types
            RegisterColumnType(DbType.Binary, 2147483647, "IMAGE($1)");
            RegisterColumnType(DbType.Binary, 65000, "VARBINARY($1)");
            RegisterColumnType(DbType.Binary, "BINARY");
            RegisterColumnType(DbType.Binary, "RAW");
        }

        protected void RegisterFunctions()
        {
            RegisterMathFunctions();
            RegisterDateFunctions();
            RegisterStringFunctions();
            RegisterMiscellaneousFunctions();
        }

        protected void RegisterMathFunctions()
        {
            // mathematical functions
            RegisterFunction("abs", new StandardSQLFunction("abs", NHibernateUtil.Int32));
            RegisterFunction("acos", new StandardSQLFunction("acos", NHibernateUtil.Double));
            RegisterFunction("asin", new StandardSQLFunction("asin", NHibernateUtil.Double));
            RegisterFunction("atan", new StandardSQLFunction("atan", NHibernateUtil.Double));
            RegisterFunction("atan2", new StandardSQLFunction("atan2", NHibernateUtil.Double));
            RegisterFunction("ceiling", new StandardSQLFunction("ceiling"));
            RegisterFunction("cos", new StandardSQLFunction("cos", NHibernateUtil.Double));
            RegisterFunction("cot", new StandardSQLFunction("cot", NHibernateUtil.Double));
            RegisterFunction("degrees", new StandardSQLFunction("degrees", NHibernateUtil.Double));
            RegisterFunction("exp", new StandardSQLFunction("exp", NHibernateUtil.Double));
            RegisterFunction("floor", new StandardSQLFunction("floor"));
            RegisterFunction("log", new StandardSQLFunction("log", NHibernateUtil.Double));
            RegisterFunction("log10", new StandardSQLFunction("log10", NHibernateUtil.Double));
            RegisterFunction("mod", new StandardSQLFunction("mod"));
            RegisterFunction("pi", new NoArgSQLFunction("pi", NHibernateUtil.Double, true));
            RegisterFunction("power", new StandardSQLFunction("power", NHibernateUtil.Double));
            RegisterFunction("radians", new StandardSQLFunction("radians", NHibernateUtil.Double));
            RegisterFunction("rand", new StandardSQLFunction("rand", NHibernateUtil.Double));    
            RegisterFunction("round", new StandardSQLFunction("round"));
            RegisterFunction("sign", new StandardSQLFunction("sign"));
            RegisterFunction("sin", new StandardSQLFunction("sin", NHibernateUtil.Double));
            RegisterFunction("sqrt", new StandardSQLFunction("sqrt", NHibernateUtil.Double));
            RegisterFunction("tan", new StandardSQLFunction("tan", NHibernateUtil.Double));
            RegisterFunction("truncate", new StandardSQLFunction("truncate"));
        }

        protected void RegisterDateFunctions()
        {
            RegisterFunction("datename", new StandardSQLFunction("datename", NHibernateUtil.String));
            RegisterFunction("dayname", new StandardSQLFunction("dayname", NHibernateUtil.String));
            RegisterFunction("days", new StandardSQLFunction("days"));
            RegisterFunction("DAYOFWEEK", new StandardSQLFunction("dow", NHibernateUtil.Int32));
            RegisterFunction("getdate", new StandardSQLFunction("getdate", NHibernateUtil.Timestamp));
            RegisterFunction("hour", new StandardSQLFunction("hour", NHibernateUtil.Int32));
            RegisterFunction("minute", new StandardSQLFunction("minute", NHibernateUtil.Int32));
            RegisterFunction("month", new StandardSQLFunction("month", NHibernateUtil.Int32));
            RegisterFunction("monthname", new StandardSQLFunction("monthname", NHibernateUtil.String));            
            RegisterFunction("now", new NoArgSQLFunction("now", NHibernateUtil.Timestamp));
            RegisterFunction("quarter", new StandardSQLFunction("quarter", NHibernateUtil.Int32));
            RegisterFunction("second", new StandardSQLFunction("second", NHibernateUtil.Int32));            
            RegisterFunction("today", new NoArgSQLFunction("now", NHibernateUtil.Date));
            RegisterFunction("week", new StandardSQLFunction("week"));
            RegisterFunction("year", new StandardSQLFunction("year", NHibernateUtil.Int32));            
            

            // compatibility functions
            RegisterFunction("current_timestamp", new NoArgSQLFunction("CURRENT_TIMESTAMP", NHibernateUtil.Timestamp, true));
            RegisterFunction("current_time", new NoArgSQLFunction("CURRENT_TIME", NHibernateUtil.Time, true));
            RegisterFunction("current_date", new NoArgSQLFunction("CURRENT_DATE", NHibernateUtil.Date, true));
        }

        protected void RegisterStringFunctions()
        {
            RegisterFunction("ascii", new StandardSQLFunction("ascii", NHibernateUtil.Int32));
            RegisterFunction("bit_length", new StandardSQLFunction("bit_length", NHibernateUtil.Int32));
            RegisterFunction("char", new StandardSQLFunction("char", NHibernateUtil.String));        
            RegisterFunction("char_length", new StandardSQLFunction("char_length", NHibernateUtil.Int32));
            RegisterFunction("char2hex", new StandardSQLFunction("char2hex", NHibernateUtil.Binary));
            RegisterFunction("concat", new VarArgsSQLFunction(NHibernateUtil.String, "concat(", "+", ")"));
            RegisterFunction("insert", new VarArgsSQLFunction(NHibernateUtil.String, "insert(", ",", ")"));
            RegisterFunction("lcase", new StandardSQLFunction("lcase", NHibernateUtil.String));
            RegisterFunction("left", new StandardSQLFunction("left", NHibernateUtil.String));
            RegisterFunction("length", new StandardSQLFunction("length", NHibernateUtil.Int32));
            RegisterFunction("locate", new VarArgsSQLFunction(NHibernateUtil.Int32, "locate(", ",", ")"));
            RegisterFunction("lower", new StandardSQLFunction("lower", NHibernateUtil.String));
            RegisterFunction("ltrim", new StandardSQLFunction("ltrim", NHibernateUtil.String));
            RegisterFunction("octet_length", new StandardSQLFunction("octet_length", NHibernateUtil.Int32));
            
            /* look into how to put position function */
            RegisterFunction("position", new VarArgsSQLFunction(NHibernateUtil.String, "positio(", " in ", ")"));

            RegisterFunction("repeat", new StandardSQLFunction("repeat", NHibernateUtil.String));
            RegisterFunction("replace", new StandardSQLFunction("replace", NHibernateUtil.String));                
            RegisterFunction("right", new StandardSQLFunction("right", NHibernateUtil.String));
            RegisterFunction("rtrim", new StandardSQLFunction("rtrim", NHibernateUtil.String));
            
            RegisterFunction("substring", new VarArgsSQLFunction(NHibernateUtil.String, "substr(", ",", ")"));

            RegisterFunction("trim", new StandardSQLFunction("trim", NHibernateUtil.String));
            RegisterFunction("ucase", new StandardSQLFunction("ucase", NHibernateUtil.String));
            RegisterFunction("upper", new StandardSQLFunction("upper", NHibernateUtil.String));
        }

        protected void RegisterMiscellaneousFunctions()
        {
            RegisterFunction("coalesce", new VarArgsSQLFunction("coalesce(", ",", ")"));
            RegisterFunction("convert", new VarArgsSQLFunction("convert(", ",", ")"));
            RegisterFunction("errormsg", new NoArgSQLFunction("errormsg", NHibernateUtil.String, true));
            RegisterFunction("ifnull", new VarArgsSQLFunction("ifnull(", ",", ")"));
            RegisterFunction("isnull", new VarArgsSQLFunction("isnull(", ",", ")"));
            RegisterFunction("newidstring", new VarArgsSQLFunction("newidstring(", ",", ")"));
            RegisterFunction("plan", new VarArgsSQLFunction(NHibernateUtil.String, "SHOW PLAN FOR ", ",", ""));
            RegisterFunction("rowid", new NoArgSQLFunction("rowid", NHibernateUtil.String, false));
            RegisterFunction("rownum", new NoArgSQLFunction("rownum", NHibernateUtil.String, true));
        }

        protected void RegisterKeywords()
        {
            RegisterKeyword("fetch");
            RegisterKeyword("start");
            RegisterKeyword("sql_binary");
            RegisterKeyword("sql_bit");
            RegisterKeyword("sql_char");
            RegisterKeyword("sql_date");
            RegisterKeyword("sql_double");
            RegisterKeyword("sql_integer");
            RegisterKeyword("sql_money");
            RegisterKeyword("sql_numeric");
            RegisterKeyword("sql_time");
            RegisterKeyword("sql_timestamp");
            RegisterKeyword("sql_varbinary");
            RegisterKeyword("sql_varchar");
        }

        #region IDENTITY or AUTOINCREMENT support

        /// <summary>
        /// Advanatge support identity columns.
        /// </summary>
        public override bool SupportsIdentityColumns
        {
            get { return true; }
        }

        /// <summary>
        /// Gets the last inserted identity on the current connection.
        /// </summary>
        public override string IdentitySelectString
        {
            get { return "SELECT LASTAUTOINC(STATEMENT) FROM system.iota "; }
        }

        /// <summary>
        /// Advantage uses <tt>AUTOINC</tt> to identify an IDENTITY
        /// column in a <tt>CREATE TABLE</tt> statement.
        /// </summary>
        public override string IdentityColumnString
        {
            get { return "autoinc"; }
        }


        /// <summary>
        /// Advantage 10 does not support insert and return of identity in a single SQL statement
        /// </summary>
        public override bool SupportsInsertSelectIdentity
        {
            get { return false; }
        }

        #endregion

        #region LIMIT/OFFSET support

        /// <summary>
        /// Does Advantage have some kind of <c>LIMIT</c> syntax?
        /// </summary>
        /// <value>
        ///   <c>true</c>, as Advantage supports the SELECT TOP nn syntax.
        /// </value>
        public override bool SupportsLimit
        {
            get { return true; }
        }

        /// <summary>
        /// Does this Dialect support an offset?
        /// </summary>
        /// <value>
        ///   <c>true</c> as Advantage supports paging.
        /// </value>
        public override bool SupportsLimitOffset
        {
            get { return true; }
        }

        /// <summary>
        /// Can parameters be used for a statement containing a LIMIT?
        /// </summary>
        /// <value>
        ///     <c>false</c> as Advantage does not support parameters in a limit statement, i.e. <c>SELECT TOP :p0  * FROM employees</c> is invalid.
        /// </value>
        public override bool SupportsVariableLimit
        {
            get { return false; }
        }

        private static int GetAfterSelectInsertPoint(SqlString sql)
        {
            // Assume no common table expressions with the statement.
            if (sql.StartsWithCaseInsensitive("select distinct"))
            {
                return 15;
            }
            else if (sql.StartsWithCaseInsensitive("select"))
            {
                return 6;
            }
            return 0;
        }



        /// <summary>
        /// Gets the limit string in the form of <c>SELECT TOP 10 START AT 11 * FROM employees</c>
        /// </summary>
        /// <param name="queryString">The query string.</param>
        /// <param name="offset">The offset.</param>
        /// <param name="limit">The limit.</param>
        /// <returns></returns>
        public override SqlString GetLimitString(SqlString queryString, SqlString offset, SqlString limit)
        {
            int insertionPoint = GetAfterSelectInsertPoint(queryString);

            if (insertionPoint > 0)
            {
                SqlStringBuilder limitBuilder = new SqlStringBuilder();
                limitBuilder.Add("select");
                if (insertionPoint > 6)
                {
                    limitBuilder.Add(" distinct ");
                }
                limitBuilder.Add(" top ");
                limitBuilder.Add(limit);
                if (offset != null)
                {
                    limitBuilder.Add(" start at ");
                    limitBuilder.Add(offset);
                }
                limitBuilder.Add(queryString.Substring(insertionPoint));
                return limitBuilder.ToSqlString();
            }
            else
            {
                return queryString; // unchanged
            }
        }

        #endregion

        #region Lock acquisition support

        /// <summary>
        /// SAP ADS 10 does not support row locks.
        /// </summary>
        public override string GetForUpdateString(LockMode lockMode)
        {
            return String.Empty;
        }

        public override bool ForUpdateOfColumns
        {
            get { return false; }
        }


        public override bool SupportsOuterJoinForUpdate
        {
            get { return true; }
        }


        /// <summary>
        /// Snapshot isolation mode is not supported by ADS 10.
        /// </summary>
        public override bool DoesReadCommittedCauseWritersToBlockReaders
        {
            get { return true; }
        }

        /// <summary>
        /// We assume that applications using this dialect are NOT using
        /// SQL Anywhere's snapshot isolation modes.
        /// </summary>
        public override bool DoesRepeatableReadCauseReadersToBlockWriters
        {
            get { return true; }
        }

        // SQL Anywhere-specific query syntax

        public override bool SupportsCurrentTimestampSelection
        {
            get { return true; }
        }

        public override string CurrentTimestampSQLFunctionName
        {
            get { return "getdate"; }
        }

        public override bool IsCurrentTimestampSelectStringCallable
        {
            get { return false; }
        }

        public override string CurrentTimestampSelectString
        {
            get { return "select getdate()"; }
        }

        /// <summary>
        /// SQL Anywhere supports both double quotes or '[' (Microsoft syntax) for
        /// quoted identifiers.
        ///
        /// Note that quoted identifiers are controlled through
        /// the QUOTED_IDENTIFIER connection option.
        /// </summary>
        public override char CloseQuote
        {
            get { return '"'; }
        }

        /// <summary>
        /// SQL Anywhere supports both double quotes or '[' (Microsoft syntax) for
        /// quoted identifiers.
        /// </summary>
        public override char OpenQuote
        {
            get { return '"'; }
        }

        #endregion

        #region Informational metadata

        public override bool SupportsEmptyInList
        {
            get { return false; }
        }

        public override bool SupportsResultSetPositionQueryMethodsOnForwardOnlyCursor
        {
            get { return false; }
        }

        public override bool SupportsExistsInSelect
        {
            get { return false; }
        }

        public override bool AreStringComparisonsCaseInsensitive
        {
            get { return false; }
        }

        #endregion

        #region DDL support

        public override bool SupportsCommentOn
        {
            get { return false; }
        }

        public override int MaxAliasLength
        {
            get { return 127; }
        }

        public override string AddColumnString
        {
            get { return "add "; }
        }

        public override string NullColumnString
        {
            get { return " null"; }
        }

        public override bool QualifyIndexName
        {
            get { return false; }
        }

        public override string NoColumnsInsertString
        {
            get { return " values (default) "; }
        }

        public override bool DropConstraints
        {
            get { return false; }
        }

        public override string DropForeignKeyString
        {
            get { return " drop foreign key "; }
        }

        #endregion

        #region Temporary table support

        public override bool SupportsTemporaryTables
        {
            get { return true; }
        }

        /// <summary>
        /// In SQL Anywhere, the syntax, DECLARE LOCAL TEMPORARY TABLE ...,
        /// can also be used, which creates a temporary table with procedure scope,
        /// which may be important for stored procedures.
        /// </summary>
        public override string CreateTemporaryTableString
        {
            get { return "create local temporary table "; }
        }

        /// <summary>
        /// Assume that temporary table rows should be preserved across COMMITs.
        /// </summary>
        public override string CreateTemporaryTablePostfix
        {
            get { return " on commit preserve rows "; }
        }

        /// <summary>
        /// SQL Anywhere 10 does not perform a COMMIT upon creation of
        /// a temporary table.  However, it does perform an implicit
        /// COMMIT when creating an index over a temporary table, or
        /// upon ALTERing the definition of temporary table.
        /// </summary>
        public override bool? PerformTemporaryTableDDLInIsolation()
        {
            return null;
        }

        #endregion

        #region Callable statement support

        /// <summary>
        /// SQL Anywhere does support OUT parameters with callable stored procedures.
        ///  </summary>
        public override int RegisterResultSetOutParameter(DbCommand statement, int position)
        {
            return position;
        }

        public override DbDataReader GetResultSet(DbCommand statement)
        {
            DbDataReader rdr = statement.ExecuteReader();
            return rdr;
        }

        #endregion

        public override string SelectGUIDString
        {
            get { return "select newid()"; }
        }

        public override bool SupportsUnionAll
        {
            get { return true; }
        }

        public override IDataBaseSchema GetDataBaseSchema(DbConnection connection)
        {
            return new AdsDataBaseMetaData(connection);
        }
    }
}
