namespace NHibernate.SybaseAdsDriver
{
    using Dialect.Schema;
    using System;
    using System.Data;
    using System.Data.Common;

    // Metadata for connections using the Sybase Advantage Data ADO.NET provider
    public class AdsDataBaseMetaData : AbstractDataBaseSchema
    {
        public AdsDataBaseMetaData(DbConnection pObjConnection) : base(pObjConnection) {}

        public override ITableMetadata GetTableMetadata(DataRow rs, bool extras)
        {
            return new SybaseAdsTableMetaData(rs, this, extras);
        }

        public override DataTable GetTables(string catalog, string schemaPattern, string tableNamePattern, string[] types)
        {
            var restrictions = new[] {schemaPattern, tableNamePattern, null};
            DataTable objTbl = Connection.GetSchema("Tables", restrictions);
            return objTbl;
        }

        public override DataTable GetIndexInfo(string catalog, string schemaPattern, string tableName)
        {
            var restrictions = new[] {schemaPattern, tableName, null};
            DataTable objTbl = Connection.GetSchema("Indexes", restrictions);
            return objTbl;
        }

        public override DataTable GetIndexColumns(string catalog, string schemaPattern, string tableName, string indexName)
        {
            var restrictions = new[] {schemaPattern, tableName, indexName, null};
            DataTable objTbl = Connection.GetSchema("IndexColumns", restrictions);
            return objTbl;
        }

        public override DataTable GetColumns(string catalog, string schemaPattern, string tableNamePattern,
                                                string columnNamePattern)
        {
            var restrictions = new[] {schemaPattern, tableNamePattern, null};
            DataTable objTbl = Connection.GetSchema("Columns", restrictions);
            return objTbl;
        }

        public override DataTable GetForeignKeys(string catalog, string schema, string table)
        {
            var restrictions = new[] {schema, table, null};
            DataTable objTbl = Connection.GetSchema("ForeignKeys", restrictions);
            return objTbl;
        }
    }

    public class SybaseAdsTableMetaData : AbstractTableMetadata
    {
        public SybaseAdsTableMetaData(DataRow rs, IDataBaseSchema meta, bool extras) : base(rs, meta, extras) {}

        protected override IColumnMetadata GetColumnMetadata(DataRow rs)
        {
            return new SybaseAdsColumnMetaData(rs);
        }

        protected override string GetColumnName(DataRow rs)
        {
            return Convert.ToString(rs["COLUMN_NAME"]);
        }

        protected override string GetConstraintName(DataRow rs)
        {
            // There is no thing like a constraint name for ASA9 - so
            // we just use the column name here ...
            return Convert.ToString(rs["COLUMN_NAME"]);
        }

        protected override IForeignKeyMetadata GetForeignKeyMetadata(DataRow rs)
        {
            return new SybaseAdsForeignKeyMetaData(rs);
        }

        protected override IIndexMetadata GetIndexMetadata(DataRow rs)
        {
            return new SybaseAdsIndexMetaData(rs);
        }

        protected override string GetIndexName(DataRow rs)
        {
            return (string) rs["INDEX_NAME"];
        }

        protected override void ParseTableInfo(DataRow rs)
        {
            Catalog = null;
            Schema = Convert.ToString(rs["TABLE_SCHEMA"]);
            if (string.IsNullOrEmpty(Schema))
            {
                Schema = null;
            }
            Name = Convert.ToString(rs["TABLE_NAME"]);
        }
    }

    public class SybaseAdsColumnMetaData : AbstractColumnMetaData
    {
        public SybaseAdsColumnMetaData(DataRow rs) : base(rs)
        {
            Name = Convert.ToString(rs["COLUMN_NAME"]);
            object objValue = rs["COLUMN_SIZE"];
            if (objValue != DBNull.Value)
            {
                ColumnSize = Convert.ToInt32(objValue);
            }
            objValue = rs["PRECISION"];
            if (objValue != DBNull.Value)
            {
                NumericalPrecision = Convert.ToInt32(objValue);
            }
            Nullable = Convert.ToString(rs["IS_NULLABLE"]);
            TypeName = Convert.ToString(rs["DATA_TYPE"]);
        }
    }

    public class SybaseAdsIndexMetaData : AbstractIndexMetadata
    {
        public SybaseAdsIndexMetaData(DataRow rs) : base(rs)
        {
            Name = (string) rs["INDEX_NAME"];
        }
    }

    public class SybaseAdsForeignKeyMetaData : AbstractForeignKeyMetadata
    {
        public SybaseAdsForeignKeyMetaData(DataRow rs) : base(rs)
        {
            // There is no thing like a constraint name for ASA9 - so
            // we just use the column name here ...
            Name = (string) rs["COLUMN_NAME"];
        }
    }
}