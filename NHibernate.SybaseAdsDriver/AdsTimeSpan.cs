using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using NHibernate.Engine;
using NHibernate.SqlTypes;
using NHibernate.Type;

namespace NHibernate.AdsDriver
{
	/// <summary>
	/// Maps a <see cref="System.TimeSpan" /> Property to an <see cref="DbType.Time" /> column 
	/// This is an extra way to map a <see cref="DbType.Time"/>. You already have <see cref="TimeType"/>
	/// but mapping against a <see cref="DateTime"/>.
	/// </summary>	
	[Serializable]
	class AdsTimeSpan : PrimitiveType, IVersionType, ILiteralType
	{
		private static readonly DateTime BaseDateValue = new DateTime(1753, 01, 01);

		public AdsTimeSpan()
			: base(SqlTypeFactory.Time)
		{
		}

		public override string Name
		{
			get { return "TimeAsTimeSpan"; }
		}

		public override object Get(IDataReader rs, int index)
		{
			try
			{
				object value = rs[index];
				if (value is TimeSpan)
					return (TimeSpan)value;

				return ((DateTime)value).TimeOfDay;
			}
			catch (Exception ex)
			{
				throw new FormatException(string.Format("Input string '{0}' was not in the correct format.", rs[index]), ex);
			}
		}

		public override object Get(IDataReader rs, string name)
		{
			try
			{
				object value = rs[name];
				if (value is TimeSpan) //For those dialects where DbType.Time means TimeSpan.
					return (TimeSpan)value;

				return ((DateTime)value).TimeOfDay;
			}
			catch (Exception ex)
			{
				throw new FormatException(string.Format("Input string '{0}' was not in the correct format.", rs[name]), ex);
			}
		}

		public override void Set(IDbCommand st, object value, int index)
		{
			var param = (IDataParameter)st.Parameters[index];
			if (param.DbType == DbType.DateTime)
			{
				DateTime date = BaseDateValue.AddTicks(((TimeSpan)value).Ticks);
				param.Value = date;
			}
			else
			{
				param.Value = value;
			}
		}

		public override System.Type ReturnedClass
		{
			get { return typeof(TimeSpan); }
		}

		public override string ToString(object val)
		{
			return ((TimeSpan)val).Ticks.ToString();
		}

		#region IVersionType Members

		public object Next(object current, ISessionImplementor session)
		{
			return Seed(session);
		}

		public virtual object Seed(ISessionImplementor session)
		{
			return new TimeSpan(DateTime.Now.Ticks);
		}

		public object StringToObject(string xml)
		{
			return TimeSpan.Parse(xml);
		}

		public IComparer Comparator
		{
			get { return Comparer<TimeSpan>.Default; }
		}

		#endregion

		public override object FromStringValue(string xml)
		{
			return TimeSpan.Parse(xml);
		}

		public override System.Type PrimitiveClass
		{
			get { return typeof(TimeSpan); }
		}

		public override object DefaultValue
		{
			get { return TimeSpan.Zero; }
		}

		public override string ObjectToSQLString(object value, NHibernate.Dialect.Dialect dialect)
		{
			return '\'' + ((TimeSpan)value).Ticks.ToString() + '\'';
		}
	}
}
