namespace NHibernate.SybaseAdsDriver
{
    public class AdsClientDriver : Driver.ReflectionBasedDriver
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="AdsClientDriver" /> class.
        /// </summary>
        /// <exception cref="HibernateException">
        /// Thrown when the Advantage.Data.Provider assembly is not and can not be loaded.
        /// </exception>
        public AdsClientDriver()
            : base("Advantage.Data.Provider", 
                   "Advantage.Data.Provider.AdsConnection", 
                   "Advantage.Data.Provider.AdsCommand")
        {

        }

        public override bool UseNamedPrefixInSql
        {
            get { return true; }
        }

        public override bool UseNamedPrefixInParameter
        {
            get { return false; }
        }

        public override string NamedPrefix
        {
            get { return ":"; }
        }

    }
}
