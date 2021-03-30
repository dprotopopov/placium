using System;
using MySql.Data.MySqlClient;

namespace Placium.Common
{
    public static class MySqlExtensions
    {
        public static void TryOpen(this MySqlConnection connection)
        {
            try
            {
                connection.Open();
            }
            catch (Exception)
            {
            }
        }

        public static int TryExecuteNonQuery(this MySqlCommand command)
        {
            try
            {
                return command.ExecuteNonQuery();
            }
            catch (Exception)
            {
            }

            return 0;
        }
    }
}