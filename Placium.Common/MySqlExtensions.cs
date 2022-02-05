using System;
using MySql.Data.MySqlClient;

namespace Placium.Common;

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

    public static void TryClose(this MySqlConnection connection)
    {
        try
        {
            connection.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"error execute mysql command ({ex.Message}).");
        }
    }

    public static int TryExecuteNonQuery(this MySqlCommand command)
    {
        try
        {
            return command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"error execute mysql command ({ex.Message}).");
        }

        return 0;
    }
}