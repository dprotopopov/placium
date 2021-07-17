using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using NDbfReader;
using Newtonsoft.Json;
using Npgsql;
using OsmSharp;
using OsmSharp.Tags;
using Placium.Types;

namespace Placium.Common
{
    public static class PgExtensions
    {
        private const string Null = "null";

        public static string TypeAsText(this IColumn column)
        {
            if (column.Type == typeof(string))
                return $"VARCHAR({column.Size})";
            if (column.Type == typeof(decimal?))
                return $"NUMERIC({column.Size})";
            if (column.Type == typeof(DateTime?))
                return "TIMESTAMP";
            if (column.Type == typeof(int))
                return "INTEGER";
            if (column.Type == typeof(bool))
                return "BOOLEAN";

            throw new NotImplementedException();
        }

        public static string ValueAsText(this IColumn column, Reader reader)
        {
            if (column.Type == typeof(string))
                return reader.GetString(column) != null
                    ? TextEscape(reader.GetString(column))
                    : string.Empty;
            if (column.Type == typeof(decimal?))
                return reader.GetDecimal(column).HasValue
                    ? reader.GetDecimal(column).Value.ToString("G", CultureInfo.InvariantCulture)
                    : string.Empty;
            if (column.Type == typeof(DateTime?))
                return reader.GetDateTime(column).HasValue
                    ? reader.GetDateTime(column).Value.ToString("u", CultureInfo.InvariantCulture)
                    : string.Empty;
            return reader.GetValue(column) != null
                ? reader.GetValue(column).ToString()
                : string.Empty;
        }

        public static string Yo(this string s)
        {
            if (s == null) return string.Empty;

            return s.Replace("ё", "е")
                .Replace("Ё", "Е");
        }

        public static string Escape(this string s)
        {
            if (s == null) return string.Empty;

            return s.Replace("\\", @"\\")
                .Replace("\'", @"\'")
                .Replace("\r", @"\r")
                .Replace("\n", @"\n")
                .Replace("\t", @"\t")
                .Replace("\a", @"\a")
                .Replace("\b", @"\b")
                .Replace("\f", @"\f")
                .Replace("\v", @"\v")
                .Replace("\0", @"\0")
                .Replace("!", @"\!")
                .Replace("$", @"\$")
                .Replace("&", @"\&")
                .Replace("-", @"\-")
                .Replace("<", @"\<")
                .Replace("@", @"\@")
                .Replace("^", @"\^")
                .Replace("|", @"\|")
                .Replace("~", @"\~")
                .Replace("=", @"\=")
                .Replace("/", @"\/")
                .Replace("(", @"\(")
                .Replace(")", @"\)")
                .Replace("\"", @"\""");
        }
        public static string Escape2(this string s)
        {
            if (s == null) return string.Empty;

            return s.Replace("\\", @"\\\\")
                .Replace("\'", @"\\\'")
                .Replace("\r", @"\\\r")
                .Replace("\n", @"\\\n")
                .Replace("\t", @"\\\t")
                .Replace("\a", @"\\\a")
                .Replace("\b", @"\\\b")
                .Replace("\f", @"\\\f")
                .Replace("\v", @"\\\v")
                .Replace("\0", @"\\\0")
                .Replace("!", @"\\\!")
                .Replace("$", @"\\\$")
                .Replace("&", @"\\\&")
                .Replace("-", @"\\\-")
                .Replace("<", @"\\\<")
                .Replace("@", @"\\\@")
                .Replace("^", @"\\\^")
                .Replace("|", @"\\\|")
                .Replace("~", @"\\\~")
                .Replace("=", @"\\\=")
                .Replace("/", @"\\\/")
                .Replace("(", @"\\\(")
                .Replace(")", @"\\\)")
                .Replace("\"", @"\\\""");
        }

        public static string TextEscape(this string s, int mode = 0)
        {
            if (s == null) return string.Empty;

            switch (mode)
            {
                case 0:
                    return s.Replace("\\", @"\\")
                        .Replace("\'", @"\'")
                        .Replace("\r", @"\r")
                        .Replace("\n", @"\n")
                        .Replace("\t", @"\t")
                        .Replace("\a", @"\a")
                        .Replace("\b", @"\b")
                        .Replace("\f", @"\f")
                        .Replace("\v", @"\v")
                        .Replace("\0", @"\0");
                case 1:
                    return s.Replace("\\", @"\\")
                        .Replace("\"", @"\""")
                        .Replace("\'", @"\'")
                        .Replace("\r", @"\r")
                        .Replace("\n", @"\n")
                        .Replace("\t", @"\t")
                        .Replace("\a", @"\a")
                        .Replace("\b", @"\b")
                        .Replace("\f", @"\f")
                        .Replace("\v", @"\v")
                        .Replace("\0", @"\0");
                case 2:
                    return s.Replace("\\", @"\\\\")
                        .Replace("\"", @"\\\""")
                        .Replace("\'", @"\\\'")
                        .Replace("\r", @"\\\r")
                        .Replace("\n", @"\\\n")
                        .Replace("\t", @"\\\t")
                        .Replace("\a", @"\\\a")
                        .Replace("\b", @"\\\b")
                        .Replace("\f", @"\\\f")
                        .Replace("\v", @"\\\v")
                        .Replace("\0", @"\\\0");
                case 4:
                    return s.Replace("\\", @"\\\\\\\\")
                        .Replace("\"", @"\\\\\\\""")
                        .Replace("\'", @"\\\\\\\'")
                        .Replace("\r", @"\\\\\\\r")
                        .Replace("\n", @"\\\\\\\n")
                        .Replace("\t", @"\\\\\\\t")
                        .Replace("\a", @"\\\\\\\a")
                        .Replace("\b", @"\\\\\\\b")
                        .Replace("\f", @"\\\\\\\f")
                        .Replace("\v", @"\\\\\\\v")
                        .Replace("\0", @"\\\\\\\0");
                default:
                    throw new NotImplementedException();
            }
        }

        public static string ValueAsText(this double? value)
        {
            return value?.ToString("G", CultureInfo.InvariantCulture) ?? string.Empty;
        }

        public static string ValueAsText(this DateTime? value)
        {
            return value != null ? $"{value.Value.ToString("u", CultureInfo.InvariantCulture)}" : string.Empty;
        }

        public static string ValueAsText(this string value)
        {
            return value != null ? $"{value.TextEscape()}" : string.Empty;
        }

        public static string SafeGetString(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);
        }

        public static int? SafeGetInt32(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? (int?) null : reader.GetInt32(ordinal);
        }

        public static long? SafeGetInt64(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? (long?) null : reader.GetInt64(ordinal);
        }

        public static double? SafeGetDouble(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? (double?) null : reader.GetDouble(ordinal);
        }

        public static DateTime? SafeGetDateTime(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? (DateTime?) null : reader.GetDateTime(ordinal);
        }

        public static bool? SafeGetBoolean(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? (bool?) null : reader.GetBoolean(ordinal);
        }

        public static object SafeGetValue(this NpgsqlDataReader reader, int ordinal)
        {
            return reader.IsDBNull(ordinal) ? null : reader.GetValue(ordinal);
        }

        public static TagsCollection ToTags(this string s)
        {
            var result = new TagsCollection();
            var dictionary = JsonConvert.DeserializeObject<Dictionary<string, string>>(s);
            foreach (var pair in dictionary) result.AddOrReplace(pair.Key, pair.Value);
            return result;
        }

        public static TagsCollection ToTags(this Dictionary<string, string> dictionary)
        {
            var result = new TagsCollection();
            foreach (var pair in dictionary) result.AddOrReplace(pair.Key, pair.Value);
            return result;
        }

        public static void Fill(this Node node, NpgsqlDataReader reader)
        {
            node.Id = reader.SafeGetInt64(0);
            node.Version = reader.SafeGetInt32(1);
            node.Latitude = reader.SafeGetDouble(2);
            node.Longitude = reader.SafeGetDouble(3);
            node.ChangeSetId = reader.SafeGetInt64(4);
            node.TimeStamp = reader.SafeGetDateTime(5);
            node.UserId = reader.SafeGetInt64(6);
            node.UserName = reader.SafeGetString(7);
            node.Visible = reader.SafeGetBoolean(8);
            node.Tags = ((Dictionary<string, string>) reader.SafeGetValue(9)).ToTags();
        }

        public static void Fill(this Way way, NpgsqlDataReader reader)
        {
            way.Id = reader.SafeGetInt64(0);
            way.Version = reader.SafeGetInt32(1);
            way.ChangeSetId = reader.SafeGetInt64(2);
            way.TimeStamp = reader.SafeGetDateTime(3);
            way.UserId = reader.SafeGetInt64(4);
            way.UserName = reader.SafeGetString(5);
            way.Visible = reader.SafeGetBoolean(6);
            way.Tags = ((Dictionary<string, string>) reader.SafeGetValue(7)).ToTags();
            way.Nodes = (long[]) reader.SafeGetValue(8);
        }

        public static void Fill(this Relation relation, NpgsqlDataReader reader)
        {
            relation.Id = reader.SafeGetInt64(0);
            relation.Version = reader.SafeGetInt32(1);
            relation.ChangeSetId = reader.SafeGetInt64(2);
            relation.TimeStamp = reader.SafeGetDateTime(3);
            relation.UserId = reader.SafeGetInt64(4);
            relation.UserName = reader.SafeGetString(5);
            relation.Visible = reader.SafeGetBoolean(6);
            relation.Tags = ((Dictionary<string, string>) reader.SafeGetValue(7)).ToTags();
            relation.Members = ((OsmRelationMember[]) reader.SafeGetValue(8))
                .Select(x => new RelationMember(x.Id, x.Role, (OsmGeoType) x.Type)).ToArray();
        }
    }
}
